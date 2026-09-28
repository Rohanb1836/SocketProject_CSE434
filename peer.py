"""Peer program for building the DHT.

The main thread reads commands and talks to the manager. A second thread
listens for peer messages, even while the program is waiting for user input.
"""

import os
import queue
import shlex
import socket
import sys
import threading
import time
from protocol import (
    MAX_PACKET, MAX_PEERS, HashTable, configured_group, decode,
    encode, first_prime_above, integer, ipv4, load_records, log,
    message, peer_name, port, reply, send, summary, udp_socket,
    validate_record,
)

RETRY_SECONDS = 0.5
ATTEMPTS = 10
MAX_RECORDS = 100000


class Peer:
    def __init__(self, manager_ip, manager_port, group):
        self.group = group
        self.manager = (ipv4(manager_ip), port(manager_port, group))
        self.name = None
        self.ms = self.ps = None
        self.registered = False
        self.ring = None
        self.id = None
        self.dht_id = None
        self.table = None
        self.pending = {}
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.receiver = None

    def manager_request(self, req):
        """Send a request and wait for the manager's reply."""
        # Reuse the request ID so a retry doesn't repeat the manager's action.
        for attempt in range(ATTEMPTS):
            send(self.ms, req, self.manager, self.name)
            deadline = time.monotonic() + RETRY_SECONDS
            while time.monotonic() < deadline:
                self.ms.settimeout(max(.001, deadline - time.monotonic()))
                try:
                    data, source = self.ms.recvfrom(MAX_PACKET + 1)
                    result = decode(data)
                except socket.timeout:
                    break
                except (ValueError, UnicodeError):
                    continue
                if (
                    source == self.manager
                    and result.get('command') == 'response'
                    and result['request_id'] == req['request_id']
                    and result.get('for_command') == req['command']
                ):
                    log(self.name, 'RX', source=f'{source[0]}:{source[1]}', **summary(result))
                    if result.get('code') != 'SUCCESS':
                        raise ValueError(result.get('reason', 'manager rejected request'))
                    return result
            log(self.name, 'RETRY', command=req['command'], attempt=attempt + 1)
        raise TimeoutError('manager response not received; state is uncertain; restart all processes')

    def peer_request(self, req, destination, expected_command):
        """Send to a peer and wait for the matching reply or ring receipt."""
        inbox = queue.Queue()
        # Set-id gets a direct reply. Ring receipts arrive from the left neighbor.
        expected_source = destination if expected_command == 'response' else self.left
        with self.lock:
            self.pending[req['request_id']] = (inbox, expected_source, expected_command)
        try:
            for attempt in range(ATTEMPTS):
                send(self.ps, req, destination, self.name)
                try:
                    result = inbox.get(timeout=RETRY_SECONDS)
                except queue.Empty:
                    log(self.name, 'RETRY', command=req['command'], attempt=attempt + 1)
                    continue
                if result.get('code', 'SUCCESS') != 'SUCCESS':
                    raise ValueError(result.get('reason', 'peer rejected request'))
                return result
            raise TimeoutError('ring operation timed out; DHT is incomplete; restart all processes')
        finally:
            with self.lock:
                self.pending.pop(req['request_id'], None)

    @property
    def right(self):
        return tuple(self.ring[(self.id + 1) % len(self.ring)][1:])

    @property
    def left(self):
        return tuple(self.ring[(self.id - 1) % len(self.ring)][1:])

    def register(self, name, ip, mp, pp):
        if self.ms is not None:
            raise ValueError('this process has already attempted registration; restart to change identity')
        peer_name(name)
        ip, mp, pp = ipv4(ip), port(mp, self.group), port(pp, self.group)
        if mp == pp or self.manager[1] in (mp, pp):
            raise ValueError('m-port, p-port and manager port must be distinct')
        # Use the same local IP that we send in the registration message.
        ms = udp_socket((ip, mp))
        try:
            ps = udp_socket((ip, pp))
        except OSError:
            ms.close()
            raise
        self.name, self.ip, self.mp, self.pp = name, ip, mp, pp
        self.ms, self.ps = ms, ps
        self.ps.settimeout(.2)
        result = self.manager_request(
            message('register', peer_name=name, ip=ip, m_port=mp, p_port=pp)
        )
        self.registered = True
        self.receiver = threading.Thread(target=self.receive_peers, daemon=True)
        self.receiver.start()
        log(self.name, 'REGISTERED', state=result['state'], m_port=mp, p_port=pp)

    def initialize_ring(self, req, source):
        """Check set-id and save this peer's place in the ring."""
        ring = req['peers']
        n = integer(req['n'], 3, MAX_PEERS, 'n')
        ident = integer(req['id'], 0, n - 1, 'id')
        total = integer(req['total'], 1, MAX_RECORDS, 'record count')
        year = integer(req['year'], 1950, 2019, 'year')
        size = integer(req['s'], 3, 200003, 'hash size')
        if size != first_prime_above(2 * total):
            raise ValueError('incorrect prime hash table size')
        if not isinstance(ring, list) or len(ring) != n:
            raise ValueError('invalid ring membership')
        names, ports = set(), set()
        for entry in ring:
            if not isinstance(entry, list) or len(entry) != 3:
                raise ValueError('invalid peer tuple')
            peer_name(entry[0])
            ipv4(entry[1])
            port(entry[2], self.group)
            if entry[0] in names or entry[2] in ports:
                raise ValueError('duplicate ring member')
            names.add(entry[0])
            ports.add(entry[2])
        if ring[ident] != [self.name, self.ip, self.pp] or source != tuple(ring[0][1:]):
            raise ValueError('set-id target or source does not match ring')
        dht_id = req['dht_id']
        if not isinstance(dht_id, str) or len(dht_id) != 32:
            raise ValueError('invalid DHT session')
        config = (dht_id, ring, ident, size, total, year)
        if self.ring is not None:
            if config != self.config:
                raise ValueError('peer already belongs to another ring')
            # A repeated set-id must not clear records already stored here.
            return
        self.config = config
        self.dht_id, self.ring, self.id, self.size, self.total, self.year = config
        self.table = HashTable(size)
        log(self.name, 'SET_ID', id=ident, n=n, s=size, right=self.right)

    def receive_peers(self):
        while not self.stop.is_set():
            try:
                data, source = self.ps.recvfrom(MAX_PACKET + 1)
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                req = decode(data)
                log(self.name, 'RX', source=f'{source[0]}:{source[1]}', **summary(req))
                self.handle_peer(req, source)
            except (ValueError, KeyError, TypeError, IndexError, UnicodeError) as exc:
                log(self.name, 'DROP', reason=str(exc))
            except OSError as exc:
                log(self.name, 'NETWORK_ERROR', reason=str(exc))

    def deliver(self, req, source):
        # Pass the reply to the thread waiting for this request.
        with self.lock:
            pending = self.pending.get(req['request_id'])
            if pending and source == pending[1] and req['command'] == pending[2]:
                pending[0].put(req)

    def handle_peer(self, req, source):
        command = req['command']
        if command == 'set-id':
            try:
                self.initialize_ring(req, source)
                result = reply(req, True)
            except (ValueError, KeyError, TypeError, IndexError) as exc:
                result = reply(req, False, reason=str(exc))
            send(self.ps, result, source, self.name)
            return
        if command == 'response':
            if req.get('for_command') == 'set-id':
                self.deliver(req, source)
            return
        if not self.ring or req.get('dht_id') != self.dht_id or source != self.left:
            raise ValueError('message must come from left neighbor in current DHT')
        if command == 'store':
            if self.id == 0:
                raise ValueError('store returned to leader: routing invariant violated')
            record = req['record']
            event_id = validate_record(record, self.year)
            pos = event_id % self.size
            owner = pos % len(self.ring)
            integer(req['seq'], 0, self.total - 1, 'seq')
            if req['pos'] != pos or req['owner'] != owner or owner == 0 or self.id > owner:
                raise ValueError('invalid record route or hash')
            if self.id == owner:
                fresh = self.table.store(pos, record)
                log(
                    self.name, 'STORED' if fresh else 'DUPLICATE_IGNORED',
                    event_id=event_id, pos=pos, count=self.table.count,
                )
                # Keep the record here and send its receipt around the ring.
                receipt = {
                    k: req[k]
                    for k in ('v', 'request_id', 'dht_id', 'seq', 'pos', 'owner')
                }
                receipt['command'] = 'stored'
                send(self.ps, receipt, self.right, self.name)
            else:
                send(self.ps, req, self.right, self.name)
        elif command == 'stored':
            if self.id == 0:
                self.deliver(req, source)
            else:
                send(self.ps, req, self.right, self.name)
        elif command == 'count-ring':
            counts = req['counts']
            if self.id == 0:
                self.deliver(req, source)
            else:
                if not isinstance(counts, list) or len(counts) != self.id:
                    raise ValueError('count token out of ring order')
                req['counts'] = counts + [[self.id, self.name, self.table.count]]
                send(self.ps, req, self.right, self.name)
        else:
            raise ValueError('unknown P2P command')

    def setup(self, name, n, year):
        if not self.registered or name != self.name:
            raise ValueError('setup must use this registered peer name')
        if self.ring is not None:
            raise ValueError('local peer already has a DHT; restart all processes after a failed build')
        integer(n, 3, MAX_PEERS, 'n')
        integer(year, 1950, 2019, 'year')

        # Check the file and message sizes before asking the manager to start.
        records = load_records(year, os.environ.get('DHT_DATA_DIR', 'data'))
        integer(len(records), 1, MAX_RECORDS, 'record count')
        size = first_prime_above(2 * len(records))
        for row in records:
            encode(
                message('store', dht_id='0' * 32, seq=len(records),
                        owner=n, pos=size, record=row)
            )
        log(self.name, 'DATASET', year=year, records=len(records), s=size)
        req = message('setup-dht', peer_name=name, n=n, year=year)
        result = self.manager_request(req)
        config = dict(
            dht_id=result['dht_id'], n=n, s=size, total=len(records),
            year=year, peers=result['peers'],
        )
        self.initialize_ring(message('set-id', id=0, **config), (self.ip, self.pp))
        for ident in range(1, n):
            self.peer_request(
                message('set-id', id=ident, **config),
                tuple(self.ring[ident][1:]),
                'response',
            )

        for seq, record in enumerate(records):
            pos = int(record[0]) % size
            owner = pos % n
            if owner == 0:
                self.table.store(pos, record)
                log(self.name, 'STORED', event_id=int(record[0]), pos=pos, count=self.table.count)
            else:
                token = message(
                    'store', dht_id=self.dht_id, seq=seq,
                    pos=pos, owner=owner, record=record,
                )
                receipt = self.peer_request(token, self.right, 'stored')
                if any(receipt.get(k) != token[k] for k in ('dht_id', 'seq', 'pos', 'owner')):
                    raise ValueError('mismatched store receipt')
        counts = self.peer_request(
            message('count-ring', dht_id=self.dht_id,
                    counts=[[0, self.name, self.table.count]]),
            self.right,
            'count-ring',
        )['counts']

        # Compare the reported counts with the expected counts from the CSV.
        expected = [0] * n
        for record in records:
            expected[(int(record[0]) % size) % n] += 1
        if counts != [[i, self.ring[i][0], expected[i]] for i in range(n)]:
            raise ValueError('ring counts differ from independently calculated distribution')
        for ident, member, count in counts:
            log(self.name, 'PEER_COUNT', id=ident, peer_name=member, records=count)
        log(self.name, 'TOTAL_VERIFIED', records=sum(expected), expected=len(records))
        self.complete()

    def complete(self):
        self.manager_request(message('dht-complete', peer_name=self.name))
        log(self.name, 'DHT_COMPLETE', year=self.year, n=len(self.ring))

    def command(self, line):
        args = shlex.split(line)
        if not args:
            return
        if args[0] == 'register' and len(args) == 5:
            self.register(args[1], args[2], int(args[3]), int(args[4]))
        elif args[0] == 'setup-dht' and len(args) == 4:
            self.setup(args[1], int(args[2]), int(args[3]))
        elif args[0] == 'dht-complete' and len(args) == 2:
            # Setup sends this after it finishes checking the record counts.
            raise ValueError('dht-complete is sent automatically only after verified construction')
        else:
            raise ValueError('commands: register NAME IPv4 MPORT PPORT | setup-dht NAME N YYYY')

    def close(self):
        self.stop.set()
        if self.receiver:
            self.receiver.join(timeout=1)
        for sock in (self.ms, self.ps):
            if sock:
                sock.close()

    def run(self):
        log('peer', 'START', manager=self.manager, group=self.group)
        print('Commands: register NAME IPv4 MPORT PPORT | setup-dht NAME N YYYY', flush=True)
        try:
            while True:
                line = sys.stdin.readline()
                if not line:
                    # Keep listening for peers if the input stream closes.
                    self.stop.wait()
                    break
                try:
                    self.command(line)
                except (ValueError, OSError, TimeoutError) as exc:
                    log(self.name or 'peer', 'COMMAND_FAILED', reason=str(exc))
        finally:
            self.close()


def main():
    try:
        if len(sys.argv) != 3:
            raise ValueError('usage: python3 peer.py <manager IPv4> <manager port>')
        Peer(sys.argv[1], int(sys.argv[2]), configured_group()).run()
    except KeyboardInterrupt:
        print('\nPeer stopped.', flush=True)
    except (ValueError, OSError) as exc:
        sys.exit(str(exc))


if __name__ == '__main__':
    main()
