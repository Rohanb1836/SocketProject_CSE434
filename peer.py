"""Interactive UDP peer: setup, hot-potato queries, churn and teardown.

Main thread owns manager exchanges. Receiver never blocks on another peer;
long rebuild requests run in a worker. Run from the project directory with
DHT_GROUP=71 and the same data/ directory on every host.
"""
import hashlib
import json
import os
import queue
import random
import shlex
import socket
import sys
import threading
import time
from collections import OrderedDict
from protocol import (FIELDS, MAX_PACKET, MAX_PEERS, PRINT_LOCK, HashTable,
                      configured_group, decode, encode, first_prime_above,
                      integer, ipv4, load_records, log, message, peer_name,
                      port, reply, send, summary, udp_socket, validate_record)

RETRY_SECONDS = 0.5
OP_TIMEOUT = 600


def endpoint(entry):
    return tuple(entry[1:])


class Peer:
    def __init__(self, manager_ip, manager_port, group):
        self.group = group
        self.manager = (ipv4(manager_ip), port(manager_port, group))
        self.name = None
        self.ms = self.ps = None
        self.registered = False
        self.ring = []
        self.id = self.table = self.dht_id = None
        self.pending = {}
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.receiver = None
        self.workers = []
        # Cache control replies so retries do not repeat teardown or reset.
        self.control_cache = OrderedDict()
        self.jobs = {}
        self.dataset_cache = {}

    @property
    def right(self):
        return endpoint(self.ring[(self.id + 1) % len(self.ring)])

    @property
    def left(self):
        return endpoint(self.ring[(self.id - 1) % len(self.ring)])

    @property
    def identity(self):
        return [self.name, self.ip, self.pp]

    def dataset(self, year):
        directory = os.environ.get('DHT_DATA_DIR', 'data')
        path = os.path.join(directory, 'details-%s.csv' % year)
        stamp = (os.stat(path).st_mtime_ns, os.stat(path).st_size)
        cached = self.dataset_cache.get(year)
        if cached and cached[0] == stamp:
            return cached[1:]
        rows = load_records(year, directory)
        integer(len(rows), 1, 100000, 'record count')
        digest = hashlib.sha256(encode_rows(rows)).hexdigest()
        for row in rows:
            encode(message('store', dht_id='0'*32, record=row,
                           pos=200003, owner=MAX_PEERS, seq=100000))
        # Cache the signature only; each build rereads its records from disk.
        self.dataset_cache[year] = (stamp, len(rows), digest)
        return len(rows), digest

    def manager_request(self, req):
        for attempt in range(10):
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
                if (source == self.manager and result.get('command') == 'response'
                        and result['request_id'] == req['request_id']
                        and result.get('for_command') == req['command']):
                    log(self.name, 'RX', source='%s:%s' % source, **summary(result))
                    if result.get('code') != 'SUCCESS':
                        raise ValueError(result.get('reason', 'manager rejected request'))
                    return result
            log(self.name, 'RETRY', command=req['command'], attempt=attempt + 1)
        raise TimeoutError('manager timeout; state uncertain: restart all processes')

    def request(self, req, destination, command='response', sources=None, timeout=5):
        inbox = queue.Queue()
        sources = {destination} if sources is None else set(sources)
        with self.lock:
            self.pending[req['request_id']] = (inbox, sources, command)
        deadline = time.monotonic() + timeout
        interval = 2 if timeout > 10 else RETRY_SECONDS
        try:
            while time.monotonic() < deadline and not self.stop.is_set():
                send(self.ps, req, destination, self.name)
                try:
                    result = inbox.get(timeout=min(interval, max(.001, deadline-time.monotonic())))
                except queue.Empty:
                    continue
                if result.get('code', 'SUCCESS') != 'SUCCESS' and command != 'find-result':
                    raise ValueError(result.get('reason', 'peer rejected operation'))
                return result
            raise TimeoutError('peer operation timed out; restart all processes after failed mutation')
        finally:
            with self.lock:
                self.pending.pop(req['request_id'], None)

    def deliver(self, req, source):
        with self.lock:
            pending = self.pending.get(req['request_id'])
            if pending and source in pending[1] and req['command'] == pending[2]:
                pending[0].put(req)

    def register(self, name, ip, mp, pp):
        if self.ms:
            raise ValueError('registration already attempted; restart to change identity')
        peer_name(name)
        ip, mp, pp = ipv4(ip), port(mp, self.group), port(pp, self.group)
        if mp == pp or self.manager[1] in (mp, pp):
            raise ValueError('ports must be distinct')
        self.ms = udp_socket((ip, mp))
        try:
            self.ps = udp_socket((ip, pp))
        except OSError:
            self.ms.close()
            self.ms = None
            raise
        self.name, self.ip, self.mp, self.pp = name, ip, mp, pp
        self.manager_request(message('register', peer_name=name, ip=ip, m_port=mp, p_port=pp))
        self.registered = True
        self.ps.settimeout(.2)
        self.receiver = threading.Thread(target=self.receive_peers, daemon=True)
        self.receiver.start()
        log(name, 'REGISTERED', state='Free', m_port=mp, p_port=pp)

    def install(self, config):
        ring = config['peers']
        if not isinstance(ring, list) or not 1 <= len(ring) <= MAX_PEERS:
            raise ValueError('invalid ring')
        names, ports = set(), set()
        for entry in ring:
            if not isinstance(entry, list) or len(entry) != 3:
                raise ValueError('invalid member tuple')
            peer_name(entry[0]); ipv4(entry[1]); port(entry[2], self.group)
            if entry[0] in names or entry[2] in ports:
                raise ValueError('duplicate ring member')
            names.add(entry[0]); ports.add(entry[2])
        total = integer(config['total'], 1, 100000, 'total')
        integer(config['year'], 1950, 2019, 'year')
        if config['s'] != first_prime_above(2*total):
            raise ValueError('incorrect hash size')
        if self.identity not in ring:
            self.clear()
            return
        self.ring = ring
        self.id = ring.index(self.identity)
        self.dht_id = config['dht_id']
        self.year, self.size, self.total = config['year'], config['s'], total
        self.digest = config['digest']
        self.table = HashTable(self.size)
        log(self.name, 'SET_ID', id=self.id, n=len(ring), s=self.size,
            leader=ring[0][0], right=self.right)

    def clear(self):
        self.ring, self.table, self.id, self.dht_id = [], None, None, None
        log(self.name, 'LOCAL_DHT_CLEARED')

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
                log(self.name, 'RX', source='%s:%s' % source, **summary(req))
                with self.lock:
                    self.handle_peer(req, source)
            except (ValueError, KeyError, TypeError, IndexError, UnicodeError, OSError) as exc:
                log(self.name, 'DROP', reason=str(exc))

    def remember(self, key, outgoing, destination):
        self.control_cache[key] = (outgoing, destination)
        if len(self.control_cache) > 2048:
            self.control_cache.popitem(last=False)
        send(self.ps, outgoing, destination, self.name)

    def handle_peer(self, req, source):
        command = req['command']
        if command in ('response', 'find-result'):
            self.deliver(req, source)
            return
        if command in ('set-id', 'teardown', 'reset-id', 'count-ring', 'preflight'):
            key = (source, req['request_id'], command)
            if key in self.control_cache:
                outgoing, destination = self.control_cache[key]
                send(self.ps, outgoing, destination, self.name)
                return
            if command == 'set-id':
                if source != endpoint(req['peers'][0]) or self.ring:
                    raise ValueError('set-id requires a Free peer and the proposed leader')
                self.install(req)
                self.remember(key, reply(req, True), source)
                return
            self.handle_token(req, source, key)
            return
        if command in ('join-ring', 'rebuild-dht'):
            self.start_job(req, source)
            return
        if command == 'find-event':
            self.find_event(req, source)
            return
        if not self.ring or req.get('dht_id') != self.dht_id or source != self.left:
            raise ValueError('ring message has wrong session or neighbor')
        if command == 'store':
            if self.id == 0:
                raise ValueError('store returned to leader')
            record = req['record']
            event_id = validate_record(record, self.year)
            pos = event_id % self.size
            owner = pos % len(self.ring)
            if req['pos'] != pos or req['owner'] != owner or owner < self.id:
                raise ValueError('invalid store route')
            if self.id == owner:
                self.table.store(pos, record)
                receipt = {k: req[k] for k in ('v','request_id','dht_id','pos','owner','seq')}
                receipt['command'] = 'stored'
                send(self.ps, receipt, self.right, self.name)
            else:
                send(self.ps, req, self.right, self.name)
        elif command == 'stored':
            if self.id == 0:
                self.deliver(req, source)
            else:
                send(self.ps, req, self.right, self.name)
        else:
            raise ValueError('unknown peer command')

    def token(self, command, route=None, **payload):
        # Route starts at the initiator and follows clockwise links.
        route = route or self.ring[self.id:] + self.ring[:self.id]
        req = message(command, route=route, hop=1, **payload)
        if len(route) == 1:
            return req
        return self.request(req, endpoint(route[1]), command,
                            {endpoint(route[-1])}, timeout=30)

    def handle_token(self, req, source, key):
        route, hop = req['route'], req['hop']
        if not isinstance(hop, int) or not 1 <= hop <= len(route):
            raise ValueError('invalid token hop')
        if route[hop % len(route)] != self.identity or source != endpoint(route[hop-1]):
            raise ValueError('token is out of clockwise order')
        if hop == len(route):
            self.deliver(req, source)
            return
        command = req['command']
        if command == 'preflight':
            try:
                total, digest = self.dataset(req['year'])
                if (total, digest) != (req['total'], req['digest']):
                    raise ValueError('dataset differs from leader')
            except (ValueError, OSError) as exc:
                req['errors'] = req.get('errors', []) + [self.name + ': ' + str(exc)]
        elif command == 'teardown':
            if req['dht_id'] != self.dht_id:
                raise ValueError('wrong teardown session')
            self.table = None
            log(self.name, 'TABLE_DELETED')
            if req.get('final'):
                self.clear()
        elif command == 'reset-id':
            self.install(req['config'])
        elif command == 'count-ring':
            if req['dht_id'] != self.dht_id or self.table is None:
                raise ValueError('count requested for inactive DHT')
            req['counts'] = req['counts'] + [[self.id, self.name, self.table.count]]
        req['hop'] += 1
        self.remember(key, req, endpoint(route[req['hop'] % len(route)]))

    def preflight(self, route, config):
        result = self.token('preflight', route, year=config['year'], total=config['total'],
                            digest=config['digest'], errors=[])
        if result.get('errors'):
            raise ValueError('; '.join(result['errors']))

    def config(self, peers, year, dht_id):
        total, digest = self.dataset(year)
        return dict(peers=peers, year=year, total=total, digest=digest,
                    s=first_prime_above(2*total), dht_id=dht_id)

    def build(self):
        if self.id != 0:
            raise ValueError('only leader can populate the ring')
        rows = load_records(self.year, os.environ.get('DHT_DATA_DIR', 'data'))
        if len(rows) != self.total or hashlib.sha256(encode_rows(rows)).hexdigest() != self.digest:
            raise ValueError('dataset changed after preflight')
        expected = [0]*len(self.ring)
        log(self.name, 'BUILD_START', records=len(rows), n=len(self.ring))
        for seq, record in enumerate(rows):
            pos = int(record[0]) % self.size
            owner = pos % len(self.ring)
            expected[owner] += 1
            if owner == 0:
                self.table.store(pos, record)
            else:
                req = message('store', dht_id=self.dht_id, seq=seq,
                              pos=pos, owner=owner, record=record)
                receipt = self.request(req, self.right, 'stored', {self.left})
                if any(receipt.get(k) != req[k] for k in ('dht_id','seq','pos','owner')):
                    raise ValueError('mismatched store receipt')
            if (seq+1) % 5000 == 0:
                log(self.name, 'BUILD_PROGRESS', stored=seq+1, total=len(rows))
        counts = self.token('count-ring', dht_id=self.dht_id,
                            counts=[[0,self.name,self.table.count]])['counts']
        if counts != [[i,p[0],expected[i]] for i,p in enumerate(self.ring)]:
            raise ValueError('actual peer counts do not match the dataset distribution')
        for ident, name, count in counts:
            log(self.name, 'PEER_COUNT', id=ident, peer_name=name, records=count)
        log(self.name, 'TOTAL_VERIFIED', records=sum(expected), expected=len(rows))

    def setup(self, n, year):
        integer(n, 3, MAX_PEERS, 'n')
        integer(year, 1950, 2019, 'year')
        self.dataset(year)
        result = self.manager_request(message('setup-dht', peer_name=self.name, n=n, year=year))
        config = self.config(result['peers'], year, result['dht_id'])
        self.install(config)
        for entry in self.ring[1:]:
            self.request(message('set-id', **config), endpoint(entry))
        self.preflight(self.ring, config)
        self.build()
        self.manager_request(message('dht-complete', peer_name=self.name))
        log(self.name, 'DHT_COMPLETE', year=year, n=n)

    def query(self, event_id=None):
        if event_id is not None:
            integer(event_id, 0, 2**63-1, 'event_id')
        result = self.manager_request(message('query-dht', peer_name=self.name))
        self.query_target = result
        if event_id is not None:
            self.search(event_id)
        else:
            print('Enter: find-event EVENT_ID', flush=True)

    def search(self, event_id):
        integer(event_id, 0, 2**63-1, 'event_id')
        target = getattr(self, 'query_target', None)
        if target is None:
            raise ValueError('issue query-dht NAME first')
        req = message('find-event', event_id=event_id, requester=self.identity,
                      dht_id=target['dht_id'], id_seq=[])
        result = self.request(req, endpoint(target['peer']),
                              sources={endpoint(p) for p in target['peers']})['result']
        with PRINT_LOCK:
            if result['code'] == 'SUCCESS':
                print('SUCCESS', flush=True)
                for key, value in zip(FIELDS, result['record']):
                    print('%s: %s' % (key, value), flush=True)
            else:
                print('Storm event %s not found in the DHT.' % event_id, flush=True)
            print('id-seq: ' + ', '.join(map(str, result['id_seq'])), flush=True)
        log(self.name, 'QUERY_RESULT', event_id=event_id, code=result['code'], id_seq=result['id_seq'])
        self.query_target = None

    def find_event(self, req, source):
        # Probe locally, then reply directly or forward to an unvisited peer.
        if not self.ring or self.table is None or req.get('dht_id') != self.dht_id:
            send(self.ps, reply(req, False, reason='DHT changed or unavailable'), source, self.name)
            return
        seq = req['id_seq']
        if not isinstance(seq, list) or len(set(seq)) != len(seq) or self.id in seq:
            raise ValueError('invalid query visitation sequence')
        if seq:
            if source != endpoint(self.ring[seq[-1]]):
                raise ValueError('query source does not match previous hop')
        elif source != endpoint(req['requester']):
            raise ValueError('invalid query origin')
        event_id = integer(req['event_id'], 0, 2**63-1, 'event_id')
        seq = seq + [self.id]
        record = self.table.find(event_id % self.size, event_id)
        remaining = [i for i in range(len(self.ring)) if i not in seq]
        if record is not None or not remaining:
            result = dict(code='SUCCESS' if record is not None else 'FAILURE',
                          record=record, id_seq=seq, event_id=event_id)
            response = reply(req, True, result=result)
            send(self.ps, response, endpoint(req['requester']), self.name)
        else:
            req['id_seq'] = seq
            send(self.ps, req, endpoint(self.ring[random.choice(remaining)]), self.name)

    def start_job(self, req, source):
        key = (source, req['request_id'])
        if key in self.jobs:
            if self.jobs[key] is not None:
                send(self.ps, self.jobs[key], source, self.name)
            return
        if req['command'] == 'rebuild-dht':
            if self.id != 0 or source != endpoint(req['initiator']):
                raise ValueError('invalid rebuild origin')
        else:
            if self.id != 0 or source != endpoint(req['peers'][-1]):
                raise ValueError('join must come from the joining peer to the leader')
        self.jobs[key] = None
        def work():
            try:
                if req['command'] == 'join-ring':
                    config = self.config(req['peers'], self.year, req['dht_id'])
                    self.preflight(req['peers'], config)
                    self.token('teardown', dht_id=self.dht_id, final=False)
                    self.table = None
                    self.token('reset-id', req['peers'], config=config)
                    self.install(config)
                self.build()
                result = reply(req, True)
            except (ValueError, OSError, TimeoutError, KeyError) as exc:
                result = reply(req, False, reason=str(exc))
            with self.lock:
                self.jobs[key] = result
            send(self.ps, result, source, self.name)
        thread = threading.Thread(target=work, daemon=True)
        self.workers.append(thread)
        thread.start()

    def leave(self):
        result = self.manager_request(message('leave-dht', peer_name=self.name))
        config = self.config(result['peers'], result['year'], result['dht_id'])
        route = self.ring[self.id:] + self.ring[:self.id]
        self.preflight(route, config)
        self.token('teardown', route, dht_id=self.dht_id, final=False)
        self.table = None
        self.token('reset-id', route, config=config)
        leader = endpoint(result['peers'][0])
        self.request(message('rebuild-dht', initiator=self.identity), leader, timeout=OP_TIMEOUT)
        self.clear()
        self.manager_request(message('dht-rebuilt', peer_name=self.name,
                                     new_leader=result['peers'][0][0]))
        log(self.name, 'LEFT_DHT', leader=result['peers'][0][0])

    def join(self):
        result = self.manager_request(message('join-dht', peer_name=self.name))
        self.request(message('join-ring', peers=result['peers'], dht_id=result['dht_id']),
                     endpoint(result['peers'][0]), timeout=OP_TIMEOUT)
        self.manager_request(message('dht-rebuilt', peer_name=self.name,
                                     new_leader=result['peers'][0][0]))
        log(self.name, 'JOINED_DHT', n=len(self.ring))

    def teardown(self):
        self.manager_request(message('teardown-dht', peer_name=self.name))
        self.token('teardown', dht_id=self.dht_id, final=True)
        self.clear()
        self.manager_request(message('teardown-complete', peer_name=self.name))
        log(self.name, 'TEARDOWN_COMPLETE')

    def command(self, line):
        args = shlex.split(line)
        if not args:
            return
        command = args[0]
        if command == 'register' and len(args) == 5:
            return self.register(args[1], args[2], int(args[3]), int(args[4]))
        if command == 'status':
            log(self.name, 'STATUS', ring=self.ring, id=self.id,
                records=self.table.count if self.table else 0)
            return
        if not self.registered:
            raise ValueError('register first')
        if command == 'find-event' and len(args) == 2:
            return self.search(int(args[1]))
        if len(args) < 2 or args[1] != self.name:
            raise ValueError('use this process\'s registered peer name')
        if command == 'setup-dht' and len(args) == 4:
            return self.setup(int(args[2]), int(args[3]))
        if command == 'query-dht' and len(args) in (2,3):
            return self.query(int(args[2]) if len(args) == 3 else None)
        if len(args) == 2:
            if command == 'leave-dht': return self.leave()
            if command == 'join-dht': return self.join()
            if command == 'teardown-dht': return self.teardown()
            if command == 'deregister':
                self.manager_request(message('deregister', peer_name=self.name))
                self.registered = False
                self.stop.set()
                log(self.name, 'DEREGISTERED')
                return
        raise ValueError('invalid command; completion commands are automatic')

    def close(self):
        self.stop.set()
        if self.receiver:
            self.receiver.join(timeout=1)
        for sock in (self.ms, self.ps):
            if sock: sock.close()

    def run(self):
        log('peer', 'START', manager=self.manager, group=self.group)
        print('Commands: register | setup-dht | query-dht | find-event | leave-dht | join-dht | teardown-dht | deregister | status', flush=True)
        try:
            while not self.stop.is_set():
                line = sys.stdin.readline()
                if not line:
                    self.stop.wait()
                    break
                try:
                    self.command(line)
                except (ValueError, OSError, TimeoutError) as exc:
                    log(self.name or 'peer', 'COMMAND_FAILED', reason=str(exc))
        finally:
            self.close()


def encode_rows(rows):
    return json.dumps(rows, separators=(',', ':'), ensure_ascii=True).encode('utf-8')


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
