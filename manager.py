"""Track registrations and serialize the complete DHT lifecycle.

Run with: DHT_GROUP=G python3 manager.py PORT
"""

import random
import sys
from collections import OrderedDict
from protocol import (
    MAX_PACKET, MAX_PEERS, configured_group, decode, encode,
    integer, ipv4, log, peer_name, port, reply, send, summary, udp_socket,
)


class Manager:
    def __init__(self, listen_port, group):
        self.port, self.group = port(listen_port, group), group
        self.peers = {}
        self.phase = 'EMPTY'
        self.leader = None
        self.ring = []
        self.cache = OrderedDict()
        self.year = None
        self.pending_change = None

    def handle(self, req, source):
        """Handle a request, or return the saved reply if it is a retry."""
        key = (source, req['request_id'])
        fingerprint = encode(req)
        if key in self.cache:
            old_bytes, result = self.cache[key]
            if old_bytes != fingerprint:
                return reply(req, False, reason='request_id reused for different payload')
            return result
        try:
            result = self._apply(req, source)
        except (KeyError, TypeError, ValueError) as exc:
            result = reply(req, False, reason=str(exc))
        self.cache[key] = (fingerprint, result)
        if len(self.cache) > 4096:
            # Drop the oldest reply to keep the cache from growing indefinitely.
            self.cache.popitem(last=False)
        return result

    def _apply(self, req, source):
        command = req['command']
        # Wait for the leader to finish before accepting other commands.
        expected = {'BUILDING': 'dht-complete', 'REBUILDING': 'dht-rebuilt',
                    'TEARING_DOWN': 'teardown-complete'}.get(self.phase)
        if expected and command != expected:
            return reply(req, False, reason='waiting for ' + expected)

        if command == 'register':
            name = peer_name(req['peer_name'])
            ip = ipv4(req['ip'])
            mp, pp = port(req['m_port'], self.group), port(req['p_port'], self.group)
            if source != (ip, mp):
                raise ValueError('source must match advertised IPv4 and m-port')
            used = {
                p
                for peer in self.peers.values()
                for p in (peer['m_port'], peer['p_port'])
            }
            if name in self.peers or mp == pp or {mp, pp} & (used | {self.port}):
                raise ValueError('duplicate peer name or port')
            if len(self.peers) >= MAX_PEERS:
                raise ValueError('registered peer limit reached')
            self.peers[name] = dict(ip=ip, m_port=mp, p_port=pp, state='Free')
            return reply(req, True, state='Free')

        # All other requests must come from a registered peer's manager port.
        name = peer_name(req['peer_name'])
        if name not in self.peers:
            raise ValueError('peer is not registered')
        peer = self.peers[name]
        if source != (peer['ip'], peer['m_port']):
            raise ValueError('request does not originate from registered m-port')

        if command == 'setup-dht':
            n = integer(req['n'], 3, MAX_PEERS, 'n')
            integer(req['year'], 1950, 2019, 'year')
            if self.phase != 'EMPTY':
                raise ValueError('a DHT already exists or is under construction')
            candidates = [
                p for p in self.peers
                if p != name and self.peers[p]['state'] == 'Free'
            ]
            if len(candidates) < n - 1:
                raise ValueError('fewer than n free registered peers')
            # Put the leader first and choose the remaining members at random.
            self.ring = [name] + random.sample(candidates, n - 1)
            self.leader, self.phase = name, 'BUILDING'
            self.year = req['year']
            self.dht_id = req['request_id']
            for member in self.ring:
                self.peers[member]['state'] = 'Leader' if member == name else 'InDHT'
            tuples = [
                [p, self.peers[p]['ip'], self.peers[p]['p_port']]
                for p in self.ring
            ]
            return reply(req, True, peers=tuples, dht_id=req['request_id'])

        if command == 'dht-complete':
            if name != self.leader or self.phase != 'BUILDING':
                raise ValueError('only the building DHT leader can report completion')
            self.phase = 'READY'
            return reply(req, True, phase='READY')
        if command == 'query-dht':
            if self.phase != 'READY' or peer['state'] != 'Free':
                raise ValueError('query requires a ready DHT and a Free registered peer')
            return reply(req, True, peer=self.tuples([random.choice(self.ring)])[0],
                         dht_id=self.dht_id, peers=self.tuples(self.ring))

        if command in ('leave-dht', 'join-dht'):
            if self.phase != 'READY':
                raise ValueError('DHT is not ready')
            old = self.ring[:]
            if command == 'leave-dht':
                if name not in old:
                    raise ValueError('peer is not in the DHT')
                if len(old) == 1:
                    raise ValueError('last member must teardown the DHT before leaving')
                i = old.index(name)
                new = old[i + 1:] + old[:i]
            else:
                if peer['state'] != 'Free' or len(old) >= MAX_PEERS:
                    raise ValueError('join requires a Free peer and available ring capacity')
                new = old + [name]
            self.pending_change = dict(initiator=name, ring=new,
                                       dht_id=req['request_id'])
            self.phase = 'REBUILDING'
            return reply(req, True, old_peers=self.tuples(old), peers=self.tuples(new),
                         year=self.year, dht_id=req['request_id'])

        if command == 'dht-rebuilt':
            change = self.pending_change
            if self.phase != 'REBUILDING' or not change or name != change['initiator']:
                raise ValueError('only the peer initiating churn can report completion')
            if req['new_leader'] != change['ring'][0]:
                raise ValueError('incorrect new leader')
            for p in self.ring:
                self.peers[p]['state'] = 'Free'
            self.ring = change['ring']
            self.leader = self.ring[0]
            self.dht_id = change['dht_id']
            for p in self.ring:
                self.peers[p]['state'] = 'Leader' if p == self.leader else 'InDHT'
            self.pending_change = None
            self.phase = 'READY'
            return reply(req, True, phase=self.phase, leader=self.leader)

        if command == 'teardown-dht':
            if self.phase != 'READY' or name != self.leader:
                raise ValueError('only the current leader can teardown a ready DHT')
            self.phase = 'TEARING_DOWN'
            return reply(req, True)

        if command == 'teardown-complete':
            if self.phase != 'TEARING_DOWN' or name != self.leader:
                raise ValueError('only the tearing-down leader can report completion')
            for p in self.ring:
                self.peers[p]['state'] = 'Free'
            self.ring, self.leader, self.year = [], None, None
            self.phase = 'EMPTY'
            return reply(req, True, phase=self.phase)

        if command == 'deregister':
            if peer['state'] != 'Free':
                raise ValueError('only a Free peer can deregister')
            del self.peers[name]
            return reply(req, True)
        return reply(req, False, reason='unknown command')

    def tuples(self, names):
        return [[p, self.peers[p]['ip'], self.peers[p]['p_port']] for p in names]


    def run(self):
        with udp_socket(('0.0.0.0', self.port)) as sock:
            log('manager', 'LISTEN', port=self.port, group=self.group)
            while True:
                data, source = sock.recvfrom(MAX_PACKET + 1)
                try:
                    req = decode(data)
                except (ValueError, UnicodeError) as exc:
                    log('manager', 'DROP', reason=str(exc))
                    continue
                log('manager', 'RX', source=f'{source[0]}:{source[1]}', **summary(req))
                result = self.handle(req, source)
                send(sock, result, source, 'manager')
                log(
                    'manager', 'STATE', phase=self.phase,
                    peers={name: p['state'] for name, p in self.peers.items()},
                )


def main():
    try:
        if len(sys.argv) != 2:
            raise ValueError('usage: python3 manager.py <port>')
        Manager(int(sys.argv[1]), configured_group()).run()
    except KeyboardInterrupt:
        print('\nManager stopped.', flush=True)
    except (ValueError, OSError) as exc:
        sys.exit(str(exc))


if __name__ == '__main__':
    main()
