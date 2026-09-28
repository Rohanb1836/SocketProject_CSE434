"""Message handling and helpers used by the manager and peers."""

import csv
import ipaddress
import json
import math
import os
import re
import socket
import threading
import time
import uuid
from pathlib import Path

VERSION = 1
MAX_PACKET = 8192
MAX_PEERS = 32
FIELDS = (
    'event_id state year month_name event_type cz_type cz_name '
    'injuries_direct injuries_indirect deaths_direct deaths_indirect '
    'damage_property damage_crops tor_f_scale'
).split()
PRINT_LOCK = threading.Lock()


def log(actor, event, **fields):
    # Keep output from the two peer threads on separate lines.
    with PRINT_LOCK:
        print(
            json.dumps(
                dict(time=time.strftime('%H:%M:%S'), actor=actor, event=event, **fields),
                ensure_ascii=True,
            ),
            flush=True,
        )


def integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f'{label} must be an integer in [{low}, {high}]')
    return value


def peer_name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z]{1,15}', value):
        raise ValueError('peer-name must contain 1-15 ASCII letters')
    return value


def ipv4(value):
    return str(ipaddress.IPv4Address(value))


def group_range(group):
    integer(group, 1, 128, 'group')
    # Each group gets 500 ports, starting at 1500 for group 1.
    return 1000 + 500 * group, 1499 + 500 * group


def configured_group():
    try:
        group = int(os.environ['DHT_GROUP'])
    except (KeyError, ValueError):
        raise ValueError('Set DHT_GROUP to your actual Canvas Socket Project group number')
    group_range(group)
    return group


def port(value, group):
    lo, hi = group_range(group)
    return integer(value, lo, hi, 'port')


def message(command, **payload):
    return dict(v=VERSION, command=command, request_id=uuid.uuid4().hex, **payload)


def encode(msg):
    data = json.dumps(msg, separators=(',', ':'), ensure_ascii=True).encode('utf-8')
    if len(data) > MAX_PACKET:
        raise ValueError(f'datagram exceeds {MAX_PACKET} bytes')
    return data


def decode(data):
    if len(data) > MAX_PACKET:
        raise ValueError('oversized datagram')
    msg = json.loads(data.decode('utf-8'))
    if not isinstance(msg, dict) or msg.get('v') != VERSION:
        raise ValueError('unsupported message version')
    if not isinstance(msg.get('command'), str):
        raise ValueError('missing command')
    if (
        not isinstance(msg.get('request_id'), str)
        or not re.fullmatch(r'[0-9a-f]{32}', msg['request_id'])
    ):
        raise ValueError('invalid request_id')
    return msg


def reply(request, ok, **payload):
    return dict(
        v=VERSION,
        command='response',
        request_id=request['request_id'],
        for_command=request['command'],
        code='SUCCESS' if ok else 'FAILURE',
        **payload,
    )


def summary(msg):
    """Choose the fields to print in a message log."""
    keys = ('command', 'request_id', 'code', 'peer_name', 'id', 'owner', 'pos', 'seq', 'reason')
    return {k: msg[k] for k in keys if k in msg}


def send(sock, msg, address, actor):
    sock.sendto(encode(msg), address)
    log(actor, 'TX', destination=f'{address[0]}:{address[1]}', **summary(msg))


def first_prime_above(value):
    # Start above value because the table size must be strictly larger than 2*l.
    n = value + 1
    while n < 2 or any(n % d == 0 for d in range(2, math.isqrt(n) + 1)):
        n += 1
    return n


def validate_record(record, year):
    if (
        not isinstance(record, list)
        or len(record) != 14
        or not all(isinstance(x, str) for x in record)
    ):
        raise ValueError('record must have 14 string fields')
    if not record[0].isdigit() or int(record[2]) != year:
        raise ValueError('invalid event_id or mismatched year')
    # These columns hold injury and death counts.
    for index in (7, 8, 9, 10):
        if not record[index].isdigit():
            raise ValueError(f'{FIELDS[index]} must be a nonnegative integer')
    return int(record[0])


def load_records(year, directory):
    """Read the year's CSV file and check each record."""
    path = Path(directory) / f'details-{year}.csv'
    records = []
    ids = set()
    with path.open(newline='', encoding='utf-8-sig') as stream:
        # Some fields contain commas inside quotes, so don't split lines by hand.
        reader = csv.reader(stream)
        header = next(reader, None)
        normalized = [re.sub(r'[\s-]+', '_', x.strip().lower()) for x in header or []]
        if normalized != FIELDS:
            raise ValueError('CSV must use the 14 assignment fields in their specified order')
        for line, row in enumerate(reader, 2):
            if not row:
                continue
            try:
                event_id = validate_record(row, year)
                if event_id in ids:
                    raise ValueError(f'duplicate event_id {event_id}')
            except (ValueError, IndexError) as exc:
                raise ValueError(f'{path}:{line}: {exc}') from exc
            ids.add(event_id)
            records.append(row)
    if not records:
        raise ValueError('dataset must contain at least one record')
    return records


class HashTable:
    """Use a list at each slot to keep records whose hashes collide."""

    def __init__(self, size):
        self.slots = [[] for _ in range(size)]
        self.count = 0

    def store(self, pos, record):
        bucket = self.slots[pos]
        for old in bucket:
            if old[0] == record[0]:
                # A retried store should not add the same event twice.
                if old != record:
                    raise ValueError('same event_id has conflicting data')
                return False
        bucket.append(record)
        self.count += 1
        return True


def udp_socket(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(address)
    except BaseException:
        sock.close()
        raise
    return sock
