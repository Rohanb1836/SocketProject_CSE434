# Socket Project — Group 71

A distributed hash table for storm-event records, written in Python for
CSE 434. All program communication uses UDP. Requires Python 3.8 or newer;
no external packages are needed.

## Files

- `manager.py`: registers peers and tracks DHT membership and state.
- `peer.py`: handles terminal commands, record storage, queries, and membership changes.
- `protocol.py`: shared UDP, validation, CSV, and hash-table functions.
- `data/details-1950.csv`: 223 records for the milestone.
- `data/details-1996.csv`: 48,534 records for the full project.
- `docs/Design.pdf`: design details and message diagrams.

## Running the programs

Run from the project folder. Keep all three Python files together and the same
CSV files in `data/` on every peer host. Set `DHT_DATA_DIR` if the data is stored
elsewhere. Hosts must be able to communicate over IPv4 and UDP.

In every terminal:

```bash
export DHT_GROUP=71
```

Check the source files:

```bash
python3 -m py_compile manager.py peer.py protocol.py
```

Start the manager:

```bash
python3 -u manager.py 36500
```

Start each peer in a separate terminal, replacing `MANAGER_IP` with the manager
host's IPv4 address:

```text
python3 -u peer.py MANAGER_IP 36500
```

Register each peer using its own host's IPv4 address and the following ports:

| Peer | Manager communication port | Peer communication port |
|---|---:|---:|
| Alpha | 36501 | 36502 |
| Beta | 36503 | 36504 |
| Gamma | 36505 | 36506 |
| Delta | 36507 | 36508 |
| Epsilon | 36509 | 36510 |
| Zeta | 36511 | 36512 |

For example, in Alpha's terminal, replace `ALPHA_IP` with its host address:

```text
register Alpha ALPHA_IP 36501 36502
```

Group 71 uses ports 36500–36999. For a local rehearsal, use `127.0.0.1` for
all addresses. For separate hosts, use their reachable IPv4 addresses.

## Building the DHT

For the milestone, register Alpha, Beta, and Gamma, then enter this in Alpha:

```text
setup-dht Alpha 3 1950
```

For the full project, register all six peers, then enter this in Alpha:

```text
setup-dht Alpha 5 1996
```

Wait for `TOTAL_VERIFIED` and `DHT_COMPLETE` before continuing. Run `status`
in Alpha to see the ring order. Members are selected randomly; the registered
peer missing from the five-member ring remains Free and can issue queries.

## Commands

Enter commands inside the peer program. `NAME` must be that peer's registered
name. Wait for each operation to complete before starting another.

| Command | Purpose |
|---|---|
| `register NAME IP MPORT PPORT` | Register the peer with its host address and two ports. |
| `setup-dht NAME N YYYY` | Build a DHT with N peers using the selected year's CSV. |
| `query-dht NAME EVENT_ID` | Search for an event from a Free peer. |
| `leave-dht NAME` | Leave the ring and rebuild with the remaining members. |
| `join-dht NAME` | Join the ring from the Free state and rebuild. |
| `teardown-dht NAME` | Delete the DHT; issued by the current leader. |
| `deregister NAME` | Deregister a Free peer and exit its program. |
| `status` | Show the local ring, node ID, and record count. |

Queries also support two steps: `query-dht NAME`, then `find-event EVENT_ID`.
A successful query prints the record fields and visited node IDs. An unsuccessful
query prints a not-found message and visited node IDs.

A leaving peer's clockwise successor becomes the new leader. Joining retains
the current leader. Completion messages are sent automatically. After teardown,
deregister each peer and stop the manager with Ctrl+C.

## Notes

- Peer names must be 1–15 letters. Registered names and ports must be unique.
- CSV files must contain the expected 14 columns and records from the requested year.
- UDP requests are retried, and duplicate control requests are handled using request IDs.
- If setup or a membership change fails or times out, restart all processes.
  Automatic rollback and crash recovery are not implemented.
