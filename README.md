# Socket Project — Group 71

This is the Python 3.8+ implementation for CSE 434. It uses only the
Python standard library, and all program communication uses UDP.

The full project extends the milestone with event queries, leaving and joining
the DHT, rebuilding the ring, teardown, and deregistration.

## What's in this folder

- `manager.py` runs the manager. It accepts registrations, selects the DHT ring,
  and tracks membership and DHT state.
- `peer.py` runs a peer. It reads commands from the terminal, participates in
  the ring, stores records, and handles queries and membership changes.
- `protocol.py` contains the UDP message helpers, validation, CSV loader, and
  hash table. Keep it alongside both programs because they import it.
- `data/details-1950.csv` contains the 223 records used for the milestone.
- `data/details-1996.csv` contains the 48,534 records used for the full project.
- `docs/Design.pdf` contains the design document and message diagrams.

## Before you run the demo

1. Use the same source files and datasets on every host. Run commands from the
   project folder.
2. Make sure the hosts can reach each other over IPv4 and UDP.
3. Set `DHT_GROUP=71` in every terminal. Group 71 may use ports **36500 through
   36999**.
4. Use a separate port for the manager and two distinct ports for each peer.
   The peer ports are separate because a peer talks both to the manager and
   to the other peers in the DHT.
5. Keep the CSV files in `data/` on every peer host. Any member may become the
   leader after a departure. Set `DHT_DATA_DIR` only if the data is elsewhere.

## Quick local rehearsal

You can rehearse the milestone on one computer using four Terminal windows.
For the full project, use seven windows: one manager and six peers. Local
rehearsal does not replace the final recording across four distinct hosts.

In every window, first run:

```bash
export DHT_GROUP=71
```

Start the manager in the first window:

```bash
python3 -u manager.py 36500
```

Start Alpha in a second window:

```bash
python3 -u peer.py 127.0.0.1 36500
```

Then enter:

```text
register Alpha 127.0.0.1 36501 36502
```

Start Beta and Gamma in two more windows using the same `peer.py` command. Enter
these registrations in their own windows:

```text
register Beta 127.0.0.1 36503 36504
register Gamma 127.0.0.1 36505 36506
```

For the milestone rehearsal, after all three peers report `REGISTERED`, enter
this in Alpha:

```text
setup-dht Alpha 3 1950
```

At the end, Alpha should print `TOTAL_VERIFIED` and `DHT_COMPLETE`. The manager
should report a state of `READY`.

## Full project setup

Start a fresh manager and six peers for the full demonstration. One possible
layout uses four distinct hosts:

| Host | Processes |
|---|---|
| A | Manager, Beta |
| B | Alpha, Gamma |
| C | Delta, Epsilon |
| D | Zeta |

Start the manager on host A with `python3 -u manager.py 36500`. Start each peer
in its own terminal with the following command, replacing `MANAGER_IP` with
host A's actual IPv4 address:

```text
python3 -u peer.py MANAGER_IP 36500
```

Register each peer using its own host's IPv4 address and these ports:

| Peer | Manager communication port | Peer communication port |
|---|---:|---:|
| Alpha | 36501 | 36502 |
| Beta | 36503 | 36504 |
| Gamma | 36505 | 36506 |
| Delta | 36507 | 36508 |
| Epsilon | 36509 | 36510 |
| Zeta | 36511 | 36512 |

For example, enter this in Alpha, replacing `ALPHA_IP` with its host address:

```text
register Alpha ALPHA_IP 36501 36502
```

For a local rehearsal, use `127.0.0.1` for all addresses. For separate hosts,
use their reachable IPv4 addresses.

After all six peers report `REGISTERED`, enter this in Alpha:

```text
setup-dht Alpha 5 1996
```

Wait for `TOTAL_VERIFIED` to show 48,534 records and for `DHT_COMPLETE`.
Then enter `status` in Alpha to see the ring order. Alpha has ID 0. The other
four members are selected randomly, so identify the Free peer by finding the
registered name missing from the ring.

## Full project commands

Enter commands inside the peer program, using that process's registered name.

| Command | Purpose |
|---|---|
| `setup-dht NAME N YYYY` | Build a DHT of N peers using the selected year's CSV. |
| `query-dht NAME EVENT_ID` | Request access and search for a storm event from a Free peer. |
| `query-dht NAME` | Request access before entering `find-event EVENT_ID`. |
| `leave-dht NAME` | Leave the DHT and rebuild it with the remaining members. |
| `join-dht NAME` | Join an existing DHT from the Free state and rebuild it. |
| `teardown-dht NAME` | Delete the DHT; only the current leader may issue this. |
| `deregister NAME` | Remove a Free peer's registration and exit its program. |
| `status` | Display the local ring, node ID, and stored record count. |

Completion messages (`dht-complete`, `dht-rebuilt`, and `teardown-complete`)
are sent automatically. Wait for each operation to finish before starting
the next one.

## Checks to run before recording

Run these from the project folder on each host:

```bash
python3 --version
python3 -m py_compile manager.py peer.py protocol.py
```

On the Linux demo hosts, show the hostname and IPv4 addresses:

```bash
hostname
hostname -I
```

Verify registration, record totals, queries, and membership changes during a
rehearsal before recording. Syntax checks alone do not verify network behavior.

## What to show in the recording

The milestone demonstration uses three peers on two hosts with the 1950 data.
The full-project demonstration uses six peers across at least four distinct
hosts and follows this sequence:

1. Show the Python setup, syntax checks, and distinct hosts.
2. Start the manager and register all six peers.
3. Run `setup-dht Alpha 5 1996`; show peer counts, the verified total, and
   successful completion.
4. From the Free peer, run `query-dht NAME EVENT_ID` for each ID below,
   replacing `NAME` with that peer's name. Show the record fields or not-found
   message and the visited node IDs.

   | Event ID | Expected result in the supplied 1996 data |
   |---|---|
   | 5536849 | Found |
   | 2402920 | Not found |
   | 5539287 | Found |
   | 55770111 | Not found |

5. In Alpha, run `leave-dht Alpha`. Wait for `LEFT_DHT`, then run
   `query-dht Alpha 5536849`. Alpha's previous clockwise successor becomes
   the new leader.
6. In Alpha, run `join-dht Alpha` and wait for `JOINED_DHT`. From the peer
   that remained outside the ring, query event 5539287 again.
7. In the current leader's terminal, run `teardown-dht NAME` with that leader's
   name. Wait for `TEARDOWN_COMPLETE`.
8. Run `deregister NAME` in every peer's terminal. After all peers exit,
   stop the manager with Ctrl+C.

Keep the full-project video within 15 minutes, include audio, and upload it to
an accessible YouTube link. Add the actual timestamps for the assignment's
required steps to the design document.

## Notes about the implementation

- Peer names must be 1–15 letters. Registered names and ports must be unique.
- The manager handles one request at a time and checks whether commands are
  valid for the current DHT and peer states.
- UDP requests are retried, and request IDs are reused so duplicate control
  messages do not repeat an action.
- Records are hashed using `pos = event_id % s`, where `s` is the first prime
  greater than twice the record count. The owning node is `pos % n`.
- Queries search the current node, then a randomly chosen unvisited member.
  The result is returned directly to the requesting peer.
- Joining or leaving rebuilds the DHT and redistributes records. The leaving
  peer's clockwise successor becomes the leader; joining retains the leader.
- The CSV must contain the expected 14 columns and only records from the year
  requested by `setup-dht`. Each peer checks that its dataset matches.
- If setup or a membership change fails or times out, stop all processes and
  restart. Automatic rollback and crash recovery are not implemented.

## Packaging

Finish `docs/Design.pdf` with contributor details, the final YouTube URL,
actual video timestamps, and a screenshot of the real Git history.

Create `Socket Project 71.zip` containing `manager.py`, `peer.py`,
`protocol.py`, this README, the `data/` CSV files, and `docs/Design.pdf`.
Preserve the `data/` and `docs/` folder structure. Exclude `.git`, Python
caches, and local logs. Open the ZIP and check its contents before submitting.
