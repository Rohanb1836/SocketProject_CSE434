# Socket Project Milestone

This is the Python 3.8+ milestone implementation for CSE 434. It uses only the
Python standard library, and all program communication uses UDP.

**Current status:** The project is set up for Group 70. The included 1950 CSV
has completed a local three-peer run. The remaining submission work is a
recorded two-host demo and an accessible YouTube link.

## What's in this folder

- `manager.py` runs the manager. It accepts registrations, selects the DHT ring,
  and marks the DHT ready after the leader reports completion.
- `peer.py` runs a peer. It reads commands from the terminal, participates in
  the ring, and stores records.
- `protocol.py` contains the UDP message helpers, validation, CSV loader, and
  hash table.
- `tests/test_milestone.py` contains the automated checks.
- `data/details-1950.csv` is the 1950 data used by the local run.
- `tools/demo_commands.py` prints a command sheet once you supply two host IPs.
- `docs/Design.pdf` contains the design document and message diagrams.
- `docs/DEMO_GUIDE.md` has a short recording checklist.
- `submission.json` holds the submission details to finish after recording.

## Before you run the demo

1. Use the same copy of this folder on both computers.
2. Make sure both computers can reach each other over IPv4 and UDP.
3. Set `DHT_GROUP=70` in every terminal. Group 70 may use ports **36000 through
   36499**.
4. Use seven different ports: one for the manager and two for each of the three
   peers. The peer ports are separate because a peer talks both to the manager
   and to the other peers in the DHT.
5. Keep `data/details-1950.csv` in the project folder on the Alpha/leader host.
   The program looks in `data/` by default. Set `DHT_DATA_DIR` only if you place
   the data somewhere else.

To generate commands for a two-computer demo, run:

```bash
python3 tools/demo_commands.py 70 HOST_A_IPV4 HOST_B_IPV4
```

Replace the two placeholders with the real IPv4 addresses. Run the manager and
Alpha on host A, then run Beta and Gamma on host B.

## Quick local rehearsal

You can rehearse on one computer using four Terminal windows. This verifies the
program, but the graded recording still needs two distinct end hosts.

In every window, first run:

```bash
export DHT_GROUP=70
```

Start the manager in the first window:

```bash
python3 -u manager.py 36000
```

Start Alpha in a second window:

```bash
python3 -u peer.py 127.0.0.1 36000
```

Then enter:

```text
register Alpha 127.0.0.1 36001 36002
```

Start Beta and Gamma in two more windows using the same `peer.py` command. Enter
these registrations in their own windows:

```text
register Beta 127.0.0.1 36003 36004
register Gamma 127.0.0.1 36005 36006
```

After all three registrations return `SUCCESS`, enter this in Alpha:

```text
setup-dht Alpha 3 1950
```

At the end, Alpha should print `TOTAL_VERIFIED` and `DHT_COMPLETE`. The manager
should print a final state of `READY`.

## Checks to run before recording

Run these from the project folder:

```bash
python3 --version
python3 -X pycache_prefix=./.pycache -m py_compile manager.py peer.py protocol.py
python3 -B -m unittest discover -s tests -v
```

The automated network tests use localhost and temporary test records. They are
useful for checking the code, but they do not replace the required 1950 two-host
demo.

## What to show in the recording

The milestone video needs to show:

1. The syntax check or Python setup.
2. The manager and three peers running on at least two computers.
3. All three peers registering successfully.
4. Alpha running `setup-dht Alpha 3 1950`.
5. The record count at each peer, a verified total, and the manager reaching
   `READY`.

Keep the full recording under seven minutes, include audio, and upload it to an
accessible YouTube link before the deadline. Record the timestamps for each
required step in `submission.json` and the design document.

## Notes about the implementation

- Peer names must be 1-15 letters. Names and ports cannot be reused.
- The manager handles one request at a time. It rejects new setup work while a
  DHT is being built.
- Peers retry UDP messages every 0.5 seconds, up to ten times. A retry uses the
  same request ID so the manager does not repeat an action.
- The program is designed for the milestone. If a setup run is interrupted,
  stop all four processes and start again.
- The CSV must contain the expected 14 columns and only records from the year
  requested by `setup-dht`.

## Packaging

Before submitting, add the actual contributors, YouTube URL, and video
timestamps to `submission.json`. Regenerate and review the design PDF if its
evidence page changes:

```bash
python3 tools/build_design.py
```

Create the final archive with:

```bash
python3 tools/package.py
```

Follow the course's authorship and disclosure policy, and make sure you can
explain how the code works before submitting it.
