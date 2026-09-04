# MemCloud (Python backend)

Peer-to-peer LAN RAM pooling: nodes on the same network donate spare RAM to
each other's `memnode` daemon so a RAM-constrained machine can offload data
to someone else's idle memory instead of hitting local limits. Nothing ever
touches disk; a daemon restart drops everything it was holding. It's a
volatile mesh cache, not a database.

This is a from-scratch Python implementation, built to avoid a set of
concrete bugs found in an earlier Rust prototype's audit (unbounded
allocation from an unchecked length prefix, panics on malformed peer
messages, no tests, no CI, no redundancy). Every one of those has a
direct fix or an explicit, documented trade-off below.

## Layout

```
memnode/
  config.py      size caps, ports, paths -- the numbers that gate every allocation
  protocol.py    bounded length-prefixed framing + message definitions (JSON local, msgpack peer wire)
  blocks.py      the actual RAM store: quota, per-block cap, pinned vs cache, eviction
  security.py    shared-secret HMAC peer authentication (deliberately no encryption -- see below)
  peers.py       peer connections, handshake, capacity-aware placement, heartbeat/pruning, mesh gossip
  discovery.py   mDNS advertise + browse via zeroconf, hands off to peers.connect_to()
  rpc.py         local RPC server (Unix socket + TCP fallback) for the CLI/SDK/dashboard
  daemon.py      wires it all together, the `memnode` entrypoint
memcli/
  cli.py         command-line client -- store, load, peers, connect, stats, streaming
tests/
  test_blocks.py, test_protocol.py, test_security.py    unit tests, no network
  test_peers_integration.py, test_rpc_integration.py    real sockets, two-daemon scenarios
```

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# or: pip install -e .[dev]
```

Requires Python 3.9+. Unix sockets (the primary local RPC transport) need
macOS/Linux; on Windows the daemon automatically falls back to TCP-only
local RPC (`127.0.0.1:7070`) and logs that it did so.

## Running two nodes and demoing a real cross-machine store

On two machines on the same LAN (or two terminals on one machine, using
different `--peer-port`/`--rpc-port` so they don't collide):

```bash
# machine A
export MEMCLOUD_SECRET=your-team-secret
python3 -m memnode.daemon --name alice

# machine B
export MEMCLOUD_SECRET=your-team-secret
python3 -m memnode.daemon --name bob
```

With mDNS enabled (the default), alice and bob find each other automatically
on the LAN within a couple seconds. Across subnets/VLANs where multicast
doesn't reach, connect manually instead:

```bash
memcli connect 192.168.1.42:8080
```

Then, from alice's machine:

```bash
memcli peers                          # confirm bob shows up
memcli store myfile.txt --peer bob    # store directly into bob's RAM
memcli load <block_id>                # fetches it back over the wire from bob
memcli stats                          # confirm it's NOT using alice's local RAM
```

Kill bob's process mid-demo and `memcli peers` on alice will show him gone
within one heartbeat interval (~5s) -- pruned automatically, not left stale.

Large files stream in chunks so the sender's memory footprint stays flat
regardless of source size:

```bash
memcli stream-store big.log --peer bob
memcli stream-load <manifest_block_id> restored.log
```

## Security model: authentication without encryption, on purpose

The reference Rust implementation this is based on used a full Noise-XX
handshake plus ChaCha20-Poly1305 transport encryption. For a same-LAN
project among a known team, that's disproportionate engineering time for
the actual threat model -- eavesdropping on your own trusted wifi is
low-probability, and getting AEAD crypto right is not free.

What's kept instead: **authentication**, not confidentiality. Every `Hello`
is challenged with an HMAC-SHA256 over a fresh nonce, keyed with a secret
every legitimate node shares (`MEMCLOUD_SECRET`). A node that doesn't know
the secret gets a `Deny` and never joins the mesh. This matters because
mDNS *broadcasts* your service to the whole LAN, not just your team --
on shared venue wifi, "anyone can see your daemon" and "anyone can use
your daemon" are different problems, and only the second one is solved by
skipping encryption but keeping auth.

If you need real confidentiality later, `protocol.py`'s `Hello`/`Welcome`
messages already carry the `nonce` field a Noise-XX handshake would need --
swap `security.verify_auth` for a real handshake and wrap
`read_frame`/`write_frame` in an AEAD transport without changing the
message shapes.

**Set `MEMCLOUD_SECRET` to something real before using this beyond local
testing** -- the code falls back to a well-known demo value so a fresh
checkout still boots, and loudly warns in the logs when it's using it.

## What's deliberately simple (and the fast follow-ups if you have time)

- **No replication.** A block lives on exactly one node. If that node's
  daemon dies, the block is gone. `peers.py` already tracks `remote_blocks`
  (which peer holds which block we placed), so a primary+backup write
  (`store_on_peer` to two peers, read tries the first then the second) is
  a bounded, demoable addition on top of what's here.
- **Capacity-aware placement exists but is heartbeat-fresh, not live.**
  `pick_best_peer()` picks the peer that last reported the most free
  quota, refreshed every `HEARTBEAT_INTERVAL_SECONDS` (5s default) via
  Ping/Pong. Fine for a demo; a tighter interval or piggybacking quota on
  every message would make it more responsive.
- **mDNS is LAN-only**, by construction of multicast -- doesn't cross
  subnets/VLANs or work in most container/cloud setups. `memcli connect`
  is the documented manual fallback, not a hidden gap.
- **No leader/coordination layer.** Keys are owned by whichever node's
  `SetKey` call reaches a peer first; there's no conflict resolution if
  two nodes race to claim the same key. Fine for a hackathon demo, worth
  a paragraph in your slides if judges ask about consistency.

## What's already handled (the bugs this was built to avoid)

- **Bounded allocation everywhere.** Every length-prefixed read checks the
  declared size against `MAX_FRAME_SIZE`/`MAX_BLOCK_SIZE` *before*
  allocating or reading the body (`protocol.read_frame`,
  `tests/test_protocol.py::test_read_frame_rejects_oversized_length_without_reading_body`).
- **No panics on malformed/hostile input.** Every peer-message handler and
  every RPC handler is wrapped so a bad message logs and continues instead
  of killing the connection or the daemon
  (`tests/test_rpc_integration.py::test_malformed_json_does_not_kill_the_server`,
  `::test_oversized_frame_is_rejected_gracefully_not_a_crash`).
- **Stale peers get pruned.** A heartbeat loop drops any peer that's gone
  quiet past `PEER_TIMEOUT_SECONDS`, and a broken connection is removed
  immediately rather than lingering in the peer list.
- **Real tests, not just unit tests.** 43 tests total, including
  integration tests that spin up two real daemons on real TCP sockets and
  verify a block placed via one daemon's RPC actually lands in the other
  daemon's memory and can be read back over the wire.

## Running the tests

```bash
pip install -r requirements.txt
pytest -v
```

43 tests, ~1.5s, no network access required (everything binds to
127.0.0.1 on ephemeral high ports chosen per test file to avoid clashes).

## CLI reference

| Command | What it does |
|---|---|
| `memcli store <file\|-> [--mode pinned\|cache] [--peer NAME \| --auto]` | Store a file's contents as a block |
| `memcli load <block_id> [--out file]` | Load a block, locally or over the wire |
| `memcli free <block_id>` | Free a block |
| `memcli peers` | List connected peers and their last-known free quota |
| `memcli connect <host:port>` | Manually connect to a peer (mDNS's cross-subnet fallback) |
| `memcli stats` | This node's quota/usage/peer count |
| `memcli stream-store <file> [--peer NAME]` | Chunked upload for large files |
| `memcli stream-load <manifest_id> <out>` | Reassemble a file stored with `stream-store` |
