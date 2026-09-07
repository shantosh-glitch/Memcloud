# MemCloud — distributed RAM over a LAN

Use the idle RAM of other laptops on the network as a memory tier that sits
between local RAM and disk. When an application on Node A runs out of local
memory budget, its data is placed in **Node B's physical RAM** instead of
falling straight to disk.

Phase 1 (this build): distributed RAM + image cache demo, over mutual TLS.
Phase 2 (later): failure recovery, real LLM engine integration.

All data-plane traffic is encrypted and authenticated with mutual TLS. See
**LLM_NOTES.md** for what LLM workloads can and cannot use remote RAM for.

---

## Requirements

- Python **3.8 or newer**. Nothing else. No `pip install`, no internet.
- Both laptops on the **same network**, able to reach each other by IP.

Check on both machines:

```bash
python3 --version
```

---

## Quick start — two laptops

Copy the `memcloud/` folder to both laptops.

### Step 0: generate the cluster certificate (once)

On **one** laptop:

```bash
cd memcloud
python3 -m memcloud.security --out cluster
```

This writes `cluster.pem` and `cluster.key` and prints a SHA-256 fingerprint.
**Copy both files to the other laptop.** A node that does not hold this key
pair cannot join, cannot read blocks, and cannot impersonate a node. The
fingerprint printed at startup must be identical on both machines.

`cluster.key` is a secret. Do not commit it.

**Laptop B** (the one lending RAM). Run first:

```bash
cd memcloud
python3 -m memcloud.node --name laptop-b --reserve-pct 25 --budget-mb 64
```

It prints its LAN address, e.g. `data plane 192.168.1.42:47801`.

**Laptop A** (the one running the app). Use B's address:

```bash
cd memcloud
python3 -m memcloud.node --name laptop-a --peer 192.168.1.42:47801 --budget-mb 512
```

Open **http://127.0.0.1:5892** on Laptop A. Both nodes should appear within
a few seconds.

> `--peer` is the reliable path. UDP broadcast auto-discovery also runs, but
> many campus and office networks block broadcast, so always pass `--peer`
> for a demo you cannot afford to have fail.

---

## Running the demo

In the dashboard on Laptop A:

1. **Seed cache** — generates N frames, writes them to a disk cold store, and
   loads them into MemCloud. Frames fill the local budget first, then overflow
   into Laptop B's RAM. Watch B's "hosting for peers" figure climb.
2. **Run reads** — random frame reads. The table shows `LOCAL` / `PEER` /
   `DISK` per read with measured latency.
3. **Spill 25% to peers** — moves the least-recently-used local blocks to B.
   This is the "A's RAM drops, B's RAM rises" moment.
4. The **frame preview** performs a real read; if that frame lives on B, those
   pixels crossed the network to render.

Everything is also available over HTTP:

```
GET  /api/state              full cluster + cache state (dashboard polls this)
GET  /api/cache              cache report only
GET  /api/nodes              node list
GET  /api/events             SSE event stream
GET  /api/frame/<id>         the frame itself, as image/bmp
                             (X-MemCloud-Source header says LOCAL/PEER/DISK)
POST /api/demo/seed?frames=400
POST /api/demo/reads?count=200
POST /api/demo/spill?fraction=0.25
POST /api/demo/reset
```

---

## Sizing for 32 GB laptops

Default frame is 640x480 24-bit BMP = **921,654 bytes** (~0.88 MB).

| frames | RAM needed | disk needed |
|---|---|---|
| 500   | ~440 MB | ~440 MB |
| 1,000 | ~880 MB | ~880 MB |
| 4,000 | ~3.5 GB | ~3.5 GB |

To show ~3 GB of remote RAM, seed ~4,000 frames with a local budget of
512 MB on A and a reserve of 6 GB on B:

```bash
# Laptop B
python3 -m memcloud.node --name laptop-b --reserve-mb 6144 --budget-mb 64

# Laptop A
python3 -m memcloud.node --name laptop-a --peer <B-ip>:47801 \
        --budget-mb 512 --reserve-mb 512
```

Generation runs at roughly 5 ms/frame, so 4,000 frames takes ~20 s of CPU
plus disk write time.

---

## Options

| flag | default | meaning |
|---|---|---|
| `--name` | hostname | node label in the UI |
| `--peer HOST:PORT` | — | manual peer, repeatable |
| `--budget-mb` | 3% of RAM | local app cache budget; overflow point |
| `--reserve-mb` | 20% of RAM | RAM this node offers to peers |
| `--min-free-mb` | 1024 | never let host available RAM drop below this |
| `--data-port` | 47801 | TCP data plane |
| `--api-port` | 5892 | dashboard / HTTP API |
| `--discovery-port` | 47800 | UDP broadcast |
| `--cluster` | `default` | must match across nodes |
| `--frames-dir` | `~/.memcloud/<name>/frames` | disk cold store |
| `--tls-cert` | `cluster.pem` | shared cluster certificate |
| `--tls-key` | `cluster.key` | shared cluster private key |
| `--insecure` | off | disable TLS. Debugging only. |
| `--chunk-mb` | 8 | chunk size for large objects / tensors |

---

## Architecture

```
              APPLICATION  (image cache)
                    |
              MemCloud client  ── local budget full? ──┐
                    |                                   |
              local RAM dict                     Placement scheduler
              (this process)                            |
                                                  pick peer by
                                                  free reserve − RTT
                                                        |
                                                  TCP data plane
                                                        |
                                                  Peer worker
                                                  self._blocks[key] = bytes
                                                  (peer's physical RAM)
                    |
              miss on both ──> disk cold store
```

| module | layer | role |
|---|---|---|
| `discovery.py` | 1 | UDP beacons + manual peers, RAM monitoring via STAT probes |
| `client.py` | 2, 4 | placement scheduler, remote put/get, LRU spill |
| `worker.py` | 3 | holds peer blocks in this process's heap, admission control |
| `protocol.py` | — | length-prefixed frames over TLS, range reads, MGET, connection reuse |
| `imagecache.py` | 7 | LOCAL → PEER → DISK tiered read path, CRC32 verification |
| `security.py` | — | cluster cert generation, mutual-TLS contexts |
| `llm.py` | 8 | tensor store, prefix KV cache over chunked objects |
| `api.py` + `dashboard.py` | — | HTTP API, SSE, dashboard |
| `node.py` | — | wires it together |

---

## What is actually proven, and what is not

Verified by `tests/test_e2e.py` (43 assertions, all passing):

- A block that exceeds the local budget is sent to a peer, and the peer's
  in-process byte count grows by exactly that amount.
- The block reads back with an identical CRC32 — the bytes on the wire are
  the bytes that were stored.
- Every cached frame is CRC-verified on every read; zero failures.
- LRU spill moves blocks off A and onto B, and spilled frames stay readable.
- Deleting a block makes the next read fall through to the disk tier.
- The data plane negotiates TLS 1.3 with AES-256-GCM, and both nodes present
  the same cluster certificate.
- A plaintext client is rejected. A TLS client *without* the cluster
  certificate is also rejected (`PEER_DID_NOT_RETURN_A_CERTIFICATE`).
- A 60 MB object stripes into 8 chunks across both nodes and reassembles
  byte-exact; a range read across a chunk boundary is exact.
- A prefix KV cache round-trips identically; a prefix differing by one token
  is a clean miss.

Run it yourself:

```bash
python3 tests/test_e2e.py
```

**Not yet implemented.** Say so if asked:

- **No failure recovery.** Kill Node B and every block it held is gone;
  reads fall through to the disk cold store only because this demo happens to
  keep one. There is no replication and no rebuild. That is Phase 2.
- **One shared key for the whole cluster.** TLS authenticates *membership*,
  not individual identity — any member could impersonate any other member.
  Per-node certificates signed by a cluster CA would fix this and is a
  contained change, but is not done. There is also no revocation.
- **No real LLM engine integration.** `llm.py` is the storage side only. It
  has never been wired into llama.cpp, vLLM, or Ollama. See LLM_NOTES.md.
- **No VRAM pooling.** Nothing here makes two GPUs act as one.
- **Disk-tier latency is optimistic.** The cold store was just written, so it
  is warm in the OS page cache. Real cold-disk reads are slower than what the
  DISK row shows. Do not quote the disk number as a worst case.
- **Local reads are dict lookups**, so they measure near 0 ms. That is real,
  but it is a pointer dereference, not a memory-copy benchmark.

### TLS cost, measured

On a 1-vCPU container over loopback, single run, high variance:

| | 64 MB write | 64 MB read |
|---|---|---|
| mutual TLS 1.3 (AES-256-GCM) | ~389 MB/s | ~249 MB/s |
| plaintext (`--insecure`) | ~621 MB/s | ~190 MB/s |

Write was roughly 37% slower under TLS. Read came out *faster* under TLS,
which is measurement noise on one core, not a real result — treat the read row
as inconclusive.

The point that survives the noise: **both figures are well above gigabit LAN
throughput (~110 MB/s)**, so on a real two-laptop network the link is the
bottleneck, not the crypto. The TLS handshake is paid once per peer because
connections are pooled and reused, not once per block.
