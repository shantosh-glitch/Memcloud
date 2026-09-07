"""
End-to-end proof: two MemCloud nodes, real TCP sockets, real block transfer.

Run:  python3 tests/test_e2e.py
"""

import os
import shutil
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memcloud import meminfo
from memcloud.imagecache import SOURCE_DISK, SOURCE_LOCAL, SOURCE_PEER
from memcloud.node import Node, build_parser

MB = 1024 * 1024
ROOT = "/tmp/memcloud-e2e"
PASS, FAIL = [], []


def check(label, cond, detail=""):
    (PASS if cond else FAIL).append(label)
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}" + (f"  {detail}" if detail else ""))


def make_node(name, data_port, api_port, budget_mb, reserve_mb, peers=()):
    argv = [
        "--name", name,
        "--data-port", str(data_port),
        "--api-port", str(api_port),
        "--discovery-port", "47899",
        "--budget-mb", str(budget_mb),
        "--reserve-mb", str(reserve_mb),
        "--min-free-mb", "200",
        "--frames-dir", os.path.join(ROOT, name, "frames"),
        "--chunk-mb", "4",
        "--tls-cert", os.path.join(ROOT, "cluster.pem"),
        "--tls-key", os.path.join(ROOT, "cluster.key"),
    ]
    for p in peers:
        argv += ["--peer", p]
    node = Node(build_parser().parse_args(argv))
    node.start()
    return node


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    print("\n=== MemCloud end-to-end test: 2 nodes, mutual TLS ===\n")

    b = make_node("laptop-b", 47881, 5991, budget_mb=4, reserve_mb=700)
    a = make_node("laptop-a", 47882, 5992, budget_mb=40, reserve_mb=40,
                  peers=["127.0.0.1:47881"])

    # ---- Layer 1: discovery ----
    print("\n[1] Discovery")
    for _ in range(40):
        if a.discovery.table.online():
            break
        time.sleep(0.25)
    peers = a.discovery.table.online()
    check("A discovers B", len(peers) == 1, f"peers={[p.name for p in peers]}")
    if not peers:
        return 1
    peer = peers[0]
    check("B's real node id resolved", peer.node_id == b.node_id, peer.node_id)
    check("B reports its RAM", peer.ram_total > 0, meminfo.human(peer.ram_total))
    check("B advertises free reserve", peer.reserve_free > 600 * MB,
          meminfo.human(peer.reserve_free))

    # ---- Layer 3/4: remote put/get ----
    print("\n[2] Remote allocation and read-back")
    blob = os.urandom(3 * MB)
    crc = zlib.crc32(blob)
    where = a.memcloud.put("probe", blob)
    check("small block stays local", where == "local", where)

    big = os.urandom(50 * MB)  # exceeds A's 40 MB budget -> must go to B
    big_crc = zlib.crc32(big)
    where_big = a.memcloud.put("bigblock", big)
    check("oversized block placed on peer", where_big == b.node_id, where_big)
    check("B physically holds it", b.store.used >= 50 * MB,
          meminfo.human(b.store.used))

    got, src, ms = a.memcloud.get("bigblock")
    check("read back from peer RAM", src == b.node_id, f"{src} in {ms:.2f} ms")
    check("bytes identical over the wire", zlib.crc32(got) == big_crc)
    got2, src2, _ = a.memcloud.get("probe")
    check("local read still local", src2 == "local" and zlib.crc32(got2) == crc)

    # ---- worker admission guard ----
    print("\n[3] Admission control")
    huge = os.urandom(4 * MB)
    a.memcloud.put("x1", huge)
    ok, why = b.store.can_accept(10 * 1024 * MB)
    check("peer refuses beyond reserve", not ok, why[:60])

    # ---- Layer 7: image cache ----
    print("\n[4] Image cache (Layer 7)")
    a.memcloud.clear()
    res = a.cache.seed(400, 320, 240)  # ~230 KB/frame -> ~92 MB, over A's 40 MB budget
    total = res["placed_local"] + res["placed_remote"] + res["disk_only"]
    print(f"      seeded {res['frames']} frames, {meminfo.human(res['total_bytes'])} "
          f"in {res['seconds']}s")
    print(f"      local={res['placed_local']} remote={res['placed_remote']} "
          f"disk_only={res['disk_only']}")
    check("all frames accounted for", total == res["frames"])
    check("some frames in LOCAL RAM", res["placed_local"] > 0)
    check("overflow reached PEER RAM", res["placed_remote"] > 0)

    ids = sorted(a.cache.frames.keys())
    entries = a.cache.read_many(ids)
    by_src = {}
    for e in entries:
        by_src[e["source"]] = by_src.get(e["source"], 0) + 1
    print(f"      read sources: {by_src}")
    check("LOCAL hits recorded", by_src.get(SOURCE_LOCAL, 0) > 0)
    check("PEER hits recorded", by_src.get(SOURCE_PEER, 0) > 0)
    check("every frame checksum verified",
          all(e.get("verified") for e in entries),
          f"{sum(1 for e in entries if not e.get('verified'))} failures")

    rep = a.cache.report()
    lat = rep["latency"]
    print(f"      avg local {lat['LOCAL']['avg_ms']} ms | "
          f"peer {lat['PEER']['avg_ms']} ms | disk {lat['DISK']['avg_ms']} ms")
    check("peer read slower than local (network cost visible)",
          (lat["PEER"]["avg_ms"] or 0) > (lat["LOCAL"]["avg_ms"] or 0))

    # ---- disk tier ----
    print("\n[5] Disk fallback tier")
    victim = ids[0]
    a.memcloud.delete(victim)
    _, entry = a.cache.read(victim)
    check("evicted frame served from DISK", entry["source"] == SOURCE_DISK,
          f"{entry['ms']:.2f} ms")
    check("disk copy is byte-identical", entry["verified"])

    # ---- Layer 6: spill ----
    print("\n[6] Memory-pressure spill")
    before_local = a.memcloud.local_used
    before_b = b.store.used
    sp = a.memcloud.spill(int(before_local * 0.5))
    after_local = a.memcloud.local_used
    after_b = b.store.used
    print(f"      A local {meminfo.human(before_local)} -> {meminfo.human(after_local)}")
    print(f"      B hosted {meminfo.human(before_b)} -> {meminfo.human(after_b)}")
    check("blocks moved off A", sp["blocks_moved"] > 0, str(sp["blocks_moved"]))
    check("A local RAM dropped", after_local < before_local)
    check("B hosted RAM rose", after_b > before_b)
    spilled = ids[1]
    _, e2 = a.cache.read(spilled)
    check("spilled frames still readable", e2["verified"], e2["source"])

    # ---- HTTP API ----
    print("\n[7] HTTP API + dashboard")
    import urllib.request
    st = urllib.request.urlopen(f"http://127.0.0.1:{a.api_port}/api/state", timeout=5)
    import json
    js = json.loads(st.read())
    check("/api/state serves cluster view", len(js["nodes"]) == 2,
          str([n["name"] for n in js["nodes"]]))
    html = urllib.request.urlopen(f"http://127.0.0.1:{a.api_port}/", timeout=5).read()
    check("dashboard HTML served", b"MemCloud" in html and len(html) > 4000,
          f"{len(html)} bytes")
    fr = urllib.request.urlopen(
        f"http://127.0.0.1:{a.api_port}/api/frame/{ids[5]}", timeout=10
    )
    img = fr.read()
    check("frame served as real BMP", img[:2] == b"BM",
          f"{len(img)} bytes, source={fr.headers.get('X-MemCloud-Source')}, "
          f"{fr.headers.get('X-MemCloud-Latency-Ms')} ms")

    # ---- transport security ----
    print("\n[8] Transport security")
    from memcloud import protocol, security
    check("TLS enabled on both nodes", a.tls_enabled and b.tls_enabled)
    check("both nodes share one cert fingerprint",
          a.tls_fingerprint == b.tls_fingerprint, a.tls_fingerprint[:23] + "...")
    sock = protocol.connect("127.0.0.1", b.data_port, ssl_ctx=a.ssl_client_ctx)
    desc = security.describe(sock)
    check("data plane negotiates TLS", "TLS" in desc, desc)
    check("cipher is an AEAD suite", "GCM" in desc or "CHACHA" in desc.upper(), desc)
    check("server presents the cluster cert", bool(sock.getpeercert()))
    sock.close()

    # a client with no certificate must be rejected
    import ssl as _ssl
    rejected = False
    try:
        bare = protocol.connect("127.0.0.1", b.data_port, timeout=5.0)
        protocol.request(bare, protocol.OP_STAT)
        bare.close()
    except Exception:
        rejected = True
    check("plaintext client is rejected", rejected)

    anon = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    anon.check_hostname = False
    anon.verify_mode = _ssl.CERT_NONE
    rejected2 = False
    try:
        s2 = protocol.connect("127.0.0.1", b.data_port, timeout=5.0, ssl_ctx=anon)
        protocol.request(s2, protocol.OP_STAT)
        s2.close()
    except Exception:
        rejected2 = True
    check("TLS client without the cluster cert is rejected", rejected2)

    # ---- range reads + chunked objects ----
    print("\n[9] Range reads and chunked objects")
    a.memcloud.clear()          # start from a known-empty local budget
    a.cache.reset()
    payload = bytes((i * 7 + 3) & 0xFF for i in range(250)) * 240000  # 60 MB
    # A's local budget is 40 MB, so a 60 MB object cannot fit locally.
    man = a.memcloud.put_object("obj1", payload, chunk_bytes=8 * MB)
    print(f"      object {meminfo.human(man['size'])} in {man['chunks']} chunks "
          f"-> {man['placement'].count('local')} local, "
          f"{sum(1 for p in man['placement'] if p != 'local')} remote")
    check("object striped into chunks", man["chunks"] == 8, str(man["chunks"]))
    check("some chunks landed on the peer",
          any(p != "local" for p in man["placement"]))
    whole, _ = a.memcloud.get_object("obj1")
    check("object reassembles byte-exact", whole == payload,
          f"{len(whole)} bytes")
    sl = a.memcloud.get_object_range("obj1", 8 * MB - 100, 300)
    check("range read spanning a chunk boundary is exact",
          sl == payload[8 * MB - 100 : 8 * MB + 200], f"{len(sl)} bytes")
    b_before = b.store.used
    check("peer holds the overflow chunks", b_before > 8 * MB,
          meminfo.human(b_before))

    # ---- LLM prefix KV store ----
    print("\n[10] LLM prefix KV cache")
    from memcloud.llm import PrefixKVStore, prefix_key
    est = PrefixKVStore.estimate_bytes(4096, layers=32, kv_heads=8, head_dim=128)
    print(f"      Llama-3-8B-shaped 4096-token KV cache would be "
          f"{meminfo.human(est)} (arithmetic estimate)")
    tokens = list(range(1, 513))
    kv = bytes(range(256)) * 16384  # 4 MB stand-in for a real KV tensor
    rec = a.kv.store("llama3-8b", "tok-v1", tokens, kv,
                     shape=[len(kv)], dtype="uint8")
    print(f"      stored {meminfo.human(rec['bytes'])} in {rec['chunks']} chunks, "
          f"{rec['remote_chunks']} on the peer")
    check("KV cache stored", rec["bytes"] == len(kv))
    got, rec2, ms = a.kv.fetch("llama3-8b", "tok-v1", tokens)
    check("KV cache hit returns identical bytes",
          bytes(got) == kv, f"{ms:.1f} ms")
    miss, _, _ = a.kv.fetch("llama3-8b", "tok-v1", tokens + [999])
    check("different prefix is a clean miss", miss is None)
    check("prefix key is content-addressed",
          prefix_key("m", "t", [1, 2]) != prefix_key("m", "t", [1, 3]))

    a.stop()
    b.stop()

    print(f"\n=== {len(PASS)} passed, {len(FAIL)} failed ===")
    if FAIL:
        for f in FAIL:
            print("  FAILED:", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
