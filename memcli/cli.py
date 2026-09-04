"""
memcli -- talk to a local memnode daemon over its local RPC socket.
Tries the Unix socket first (fast, default), falls back to TCP
127.0.0.1:7070 automatically (e.g. on platforms without Unix sockets).
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys

from memnode import config, protocol
from memnode.rpc_client import connect_rpc


async def _call(request: dict) -> dict:
    try:
        reader, writer = await connect_rpc()
    except OSError as e:
        return {"error": f"could not reach memnode daemon -- is it running? ({e})"}
    try:
        await protocol.write_frame(writer, protocol.encode_rpc(request))
        raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE)
        return protocol.decode_rpc(raw)
    finally:
        writer.close()


def _die(result: dict) -> None:
    print(json.dumps(result), file=sys.stderr)
    sys.exit(1)


def cmd_store(args: argparse.Namespace) -> None:
    data = sys.stdin.buffer.read() if args.file == "-" else open(args.file, "rb").read()
    request = {"cmd": "store", "data_b64": base64.b64encode(data).decode(), "mode": args.mode}
    if args.peer:
        request["peer"] = args.peer
    elif args.auto:
        request["auto_remote"] = True
    result = asyncio.run(_call(request))
    if "error" in result:
        _die(result)
    print(json.dumps(result))


def cmd_load(args: argparse.Namespace) -> None:
    result = asyncio.run(_call({"cmd": "load", "block_id": args.block_id}))
    if "error" in result:
        _die(result)
    data = base64.b64decode(result["data_b64"])
    if args.out:
        with open(args.out, "wb") as f:
            f.write(data)
        print(f"wrote {len(data)} bytes to {args.out} (from {result['location']})")
    else:
        sys.stdout.buffer.write(data)


def cmd_free(args: argparse.Namespace) -> None:
    print(json.dumps(asyncio.run(_call({"cmd": "free", "block_id": args.block_id}))))


def cmd_peers(args: argparse.Namespace) -> None:
    result = asyncio.run(_call({"cmd": "peers"}))
    if "error" in result:
        _die(result)
    peers = result.get("peers", [])
    if not peers:
        print("(no connected peers)")
        return
    for p in peers:
        free_mb = p["free_quota_bytes"] / (1024 * 1024)
        print(f"{p['name']:20s} {p['node_id'][:8]}  {p['host']}:{p['port']}  free={free_mb:.1f}MB")


def cmd_connect(args: argparse.Namespace) -> None:
    host, _, port = args.address.partition(":")
    if not port:
        _die({"error": "expected host:port"})
    result = asyncio.run(_call({"cmd": "connect", "host": host, "port": int(port)}))
    print(json.dumps(result))


def cmd_stats(args: argparse.Namespace) -> None:
    result = asyncio.run(_call({"cmd": "stats"}))
    if "error" in result:
        _die(result)
    print(json.dumps(result.get("stats", result), indent=2))


def cmd_stream_store(args: argparse.Namespace) -> None:
    """
    Chunked upload for large files: splits into STREAM_CHUNK_SIZE pieces,
    stores each as its own block (so the local footprint while uploading
    stays ~one chunk, regardless of source file size), then stores a
    small manifest block (ordered list of chunk block_ids) -- the handle
    you pass to `stream-load` to reassemble the file.
    """
    chunk_ids = []
    with open(args.file, "rb") as f:
        while True:
            chunk = f.read(config.STREAM_CHUNK_SIZE)
            if not chunk:
                break
            request = {"cmd": "store", "data_b64": base64.b64encode(chunk).decode(), "mode": args.mode}
            if args.peer:
                request["peer"] = args.peer
            result = asyncio.run(_call(request))
            if "error" in result:
                _die(result)
            chunk_ids.append(result["block_id"])

    manifest = json.dumps(chunk_ids).encode()
    request = {"cmd": "store", "data_b64": base64.b64encode(manifest).decode(), "mode": "pinned"}
    if args.peer:
        request["peer"] = args.peer
    result = asyncio.run(_call(request))
    if "error" in result:
        _die(result)
    print(json.dumps({"manifest_block_id": result.get("block_id"), "chunks": len(chunk_ids)}))


def cmd_stream_load(args: argparse.Namespace) -> None:
    manifest_result = asyncio.run(_call({"cmd": "load", "block_id": args.manifest_id}))
    if "error" in manifest_result:
        _die(manifest_result)
    chunk_ids = json.loads(base64.b64decode(manifest_result["data_b64"]))
    with open(args.out, "wb") as out:
        for cid in chunk_ids:
            result = asyncio.run(_call({"cmd": "load", "block_id": cid}))
            if "error" in result:
                _die(result)
            out.write(base64.b64decode(result["data_b64"]))
    print(f"wrote {args.out} from {len(chunk_ids)} chunks")


def main() -> None:
    parser = argparse.ArgumentParser(prog="memcli")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("store", help="store a file's contents as a block")
    p.add_argument("file", help="path to read, or '-' for stdin")
    p.add_argument("--mode", choices=["pinned", "cache"], default="cache")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--peer", help="store on this specific connected peer by name")
    g.add_argument("--auto", action="store_true", help="place on whichever connected peer has the most free room")
    p.set_defaults(func=cmd_store)

    p = sub.add_parser("load", help="load a block by id")
    p.add_argument("block_id")
    p.add_argument("--out", help="write to this file instead of stdout")
    p.set_defaults(func=cmd_load)

    p = sub.add_parser("free", help="free a block by id")
    p.add_argument("block_id")
    p.set_defaults(func=cmd_free)

    p = sub.add_parser("peers", help="list connected peers")
    p.set_defaults(func=cmd_peers)

    p = sub.add_parser("connect", help="manually connect to a peer (host:port), e.g. across subnets where mDNS can't reach")
    p.add_argument("address")
    p.set_defaults(func=cmd_connect)

    p = sub.add_parser("stats", help="show this node's memory stats")
    p.set_defaults(func=cmd_stats)

    p = sub.add_parser("stream-store", help="upload a large file in chunks")
    p.add_argument("file")
    p.add_argument("--peer")
    p.add_argument("--mode", choices=["pinned", "cache"], default="cache")
    p.set_defaults(func=cmd_stream_store)

    p = sub.add_parser("stream-load", help="reassemble a file stored with stream-store")
    p.add_argument("manifest_id")
    p.add_argument("out")
    p.set_defaults(func=cmd_stream_load)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()