"""
Lightweight peer authentication.

Design note (read this before ripping it out further): the reference
implementation this project is based on used a full Noise-XX handshake
plus ChaCha20-Poly1305 transport encryption. For a same-LAN hackathon
demo among a known team, that's disproportionate engineering time for
the threat model -- eavesdropping/tampering on your own trusted wifi is
low-probability, and real AEAD crypto is not free to get right.

What we kept instead of encryption: authentication. Confidentiality
(nobody can read the bytes in transit) and authorization (only people
who know the secret can join the mesh at all) are different properties.
Dropping the first is a reasonable hackathon trade-off. Dropping the
second means literally anyone on the wifi who mDNS-discovers your node
can store/read data on it -- not just your team -- which is a much
easier accident on shared venue wifi than on a home network. So: no
encryption, but every Hello is challenged with an HMAC over a fresh
nonce, keyed with a secret every legitimate node in the mesh shares
(MEMCLOUD_SECRET).

Swap-in path if you ever need real confidentiality later: replace
`verify_auth` below with a Noise-XX handshake and wrap
read_frame/write_frame in an AEAD-encrypting transport -- the Hello/
Welcome messages in protocol.py already carry a `nonce` field for
exactly this reason, so the message shapes don't need to change.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import platform
import uuid

from memnode import config


def compute_auth(secret: str, node_id: str, nonce: str) -> str:
    mac = hmac.new(secret.encode(), (node_id + nonce).encode(), hashlib.sha256)
    return mac.hexdigest()


def verify_auth(secret: str, node_id: str, nonce: str, auth: str) -> bool:
    expected = compute_auth(secret, node_id, nonce)
    # constant-time compare -- no reason to leak how many prefix bytes matched
    return hmac.compare_digest(expected, auth)


def new_nonce() -> str:
    return uuid.uuid4().hex


def load_or_create_identity() -> dict:
    """
    Persistent node identity: just a random id + a display name, stored
    once so a node's peers recognize it as the same node across restarts.
    (No keypair -- we're not signing anything without real encryption
    backing it; a keypair here without a full handshake would be
    security theater, not security.)
    """
    config.ensure_home()
    if config.IDENTITY_FILE.exists():
        return json.loads(config.IDENTITY_FILE.read_text())

    identity = {"node_id": uuid.uuid4().hex, "name": platform.node() or "memnode"}
    config.IDENTITY_FILE.write_text(json.dumps(identity))
    return identity
