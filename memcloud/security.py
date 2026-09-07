"""
Transport security for the MemCloud data plane.

Model: **mutual TLS with one shared cluster certificate.**

A single self-signed certificate + private key is generated once and copied to
every node. Each node uses it as:

  * its own server certificate,
  * its own client certificate,
  * and the only trusted CA.

Consequences:

  * A node that does not hold `cluster.pem` / `cluster.key` cannot connect,
    and cannot impersonate a node. Possession of the key pair *is* cluster
    membership -- the same trust model OpenFabric uses with its cluster secret,
    expressed through TLS instead of an HMAC handshake.
  * All block traffic is encrypted with whatever AEAD suite OpenSSL negotiates
    (TLS 1.3, normally AES-256-GCM or ChaCha20-Poly1305).
  * Hostname verification is disabled, because nodes are addressed by IP and
    those IPs change between demos. Client-certificate verification is what
    provides authentication, so this does not weaken the model.

Honest limitations:

  * One shared key for the whole cluster. Any member can impersonate any other
    member. Per-node certificates signed by a cluster CA would fix this and is
    a contained change, but is not implemented here.
  * There is no revocation. Losing the key file means regenerating and
    redistributing it.
  * `cluster.key` is a secret. It is written 0600 and must not be committed.

Generate once, then copy BOTH files to every laptop:

    python3 -m memcloud.security --out cluster
"""

from __future__ import annotations

import datetime
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
from typing import Optional, Tuple

DEFAULT_CERT = "cluster.pem"
DEFAULT_KEY = "cluster.key"


class SecurityError(Exception):
    pass


# ---------------------------------------------------------------- generation
def _generate_with_cryptography(cert_path: str, key_path: str, days: int) -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "memcloud-cluster")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("memcloud")]), critical=False
        )
        .sign(key, hashes.SHA256())
    )
    with open(key_path, "wb") as fh:
        fh.write(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            )
        )
    with open(cert_path, "wb") as fh:
        fh.write(cert.public_bytes(serialization.Encoding.PEM))


def _generate_with_openssl(cert_path: str, key_path: str, days: int) -> None:
    exe = shutil.which("openssl")
    if not exe:
        raise SecurityError("openssl not found")
    conf = tempfile.NamedTemporaryFile("w", suffix=".cnf", delete=False)
    conf.write(
        "[req]\ndistinguished_name=dn\nx509_extensions=v3\nprompt=no\n"
        "[dn]\nCN=memcloud-cluster\n"
        "[v3]\nbasicConstraints=critical,CA:TRUE\nsubjectAltName=DNS:memcloud\n"
    )
    conf.close()
    try:
        subprocess.check_call(
            [exe, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", key_path, "-out", cert_path,
             "-days", str(days), "-config", conf.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    finally:
        os.unlink(conf.name)


def generate_cluster_cert(
    cert_path: str = DEFAULT_CERT, key_path: str = DEFAULT_KEY, days: int = 3650
) -> Tuple[str, str]:
    """Create a self-signed cluster keypair. Returns (cert_path, key_path)."""
    for path in (cert_path, key_path):
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
    try:
        _generate_with_cryptography(cert_path, key_path, days)
    except ImportError:
        _generate_with_openssl(cert_path, key_path, days)
    except Exception:
        _generate_with_openssl(cert_path, key_path, days)
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return cert_path, key_path


def ensure_cluster_cert(cert_path: str, key_path: str) -> Tuple[str, str]:
    """Generate the keypair only if it is not already there."""
    if os.path.exists(cert_path) and os.path.exists(key_path):
        return cert_path, key_path
    return generate_cluster_cert(cert_path, key_path)


def fingerprint(cert_path: str) -> str:
    """SHA-256 fingerprint, so both laptops can be checked to match."""
    import hashlib

    with open(cert_path, "rb") as fh:
        pem = fh.read()
    der = ssl.PEM_cert_to_DER_cert(pem.decode("ascii"))
    h = hashlib.sha256(der).hexdigest().upper()
    return ":".join(h[i : i + 2] for i in range(0, len(h), 2))


# ------------------------------------------------------------------ contexts
def server_context(cert_path: str, key_path: str) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(cert_path, key_path)
    ctx.load_verify_locations(cert_path)   # the cluster cert is its own CA
    ctx.verify_mode = ssl.CERT_REQUIRED    # clients must present it too
    return ctx


def client_context(cert_path: str, key_path: str) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = False             # nodes are reached by IP
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cert_path)
    ctx.load_cert_chain(cert_path, key_path)
    return ctx


def describe(ssl_sock) -> str:
    try:
        v = ssl_sock.version()
        c = ssl_sock.cipher()
        return f"{v} {c[0]}" if c else str(v)
    except Exception:
        return "unknown"


def _main(argv: Optional[list] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(
        prog="memcloud.security",
        description="Generate the shared MemCloud cluster certificate.",
    )
    p.add_argument("--out", default="cluster",
                   help="basename; writes <out>.pem and <out>.key")
    p.add_argument("--days", type=int, default=3650)
    p.add_argument("--force", action="store_true")
    a = p.parse_args(argv)

    cert, key = f"{a.out}.pem", f"{a.out}.key"
    if (os.path.exists(cert) or os.path.exists(key)) and not a.force:
        print(f"{cert} / {key} already exist. Use --force to overwrite.")
    else:
        generate_cluster_cert(cert, key, a.days)
        print(f"wrote {cert} and {key}")
    print(f"SHA-256 fingerprint:\n  {fingerprint(cert)}")
    print("\nCopy BOTH files to every laptop, then start nodes with:")
    print(f"  --tls-cert {cert} --tls-key {key}")
    print("The fingerprint must match on every node.")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
