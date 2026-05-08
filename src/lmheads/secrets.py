"""End-to-end-encrypted secret sharing helpers for the lmheads vault.

Mirror of `lmheads-claude-plugin/channel/crypto.ts`: the agent owns an
X25519 keypair locally, registers the public half on its lmheads agent
profile, and decrypts incoming sealed_box ciphertext with the private
half. lmheads NEVER sees plaintext.

Typical usage from an `AgentExecutor`:

    keys = SecretsClient(api_key=API_KEY, base_url=BASE,
                         agent_id=my_agent_id, key_dir=Path.home()/'.lmheads')
    await keys.ensure_pubkey_published(http_client)   # idempotent
    plaintext = await keys.read_vault(vault_id, http_client)
"""

from __future__ import annotations

import base64
import os
import re
from dataclasses import dataclass
from pathlib import Path

import httpx
from nacl.public import PrivateKey, PublicKey, SealedBox


@dataclass
class _Keypair:
    private_key: PrivateKey
    public_key_b64: str


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def _b64d(s: str) -> bytes:
    return base64.b64decode(s)


def _load_or_create_keypair(agent_id: str, key_dir: Path) -> _Keypair:
    """Read or generate the X25519 private key for `agent_id`.

    Persistent storage: `<key_dir>/<agent_id>.priv` (mode 0600). On first
    run we generate a fresh keypair and save the private side; subsequent
    runs reload it. Symmetric with the plugin's `loadOrCreateKeypair`.
    """
    key_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = key_dir / f"{agent_id}.priv"
    if path.exists():
        sk_b64 = path.read_text().strip()
        sk = PrivateKey(_b64d(sk_b64))
    else:
        sk = PrivateKey.generate()
        path.write_text(_b64(bytes(sk)))
        os.chmod(path, 0o600)
    return _Keypair(private_key=sk, public_key_b64=_b64(bytes(sk.public_key)))


class SecretsClient:
    """Client-side adapter for the lmheads vault.

    Owns the local keypair and exposes `share_secret` / `read_vault`
    that wrap the REST endpoints with the necessary local crypto. Pass
    in a shared `httpx.AsyncClient` so authentication headers and
    keep-alive pools are reused.
    """

    def __init__(
        self,
        *,
        agent_id: str,
        base_url: str,
        key_dir: Path | str | None = None,
    ) -> None:
        self.agent_id = agent_id
        self.base_url = base_url.rstrip("/")
        path = Path(key_dir) if key_dir else (Path.home() / ".lmheads" / "keys")
        self._kp = _load_or_create_keypair(agent_id, path)

    @classmethod
    async def from_api_key(
        cls,
        api_key: str,
        *,
        base_url: str = "https://lmheads.ai",
        http: httpx.AsyncClient | None = None,
        key_dir: Path | str | None = None,
        publish: bool = True,
    ) -> "SecretsClient":
        """Construct a SecretsClient by self-discovering the bound agent id.

        Convenience wrapper for the common provider-agent shape:
        ``api_key`` → call :func:`whoami` to resolve the agent id →
        construct the client → optionally push the keypair's public
        key to the broker. Eliminates the boilerplate of looking up
        the agent id manually before instantiation.

        Pass ``publish=False`` to skip the ``/profile`` push if you
        plan to call :meth:`ensure_pubkey_published` yourself later.
        Pass ``http`` when you already have an authenticated client
        to avoid a second TCP handshake.
        """
        # Local import keeps secrets.py importable without dragging
        # the discover module's surface in.
        from .discover import whoami

        identity = await whoami(api_key, base_url=base_url, http=http)
        client = cls(agent_id=identity.agent_id, base_url=base_url, key_dir=key_dir)
        if publish:
            own_http = False
            if http is None:
                http = httpx.AsyncClient(
                    headers={"Authorization": f"Bearer {api_key}"}
                )
                own_http = True
            try:
                await client.ensure_pubkey_published(http)
            finally:
                if own_http:
                    await http.aclose()
        return client

    @property
    def public_key(self) -> str:
        return self._kp.public_key_b64

    async def ensure_pubkey_published(self, http: httpx.AsyncClient) -> None:
        """Idempotently push the local public key to the agent profile.

        Re-runs are no-ops when the server already holds the same value.
        Call once at startup; subsequent share/read paths assume the
        server's public key matches our private key.
        """
        url = f"{self.base_url}/api/v1/a2a/agents/{self.agent_id}/pubkey"
        r = await http.get(url)
        if r.status_code == 200:
            remote = r.json().get("public_key", "")
            if remote == self.public_key:
                return
        # Either remote was empty, mismatched, or the GET errored — push
        # via the user-authenticated PUT. The same agent-scoped key the
        # rest of the script uses is what authorizes this call.
        url = f"{self.base_url}/api/v1/agents/{self.agent_id}/profile"
        r = await http.put(url, json={"public_key": self.public_key})
        r.raise_for_status()

    async def read_vault(
        self, vault_id: str, http: httpx.AsyncClient
    ) -> str:
        """Fetch a vault and return the plaintext.

        Branches on `mode`: sealed_box decrypts locally with the
        agent's private key; plain returns what the server stored
        directly. Server returns 403 for non-recipients and 404 for
        missing or expired vaults. burn_after_read deletes the row on
        this read.
        """
        url = f"{self.base_url}/api/v1/vaults/{vault_id}"
        r = await http.get(url)
        r.raise_for_status()
        body = r.json()
        mode = body.get("mode", "sealed_box")
        if mode == "sealed_box":
            ct_b64 = body.get("ciphertext", "")
            if not ct_b64:
                raise ValueError(f"vault {vault_id} sealed_box but no ciphertext")
            sealed = _b64d(ct_b64)
            plaintext = SealedBox(self._kp.private_key).decrypt(sealed)
            return plaintext.decode("utf-8")
        if mode == "plain":
            pt = body.get("plaintext", "")
            if not pt:
                raise ValueError(f"vault {vault_id} plain mode but no plaintext")
            return pt
        raise ValueError(f"vault {vault_id} has unknown mode {mode!r}")

    async def share_secret(
        self,
        *,
        with_agent_id: str,
        content: str,
        ttl_seconds: int = 3600,
        burn_after_read: bool = True,
        mode: str = "sealed_box",
        http: httpx.AsyncClient,
    ) -> str:
        """Share `content` with `with_agent_id`.

        mode='sealed_box' (default): encrypts locally to the recipient's
        X25519 public key — lmheads never sees plaintext. Recipient must
        have published a pubkey.

        mode='plain': uploads literal plaintext to the broker under
        TTL/ACL/audit. Use only when sealed_box isn't possible.

        Returns the vault id. Caller forwards this id to the recipient
        (typically embedded in an A2A message).
        """
        if mode == "sealed_box":
            r = await http.get(
                f"{self.base_url}/api/v1/a2a/agents/{with_agent_id}/pubkey"
            )
            r.raise_for_status()
            recipient_pk_b64 = r.json().get("public_key", "")
            if not recipient_pk_b64:
                raise ValueError(
                    f"recipient {with_agent_id} has not published a public key — "
                    f"either ask them to register one or pass mode='plain'"
                )
            recipient_pk = PublicKey(_b64d(recipient_pk_b64))
            sealed = SealedBox(recipient_pk).encrypt(content.encode("utf-8"))
            body: dict = {
                "recipient_agent_id": with_agent_id,
                "ciphertext": _b64(sealed),
                "ttl_seconds": ttl_seconds,
                "burn_after_read": burn_after_read,
            }
        elif mode == "plain":
            body = {
                "recipient_agent_id": with_agent_id,
                "plaintext": content,
                "ttl_seconds": ttl_seconds,
                "burn_after_read": burn_after_read,
            }
        else:
            raise ValueError(f"unknown mode {mode!r}")

        r = await http.post(f"{self.base_url}/api/v1/vaults", json=body)
        r.raise_for_status()
        return r.json()["id"]


# ─── Vault id detection ──────────────────────────────────────────────

# Vault ids are "vault_" + 24 lowercase hex chars (12 bytes of entropy
# from the server). The pattern is intentionally narrow so a regex over
# arbitrary user text won't match unrelated tokens like "vault_abc".
VAULT_ID_RE = re.compile(r"\bvault_[0-9a-f]{24}\b")


def find_vault_ids(text: str) -> list[str]:
    """Return all vault ids referenced in `text`, deduped, in order."""
    seen: set[str] = set()
    out: list[str] = []
    for m in VAULT_ID_RE.findall(text or ""):
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out
