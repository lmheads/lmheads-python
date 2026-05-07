"""Unit tests for the vault SecretsClient and helpers."""

from __future__ import annotations

import base64
from pathlib import Path

from nacl.public import PrivateKey, PublicKey, SealedBox

from lmheads.secrets import SecretsClient, find_vault_ids


def test_sealedbox_round_trip():
    """Sanity check that pynacl's SealedBox API behaves as we expect —
    if this fails, the SDK has nothing to layer on top."""
    sk = PrivateKey.generate()
    pk = sk.public_key
    sealed = SealedBox(pk).encrypt(b"hunter2")
    assert SealedBox(sk).decrypt(sealed) == b"hunter2"


def test_secrets_client_keypair_persists(tmp_path: Path):
    """Constructing a SecretsClient generates and persists a private key
    on first use; a second client with the same agent_id + key_dir
    reloads it instead of generating a new one."""
    sc1 = SecretsClient(agent_id="agent-A", base_url="https://example", key_dir=tmp_path)
    pk1 = sc1.public_key
    assert pk1
    assert (tmp_path / "agent-A.priv").exists()

    sc2 = SecretsClient(agent_id="agent-A", base_url="https://example", key_dir=tmp_path)
    assert sc2.public_key == pk1, "second client should reload the same key"


def test_secrets_client_round_trip_self(tmp_path: Path):
    """Encrypt to my own pubkey, decrypt with my private — exercises
    the same code paths the real client uses on send + read."""
    sc = SecretsClient(agent_id="agent-A", base_url="https://example", key_dir=tmp_path)
    pk = PublicKey(base64.b64decode(sc.public_key))

    sealed = SealedBox(pk).encrypt(b"top secret")
    recovered = SealedBox(sc._kp.private_key).decrypt(sealed)
    assert recovered == b"top secret"


def test_find_vault_ids_matches_canonical_pattern():
    """Vault ids are 'vault_' + 24 lowercase hex (12 bytes of entropy
    from the server). Other tokens shouldn't match."""
    text = (
        "use vault_a1b2c3d4e5f6789012345678 and "
        "vault_a1b2c3d4e5f6789012345678 for both."
    )
    assert find_vault_ids(text) == ["vault_a1b2c3d4e5f6789012345678"]


def test_find_vault_ids_ignores_non_canonical():
    text = "no vault here, just vault_short and vault_NOTHEX and task_xxx"
    assert find_vault_ids(text) == []


def test_find_vault_ids_handles_empty():
    assert find_vault_ids("") == []
    assert find_vault_ids(None) == []  # type: ignore[arg-type]
