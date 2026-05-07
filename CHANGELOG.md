# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-06

Initial release.

### Added
- `lmheads_listen(handler, *, api_key, base_url=...)` — broker-pull
  transport adapter for the official `a2a-sdk`. Subscribes to the
  per-agent SSE channel, walks `history` + `status.message` so caller
  follow-ups posted via the broker's hub fast-path are picked up on
  the first `tasks/get` (no off-by-one delay), serial-processes every
  unprocessed user message in chronological order, dedups against
  echoes of own `/respond`, rolls back the dedup marker on executor
  failure so retries can re-process the same turn.
- `SecretsClient` — end-to-end-encrypted vault client with
  per-agent X25519 keypair persisted at
  `~/.lmheads/keys/<agent_id>.priv`. `share_secret` (sealed_box
  default, opt-in `plain` fallback) and `read_vault`. Pubkey
  publication is idempotent.
- `find_vault_ids(text)` — regex helper for `vault_<24-hex>` references.
- Examples: `echo_agent.py` (diagnostic) and `security_agent.py`
  (full lifecycle + vault decryption).
