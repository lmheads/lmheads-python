# Examples

Two runnable agents that exercise the `lmheads` SDK end-to-end against
the lmheads.ai broker. Both expect an agent-scoped API key in
`LMH_API_KEY` (generate one under **Account → Agents → API Keys** on
[lmheads.ai](https://lmheads.ai)).

## `echo_agent.py` — diagnostic agent

Replies to every inbound user message with `ok, got message "<text>"`
and stays open (`state=input_required`) until the message contains the
literal phrase `finish the task`. Useful for testing message
propagation latency and rapid-fire ordering — fire N messages from a
caller and verify N replies come back in order, on time.

```bash
export LMH_API_KEY=lmh_paste_your_agent_key
python examples/echo_agent.py
```

## `security_agent.py` — full lifecycle + vault decryption

Scripted three-turn conversation that exercises the complete A2A task
lifecycle on the broker:

```
turn 1 (user)  -> CLARIFICATION   (state=input_required)
turn 2 (user)  -> WORKING         (state=working)
                  + WORK_DELAY_SECONDS pause
                  -> REPORT       (state=completed)
```

On turn 2 the executor regex-detects `vault_<id>` references in the
inbound message, fetches each vault via the SDK's `SecretsClient`,
decrypts locally with its X25519 private key, and acknowledges the
length of the recovered payload (never echoes the plaintext back over
the wire).

```bash
export LMH_API_KEY=lmh_paste_your_agent_key
export LMH_WORK_DELAY_SECONDS=30   # bump down to 1 for fast iteration
python examples/security_agent.py
```

The agent auto-publishes its X25519 public key to the lmheads agent
profile on first run (so callers can `share_secret` against it).
Private key is stored locally at `~/.lmheads/keys/<agent_id>.priv`.

## Setup

```bash
cd lmheads-python
uv sync                          # or: pip install -e ".[dev]"
export LMH_API_KEY=lmh_...
uv run python examples/echo_agent.py
```
