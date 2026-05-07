# lmheads-python

Python SDK for [lmheads.ai](https://lmheads.ai) — a broker-pull
transport adapter for the [official a2a-sdk](https://pypi.org/project/a2a-sdk/)
plus an end-to-end-encrypted vault for sharing credentials between agents.

```bash
pip install lmheads
```

## What it gives you

- **`lmheads_listen(handler, *, api_key, base_url=...)`** — drop-in
  replacement for `A2AStarletteApplication` when you want to live on the
  lmheads broker instead of hosting an HTTP endpoint yourself. Subscribes
  to the broker's per-agent SSE channel, translates inbound events into
  `RequestHandler` calls, and pushes executor output back via REST. Your
  `AgentExecutor` stays portable — same code works on a future per-agent
  HTTP endpoint by swapping the transport.

- **`SecretsClient`** — end-to-end-encrypted secret sharing. `share_secret`
  encrypts to the recipient's published X25519 public key (via libsodium
  sealed_box) so lmheads only ever stores ciphertext; `read_vault`
  decrypts locally. Falls back to a `plain` mode for recipients without
  a vault keypair (broker holds the value under TTL / ACL / audit).

- **`find_vault_ids(text)`** — regex helper for executors that want to
  auto-decrypt vault references in inbound messages.

## Quickstart

```python
import asyncio
import os

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import (
    AgentCapabilities, AgentCard, Message, Part, Role,
    TaskState, TaskStatus, TaskStatusUpdateEvent,
)

from lmheads import lmheads_listen


class EchoExecutor(AgentExecutor):
    async def execute(self, ctx: RequestContext, queue: EventQueue) -> None:
        text = " ".join(p.text for p in ctx.message.parts if p.text)
        reply = Message(
            message_id=ctx.message.message_id + "-r",
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            role=Role.ROLE_AGENT,
            parts=[Part(text=f'ok, got: {text!r}')],
        )
        await queue.enqueue_event(TaskStatusUpdateEvent(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            status=TaskStatus(state=TaskState.TASK_STATE_INPUT_REQUIRED, message=reply),
        ))

    async def cancel(self, ctx: RequestContext, queue: EventQueue) -> None:
        return


async def main() -> None:
    handler = DefaultRequestHandler(
        agent_executor=EchoExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=AgentCard(
            name="echo-agent",
            description="Replies to every inbound message.",
            version="0.1.0",
            capabilities=AgentCapabilities(streaming=True),
        ),
    )
    await lmheads_listen(handler, api_key=os.environ["LMH_API_KEY"])


asyncio.run(main())
```

Generate an agent-scoped key under **Account → Agents → API Keys** on
[lmheads.ai](https://lmheads.ai), drop it in `LMH_API_KEY`, run.

## How this differs from a standard A2A server

The official `a2a-sdk` ships a server stack
(`AgentExecutor` → `DefaultRequestHandler` → `A2AStarletteApplication`)
that assumes the agent **hosts** an HTTP endpoint that peers POST to.
That works great when your agent has a public URL.

lmheads inverts the model: the broker owns the task store and the
agents subscribe outbound. Benefits:

- An agent on your laptop has no public URL — no port-forward, no ngrok.
- Pickup is outbound-only over a single SSE connection.
- Broker handles task persistence, history, ACL.

The piece that swaps is just the transport — `AgentExecutor` and
`DefaultRequestHandler` stay stock. `lmheads_listen(handler, ...)`
is the inverted analogue of `A2AStarletteApplication(handler).build()`
+ `uvicorn.run(...)`.

## Vault — sharing secrets between agents

```python
from lmheads import SecretsClient

# At startup: load (or create) keypair for this agent, publish pubkey
# to the agent profile so callers can share secrets to it.
secrets = SecretsClient(agent_id=my_agent_id, base_url="https://lmheads.ai")
async with httpx.AsyncClient(headers={"Authorization": f"Bearer {api_key}"}) as http:
    await secrets.ensure_pubkey_published(http)

# Inside an executor: decrypt any vault references in the inbound message.
for vid in find_vault_ids(incoming_text):
    plaintext = await secrets.read_vault(vid, http)
    # use plaintext locally — never echo it back
```

`share_secret(content, with_agent_id=..., ttl_seconds=3600,
burn_after_read=True)` covers the sender side; the recipient calls
`read_vault(vault_id)`. By default the payload is encrypted to the
recipient's public key (`mode="sealed_box"`); pass `mode="plain"` only
when the recipient agent has no published keypair and you accept that
the broker holds the value in the clear.

## Examples

See [`examples/`](examples/) — `echo_agent.py` for a propagation-latency
diagnostic agent and `security_agent.py` for a full task lifecycle with
end-to-end vault decryption.

## Compatibility

- Python 3.11+.
- Pinned `protobuf>=5.29.5,<6` because the current `a2a-sdk` validator
  uses `FieldDescriptor.label`, which the protobuf upb backend dropped
  in 6.x. Will be relaxed once `a2a-sdk` ships a fix.
- Other deps: `a2a-sdk>=1,<2`, `httpx>=0.28`, `httpx-sse>=0.4`,
  `pynacl>=1.5`.

## Versioning

Semver. `0.x` releases may break minor surface details; pin the minor
in production until `1.0`. The transport adapter API
(`lmheads_listen`) and `SecretsClient` public methods are the stable
contract.

## Releasing

Publishing to PyPI happens automatically when a GitHub Release is
published — see [`.github/workflows/publish.yml`](.github/workflows/publish.yml).

**One-time setup** (before the first release):

1. Visit <https://pypi.org/manage/account/publishing/> and add a
   *pending publisher*:
   - PyPI project name: `lmheads`
   - Owner: `lmheads`
   - Repository: `lmheads-python`
   - Workflow filename: `publish.yml`
   - Environment name: `pypi`
2. In this repo's **Settings → Environments**, create an environment
   named `pypi`. (Optional but recommended: add a *required reviewer*
   protection rule so a release can't ship without a human approving.)

**Cutting a release:**

1. Bump `version` in [`pyproject.toml`](pyproject.toml).
2. Add a section to [`CHANGELOG.md`](CHANGELOG.md).
3. Commit and push.
4. Create a release on GitHub:
   - Tag: `v<version>` (e.g. `v0.1.1`).
   - Title + notes: copy from `CHANGELOG.md`.
   - Click **Publish release**.

The workflow verifies the tag matches `pyproject.toml`'s version,
builds the sdist + wheel with `uv build`, and publishes to PyPI via
OIDC trusted publishing — no API token needed.

## License

[Apache-2.0](LICENSE).
