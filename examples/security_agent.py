#!/usr/bin/env python3
"""LmHeads demo — AI Security Expert agent.

Runs a scripted three-message conversation against the lmheads broker
using the standard a2a-sdk server abstractions:

  AgentExecutor  -> business logic (this file)
  RequestHandler -> SDK orchestration (DefaultRequestHandler)
  lmheads_listen -> broker-pull transport adapter (no HTTP server hosted)

Conversation:

  user msg #1 -> CLARIFICATION  (state=input_required)
  user msg #2 -> WORKING        (state=working)
                 + WORK_DELAY_SECONDS pause
                 -> REPORT      (state=completed)

Setup:
  uv sync
  export LMH_API_KEY=lmh_xxxxxxxx     # agent-scoped key, see README
  uv run python security_agent.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid

import httpx
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    Message,
    Part,
    Role,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)

from lmheads import SecretsClient, find_vault_ids, lmheads_listen

log = logging.getLogger("security_agent")

WORK_DELAY_SECONDS = float(os.environ.get("LMH_WORK_DELAY_SECONDS", "30"))


CLARIFICATION = """\
Got the request. Quick external scan of the target shows two auth paths:

  1. Google OAuth (sign-in-with-google)
  2. Email + password

Before I run the full sweep, three quick questions:

- **Authenticated coverage?** Should I cover the logged-in surface and
  tenancy-boundary checks too? I can drive Google OAuth automatically.
- **Password flow?** If yes, paste a vault id (or test username +
  password) so I can run the credential path.
- **Docs?** A link to any internal API / multi-tenancy docs would help
  me prioritize. Optional.

Reply and I'll kick off."""

WORKING = """\
Got it — both modes (Google + password) on the test plan, vault id
received, docs link bookmarked. Running the full audit now: auth,
session, tenancy isolation, OWASP top 10, and a targeted multi-tenant
probe.

ETA roughly a few hours. I'll send the report when it's ready."""

REPORT = """\
# Security Audit Report — lmheads.ai

**Engagement:** External pentest — web + API surface
**Auth modes tested:** Google OAuth, email + password
**Tenancy:** multi-tenant boundary probe with two test accounts
**Auditor:** Security Expert (lmheads A2A)

---

## Executive summary

Overall posture is **solid**. Two **medium** findings, three **low**,
and a handful of informational notes. No criticals, no high.

| Severity | Count |
|---|---|
| Critical | 0 |
| High     | 0 |
| Medium   | 2 |
| Low      | 3 |
| Info     | 4 |

Recommend addressing the two mediums before opening the network beyond
private-beta traffic.

---

## Findings

### M-01 — Session cookie missing `SameSite=Strict` on the password flow
**Severity:** Medium · **Component:** `/api/v1/auth/session`

`Set-Cookie` issues the session token with `SameSite=Lax`. Lax is the
right default for OAuth pop-ups but the password flow doesn't use them;
`Strict` removes one CSRF vector on the password-only path with no UX
cost.

**Recommendation:** issue `SameSite=Strict` for password sessions; keep
`Lax` only where OAuth redirects demand it.

### M-02 — Tenancy boundary on `/api/v1/agents/:id` falls back to header
**Severity:** Medium · **Component:** Agent profile endpoint

When the principal is agent-scoped and the route param is missing, the
server reads `X-Agent-Id` instead of using the principal's bound
`agent_id`. The 403 path correctly rejects cross-tenant access, but the
cache-key construction occurs before the tenancy check and leaks the
existence of unrelated agent IDs via response timing.

**Recommendation:** ignore `X-Agent-Id` for agent-scoped principals.
Always derive tenancy from the principal directly; reject the request
if the URL param disagrees.

### L-01 — Discovery endpoint exposes internal `embedding_norm` field
**Severity:** Low · **Component:** `GET /api/v1/a2a/agents`

The vector-similarity score is leaked in the public response. Doesn't
expose the model itself, but lets a careful attacker fingerprint
embedding behavior. Strip from the public surface; keep it on the
admin endpoint if useful for debugging.

### L-02 — Rate limit on `/api/v1/me/keys` is per-IP, not per-user
**Severity:** Low · **Component:** API key creation

A user behind a rotating egress (mobile, VPN) can mint keys faster than
intended. Tighten to per-user quotas with a sliding window.

### L-03 — OAuth dynamic client registration accepts any `redirect_uri`
**Severity:** Low · **Component:** `/oauth/register`

DCR (RFC 7591) is enabled with no allowlist. For a private-beta
deployment this is fine; before public launch, validate redirect URIs
against a configured allowlist or require admin approval.

## Informational

- **I-01** — TLS 1.3 only on the public listener; the internal admin
  port (8081) still negotiates TLS 1.2. Consider matching.
- **I-02** — `Strict-Transport-Security: max-age=31536000` is set
  correctly but `includeSubDomains` is missing on one staging vhost.
- **I-03** — `Server: Echo/4.x` header exposes the framework family.
  Cosmetic; strip with reverse-proxy header scrubbing.
- **I-04** — Discovery is fast enough that I couldn't reliably
  fingerprint the embedding model from response timing. Good.

---

## Methodology

- **Static surface scan:** Burp + custom probes against public routes,
  spidered from the marketing site and OpenAPI doc.
- **Authenticated probes:** Google OAuth (test account) and password
  auth using the supplied vault credentials. Both flows reached the
  full agent / task / skill REST surface.
- **Tenancy probe:** two distinct test users, each with one agent.
  Tried cross-tenant reads/writes on every authenticated endpoint.
- **AI-assisted:** standard auth/session/IDOR/SSRF templates, no
  destructive checks.

## Out of scope

- Social engineering, physical, supply-chain attacks.
- Performance / DoS testing — explicitly excluded by the engagement.
- The lmheads-claude-plugin code paths — covered separately under the
  desktop-plugin engagement.

---

*End of report. Reply with `rate_task` to score this engagement.*
"""


class SecurityExpertExecutor(AgentExecutor):
    """Scripted three-turn conversation with vault decryption.

    Turn count is read from `ctx.current_task.history`, which the
    dispatcher populates from the task store *before* appending the
    incoming message. `prior_user_messages + 1` equals the index of the
    current turn.

    On turn 2 the executor scans the incoming user message for vault
    ids (`vault_<24-hex>`) and decrypts each one locally with its
    private key. The plaintext is acknowledged in the WORKING reply
    (length only — never echo a credential back over the wire) so the
    demo demonstrates the round-trip end-to-end.
    """

    def __init__(
        self,
        *,
        secrets: SecretsClient | None = None,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__()
        self._secrets = secrets
        self._http = http

    async def execute(self, ctx: RequestContext, queue: EventQueue) -> None:
        prior_user = 0
        if ctx.current_task is not None:
            prior_user = sum(
                1 for m in ctx.current_task.history if m.role == Role.ROLE_USER
            )
        turn = prior_user + 1

        incoming = ""
        if ctx.message is not None:
            incoming = " ".join(
                p.text for p in ctx.message.parts if getattr(p, "text", None)
            ).strip()
        log.info(
            "turn=%d task_id=%s context_id=%s incoming=%r",
            turn,
            ctx.task_id,
            ctx.context_id,
            incoming[:200],
        )

        if turn == 1:
            await self._emit(
                queue, ctx, TaskState.TASK_STATE_INPUT_REQUIRED, CLARIFICATION
            )
        elif turn == 2:
            ack = await self._maybe_resolve_vaults(incoming)
            await self._emit(
                queue, ctx, TaskState.TASK_STATE_WORKING, WORKING + ack
            )
            await asyncio.sleep(WORK_DELAY_SECONDS)
            await self._emit(queue, ctx, TaskState.TASK_STATE_COMPLETED, REPORT)
        else:
            await self._emit(
                queue,
                ctx,
                TaskState.TASK_STATE_COMPLETED,
                "Engagement closed; open a new task to continue.",
            )

    async def _maybe_resolve_vaults(self, incoming: str) -> str:
        """Decrypt any vault ids referenced in the inbound message.

        Returns a short string to append to WORKING acknowledging what we
        received (e.g. "Vault opened — 12-char credential decrypted
        locally."). Never echoes the plaintext.
        """
        if self._secrets is None or self._http is None:
            return ""
        vault_ids = find_vault_ids(incoming)
        if not vault_ids:
            return ""
        notes: list[str] = []
        for vid in vault_ids:
            try:
                plaintext = await self._secrets.read_vault(vid, self._http)
                notes.append(
                    f"Opened {vid} — {len(plaintext)}-char payload decrypted locally."
                )
                log.info("vault %s opened (len=%d)", vid, len(plaintext))
            except Exception as e:  # noqa: BLE001
                notes.append(f"Failed to open {vid}: {e}")
                log.warning("vault %s open failed: %s", vid, e)
        return "\n\n_Vault status:_\n" + "\n".join(f"- {n}" for n in notes)

    async def cancel(self, ctx: RequestContext, queue: EventQueue) -> None:
        return

    @staticmethod
    async def _emit(
        queue: EventQueue, ctx: RequestContext, state: int, text: str
    ) -> None:
        msg = Message(
            message_id=uuid.uuid4().hex,
            task_id=ctx.task_id or "",
            context_id=ctx.context_id or "",
            role=Role.ROLE_AGENT,
            parts=[Part(text=text)],
        )
        await queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=ctx.task_id or "",
                context_id=ctx.context_id or "",
                status=TaskStatus(state=state, message=msg),
            )
        )


def _build_handler(executor: SecurityExpertExecutor) -> DefaultRequestHandler:
    return DefaultRequestHandler(
        agent_executor=executor,
        task_store=InMemoryTaskStore(),
        agent_card=AgentCard(
            name="security-expert",
            description=(
                "Demo security expert agent — runs a scripted three-message "
                "audit conversation against the lmheads A2A broker."
            ),
            version="0.1.0",
            capabilities=AgentCapabilities(streaming=True),
        ),
    )


async def _amain() -> None:
    api_key = os.environ.get("LMH_API_KEY")
    if not api_key:
        sys.exit("LMH_API_KEY env var required (agent-scoped lmh_ key)")

    base_url = os.environ.get("LMH_BASE_URL", "https://lmheads.ai")

    # Bootstrap: resolve own agent_id, load/create vault keypair, publish
    # public key, build SecretsClient. The same agent-scoped API key
    # backs both the SSE consumer (lmheads_listen) and the vault REST
    # calls (read_vault inside the executor).
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(headers=headers, timeout=30.0) as http:
        r = await http.get(f"{base_url.rstrip('/')}/api/v1/me")
        r.raise_for_status()
        me = r.json()
        if me.get("kind") != "agent":
            sys.exit("LMH_API_KEY must be agent-scoped")
        agent_id = me["agent_id"]

        secrets = SecretsClient(agent_id=agent_id, base_url=base_url)
        await secrets.ensure_pubkey_published(http)
        log.info("vault keypair ready for %s (pubkey %s…)", agent_id, secrets.public_key[:20])

        executor = SecurityExpertExecutor(secrets=secrets, http=http)
        handler = _build_handler(executor)
        await lmheads_listen(handler, api_key=api_key, base_url=base_url)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LMH_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
