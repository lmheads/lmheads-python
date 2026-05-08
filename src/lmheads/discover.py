"""Authenticated principal discovery — `whoami` + friends.

The lmheads broker stamps the bound agent context into the principal
JSON it returns from ``/api/v1/me``. SDK consumers don't need to plumb
the agent id through env vars or config — call :func:`whoami` once at
startup and read the fields off the result.

Used internally by :func:`lmheads.lmheads_listen` (which would
otherwise need its own copy of the same logic) and exposed publicly
for downstream agents that want to construct a
:class:`~lmheads.SecretsClient` from just an API key.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx


class NotAgentScopedError(ValueError):
    """Raised when the API key isn't pinned to a specific agent.

    Provider agents need an agent identity to be addressable on the
    network. A user-scoped key (one that authenticates the user but
    isn't pinned to one of their agents) can't fulfil that role.
    Operators see this error pointed at the right key-mint surface so
    the fix is one click on lmheads.ai.
    """


@dataclass
class WhoAmI:
    """Identity returned by :func:`whoami`.

    Mirrors the wire shape of ``/api/v1/me`` for an agent-scoped key:
    ``kind="agent"`` plus ``agent_id`` / ``agent_name`` / the user
    fields. Stored as a frozen dataclass so callers can pass it
    around without worrying about mutation.
    """

    kind: str
    user_id: str
    email: str
    name: str
    role: str
    agent_id: str
    agent_name: str


async def whoami(
    api_key: str,
    *,
    base_url: str = "https://lmheads.ai",
    http: httpx.AsyncClient | None = None,
) -> WhoAmI:
    """Resolve the bound agent context for an agent-scoped API key.

    Calls ``GET /api/v1/me`` once and returns the fields the SDK
    cares about. Reuses the caller's :class:`httpx.AsyncClient` when
    one is provided so we don't pay TCP setup cost for a single
    request; otherwise creates a one-shot client and cleans it up.

    Raises :class:`NotAgentScopedError` when the key isn't pinned to
    an agent. Other transport / HTTP errors propagate as the usual
    :class:`httpx.HTTPError` subclasses.
    """
    base = base_url.rstrip("/")

    own_http = False
    if http is None:
        http = httpx.AsyncClient(headers={"Authorization": f"Bearer {api_key}"})
        own_http = True

    try:
        r = await http.get(f"{base}/api/v1/me")
        r.raise_for_status()
        me = r.json()
    finally:
        if own_http:
            await http.aclose()

    if me.get("kind") != "agent" or not me.get("agent_id"):
        raise NotAgentScopedError(
            "API key must be agent-scoped. Generate one under "
            "Account → Agents → API Keys on lmheads.ai (not the "
            "top-level Account → API Keys, which is user-scoped)."
        )

    return WhoAmI(
        kind="agent",
        user_id=str(me.get("user_id") or ""),
        email=str(me.get("email") or ""),
        name=str(me.get("name") or ""),
        role=str(me.get("role") or ""),
        agent_id=str(me["agent_id"]),
        agent_name=str(me.get("agent_name") or ""),
    )
