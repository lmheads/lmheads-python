"""Tests for the public whoami() helper.

Mocked at the httpx layer with respx — no live broker needed. Covers
the agent-scoped happy path, the user-scoped error path (ValueError
subclass with a pointed message), and the missing-agent_id edge case.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from lmheads import NotAgentScopedError, WhoAmI, whoami


@pytest.mark.asyncio
@respx.mock
async def test_whoami_returns_dataclass_for_agent_scoped_key():
    respx.get("https://lmheads.ai/api/v1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "kind": "agent",
                "user_id": "u-1",
                "email": "alice@example.com",
                "name": "Alice",
                "role": "authorized",
                "agent_id": "a-1",
                "agent_name": "argusml",
            },
        )
    )

    me = await whoami("lmh_token", base_url="https://lmheads.ai")

    assert isinstance(me, WhoAmI)
    assert me.kind == "agent"
    assert me.agent_id == "a-1"
    assert me.agent_name == "argusml"
    assert me.email == "alice@example.com"


@pytest.mark.asyncio
@respx.mock
async def test_whoami_raises_for_user_scoped_key():
    respx.get("https://lmheads.ai/api/v1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "kind": "user",
                "user_id": "u-1",
                "email": "alice@example.com",
                "name": "Alice",
                "role": "authorized",
            },
        )
    )

    with pytest.raises(NotAgentScopedError) as exc_info:
        await whoami("lmh_token", base_url="https://lmheads.ai")

    assert "Account → Agents → API Keys" in str(exc_info.value)
    # NotAgentScopedError is a ValueError so existing callers' except
    # clauses keep working (lmheads_listen used to raise plain ValueError).
    assert isinstance(exc_info.value, ValueError)


@pytest.mark.asyncio
@respx.mock
async def test_whoami_raises_when_agent_id_missing_despite_kind_agent():
    """Defensive: kind says 'agent' but agent_id is empty. Treat as
    not-agent-scoped rather than constructing a SecretsClient that
    will 400 on its first call."""
    respx.get("https://lmheads.ai/api/v1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "kind": "agent",
                "user_id": "u-1",
                "agent_id": "",
                "email": "",
                "name": "",
                "role": "authorized",
            },
        )
    )
    with pytest.raises(NotAgentScopedError):
        await whoami("lmh_token", base_url="https://lmheads.ai")


@pytest.mark.asyncio
@respx.mock
async def test_whoami_reuses_caller_supplied_http_client():
    """When the caller passes their own AsyncClient, whoami uses it
    instead of opening a new one — preserves authentication headers
    and the client lifetime."""
    respx.get("https://lmheads.ai/api/v1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "kind": "agent",
                "user_id": "u",
                "agent_id": "a",
                "email": "",
                "name": "",
                "role": "authorized",
                "agent_name": "n",
            },
        )
    )

    async with httpx.AsyncClient(
        headers={"Authorization": "Bearer lmh_token"}
    ) as http:
        me = await whoami("lmh_token", base_url="https://lmheads.ai", http=http)
        # The shared client is still usable after whoami returns —
        # whoami didn't close it as it would for a self-owned one.
        assert not http.is_closed

    assert me.agent_id == "a"


@pytest.mark.asyncio
@respx.mock
async def test_whoami_strips_trailing_slash_from_base_url():
    """Defensive against config files that habitually end URLs with /."""
    respx.get("https://lmheads.ai/api/v1/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "kind": "agent",
                "user_id": "u",
                "agent_id": "a",
                "email": "",
                "name": "",
                "role": "authorized",
                "agent_name": "",
            },
        )
    )
    me = await whoami("lmh_token", base_url="https://lmheads.ai/")
    assert me.agent_id == "a"
