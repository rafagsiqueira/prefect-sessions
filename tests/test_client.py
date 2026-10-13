import json
from types import SimpleNamespace

import httpx
import pytest

from prefect_aca_sessions import client as client_module
from prefect_aca_sessions.client import (
    SessionExecutionError,
    SessionGatewayError,
    SessionNotFoundError,
    SessionPoolBusyError,
    SessionsClient,
)

ENDPOINT = "https://eastus.dynamicsessions.io/subscriptions/s/resourceGroups/r/sessionPools/p"


class FakeCredential:
    async def get_token(self, *scopes):
        self.scopes = scopes
        return SimpleNamespace(token="tok")

    async def close(self):
        pass


def make_client(handler, credential=None):
    return SessionsClient(
        ENDPOINT + "/",
        credential=credential or FakeCredential(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


async def test_start_posts_command_with_token_and_identifier():
    seen = {}

    def handler(request):
        seen["request"] = request
        return httpx.Response(200, json={"pid": 1})

    credential = FakeCredential()
    async with make_client(handler, credential) as client:
        await client.start("abc", "echo hi", {"A": "1"})

    request = seen["request"]
    assert request.method == "POST"
    assert str(request.url).startswith(ENDPOINT + "/start?")
    assert request.url.params["identifier"] == "abc"
    assert request.headers["Authorization"] == "Bearer tok"
    assert json.loads(request.content) == {"command": "echo hi", "env": {"A": "1"}}
    assert credential.scopes == ("https://dynamicsessions.io/.default",)


async def test_poll_returns_agent_response():
    seen = {}

    def handler(request):
        seen["request"] = request
        return httpx.Response(
            200, json={"log": "x", "offset": 5, "more": False, "exit_code": None}
        )

    async with make_client(handler) as client:
        poll = await client.poll("abc", 4)

    assert seen["request"].url.path.endswith("/poll")
    assert seen["request"].url.params["offset"] == "4"
    assert poll["offset"] == 5


async def test_request_raises_on_http_error():
    async with make_client(lambda r: httpx.Response(403, text="nope")) as client:
        with pytest.raises(SessionExecutionError, match="403"):
            await client.poll("abc", 0)


async def test_stop_calls_the_pool_stop_session_api():
    seen = {}

    def handler(request):
        seen["request"] = request
        return httpx.Response(200, text="Session abc in session pool p stopped.")

    async with make_client(handler) as client:
        await client.stop("abc")

    request = seen["request"]
    assert request.method == "POST"
    assert request.url.path.endswith("/sessionPools/p/.management/stopSession")
    assert request.url.params["identifier"] == "abc"
    assert request.url.params["api-version"] == "2025-02-02-preview"


async def test_request_raises_not_found_on_404():
    async with make_client(lambda r: httpx.Response(404, text="no session")) as client:
        with pytest.raises(SessionNotFoundError, match="404"):
            await client.stop("abc")


@pytest.fixture
def sleeps(monkeypatch):
    delays = []

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)
    return delays


async def test_start_retries_with_exponential_backoff_while_pool_is_busy(sleeps):
    responses = iter([httpx.Response(429)] * 3 + [httpx.Response(200, json={"pid": 1})])

    async with make_client(lambda r: next(responses)) as client:
        await client.start("abc", "echo hi", {})

    assert len(sleeps) == 3
    for attempt, delay in enumerate(sleeps):
        assert 2**attempt / 2 <= delay <= 2**attempt


async def test_start_caps_backoff_delay(sleeps, monkeypatch):
    monkeypatch.setattr(client_module, "START_MAX_ATTEMPTS", 12)

    async with make_client(lambda r: httpx.Response(429)) as client:
        with pytest.raises(SessionPoolBusyError):
            await client.start("abc", "echo hi", {})

    assert max(sleeps) <= client_module.START_BACKOFF_MAX_SECONDS


async def test_start_honors_retry_after(sleeps):
    responses = iter(
        [httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200, json={})]
    )

    async with make_client(lambda r: next(responses)) as client:
        await client.start("abc", "echo hi", {})

    assert sleeps == [7.0]


async def test_start_gives_up_after_max_attempts(sleeps):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(429, text="max sessions")

    async with make_client(handler) as client:
        with pytest.raises(SessionPoolBusyError, match="429"):
            await client.start("abc", "echo hi", {})

    assert len(calls) == client_module.START_MAX_ATTEMPTS
    assert len(sleeps) == client_module.START_MAX_ATTEMPTS - 1


@pytest.mark.parametrize("status_code", [502, 503, 504])
async def test_start_retries_gateway_errors(sleeps, status_code):
    responses = iter([httpx.Response(status_code)] * 2 + [httpx.Response(200, json={})])

    async with make_client(lambda r: next(responses)) as client:
        await client.start("abc", "echo hi", {})

    assert len(sleeps) == 2


async def test_start_gives_up_on_persistent_gateway_errors(sleeps):
    async with make_client(lambda r: httpx.Response(502)) as client:
        with pytest.raises(SessionGatewayError, match="502"):
            await client.start("abc", "echo hi", {})

    assert len(sleeps) == client_module.START_MAX_ATTEMPTS - 1


async def test_start_succeeds_when_a_retry_finds_the_process_started(sleeps):
    responses = iter([httpx.Response(502), httpx.Response(409, json={"error": "started"})])

    async with make_client(lambda r: next(responses)) as client:
        await client.start("abc", "echo hi", {})


async def test_start_raises_conflict_without_a_prior_gateway_error(sleeps):
    responses = iter([httpx.Response(429), httpx.Response(409, json={"error": "started"})])

    async with make_client(lambda r: next(responses)) as client:
        with pytest.raises(SessionExecutionError, match="409"):
            await client.start("abc", "echo hi", {})


async def test_start_does_not_retry_other_errors(sleeps):
    async with make_client(lambda r: httpx.Response(500, text="boom")) as client:
        with pytest.raises(SessionExecutionError, match="500"):
            await client.start("abc", "echo hi", {})

    assert sleeps == []
