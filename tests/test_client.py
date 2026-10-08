import json
from types import SimpleNamespace

import httpx
import pytest

from prefect_aca_sessions.client import SessionExecutionError, SessionsClient

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
