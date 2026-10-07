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
        "v1",
        credential=credential or FakeCredential(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


async def test_execute_posts_inline_code_with_token_and_identifier():
    seen = {}

    def handler(request):
        seen["request"] = request
        return httpx.Response(
            200,
            json={
                "id": "e1",
                "identifier": "abc",
                "executionType": "Synchronous",
                "status": "Succeeded",
                "result": {"stdout": "hi\n", "stderr": "", "executionResult": ""},
            },
        )

    credential = FakeCredential()
    async with make_client(handler, credential) as client:
        result = await client.execute("abc", "print('hi')")

    request = seen["request"]
    assert str(request.url).startswith(ENDPOINT + "/executions?")
    assert request.url.params["identifier"] == "abc"
    assert request.url.params["api-version"] == "v1"
    assert request.headers["Authorization"] == "Bearer tok"
    body = json.loads(request.content)
    assert body["code"] == "print('hi')"
    assert body["codeInputType"] == "Inline"
    assert body["executionType"] == "Synchronous"
    assert body["timeoutInSeconds"] > 0
    assert credential.scopes == ("https://dynamicsessions.io/.default",)
    assert result.stdout == "hi\n"


async def test_execute_raises_on_http_error():
    async with make_client(lambda r: httpx.Response(403, text="nope")) as client:
        with pytest.raises(SessionExecutionError, match="403"):
            await client.execute("abc", "x")


async def test_execute_raises_when_code_fails():
    body = {
        "status": "Failed",
        "error": {"error": {"code": "E", "message": "bad"}},
        "result": {"stderr": "boom"},
    }
    async with make_client(lambda r: httpx.Response(200, json=body)) as client:
        with pytest.raises(SessionExecutionError, match="boom"):
            await client.execute("abc", "x")


async def test_delete_session_ignores_missing_session():
    async with make_client(lambda r: httpx.Response(404)) as client:
        await client.delete_session("abc")
