"""Minimal async client for the agent in a custom container session pool."""

from typing import Any

import httpx
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import DefaultAzureCredential

SESSIONS_TOKEN_SCOPE = "https://dynamicsessions.io/.default"
REQUEST_TIMEOUT_SECONDS = 300.0


class SessionExecutionError(RuntimeError):
    """The session pool or the agent in the session rejected a request."""


class SessionsClient:
    """Talks to the agent (`prefect_aca_sessions.agent`) in a custom container session.

    The pool management endpoint proxies each request to the session's container.
    """

    def __init__(
        self,
        pool_management_endpoint: str,
        credential: AsyncTokenCredential | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._endpoint = pool_management_endpoint.rstrip("/")
        self._credential = credential or DefaultAzureCredential()
        self._http = http_client or httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS)

    async def __aenter__(self) -> "SessionsClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._http.aclose()
        await self._credential.close()

    async def _request(self, method: str, path: str, identifier: str, **kwargs: Any) -> Any:
        token = await self._credential.get_token(SESSIONS_TOKEN_SCOPE)
        response = await self._http.request(
            method,
            f"{self._endpoint}{path}",
            params={"identifier": identifier, **kwargs.pop("params", {})},
            headers={"Authorization": f"Bearer {token.token}"},
            **kwargs,
        )
        if response.is_error:
            raise SessionExecutionError(
                f"Session {method} {path} returned HTTP {response.status_code}: {response.text}"
            )
        return response.json()

    async def start(self, identifier: str, command: str, env: dict[str, str]) -> None:
        """Start `command` as a detached process in the session."""
        await self._request(
            "POST", "/start", identifier, json={"command": command, "env": env}
        )

    async def poll(self, identifier: str, offset: int) -> dict[str, Any]:
        """New log text from `offset`, the next offset, whether more log remains and the
        exit code (or None while running)."""
        return await self._request("GET", "/poll", identifier, params={"offset": offset})
