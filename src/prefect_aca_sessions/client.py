"""Minimal async client for the agent in a custom container session pool."""

import asyncio
import random
from typing import Any

import httpx
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import DefaultAzureCredential

SESSIONS_TOKEN_SCOPE = "https://dynamicsessions.io/.default"
REQUEST_TIMEOUT_SECONDS = 300.0
STOP_SESSION_API_VERSION = "2025-02-02-preview"
START_MAX_ATTEMPTS = 8
START_BACKOFF_BASE_SECONDS = 1.0
START_BACKOFF_MAX_SECONDS = 60.0


class SessionExecutionError(RuntimeError):
    """The session pool or the agent in the session rejected a request."""


class SessionNotFoundError(SessionExecutionError):
    """The session pool has no session with the requested identifier."""


class SessionPoolBusyError(SessionExecutionError):
    """The session pool could not allocate a session (HTTP 429), e.g. it is at max sessions."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _retry_after_seconds(response: httpx.Response) -> float | None:
    try:
        return max(float(response.headers["Retry-After"]), 0.0)
    except (KeyError, ValueError):
        return None


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

    async def _request(
        self, method: str, path: str, identifier: str, **kwargs: Any
    ) -> httpx.Response:
        token = await self._credential.get_token(SESSIONS_TOKEN_SCOPE)
        response = await self._http.request(
            method,
            f"{self._endpoint}{path}",
            params={"identifier": identifier, **kwargs.pop("params", {})},
            headers={"Authorization": f"Bearer {token.token}"},
            **kwargs,
        )
        if response.is_error:
            message = (
                f"Session {method} {path} returned HTTP {response.status_code}: {response.text}"
            )
            if response.status_code == 429:
                raise SessionPoolBusyError(message, _retry_after_seconds(response))
            if response.status_code == 404:
                raise SessionNotFoundError(message)
            raise SessionExecutionError(message)
        return response

    async def start(self, identifier: str, command: str, env: dict[str, str]) -> None:
        """Start `command` as a detached process in the session.

        The first request for an identifier allocates its session. While the pool cannot
        allocate one (HTTP 429) this retries with exponential backoff and jitter, honoring
        `Retry-After`; a 429 is rejected by the pool, so the agent never saw the request.
        """
        for attempt in range(START_MAX_ATTEMPTS):
            try:
                await self._request(
                    "POST", "/start", identifier, json={"command": command, "env": env}
                )
                return
            except SessionPoolBusyError as exc:
                if attempt == START_MAX_ATTEMPTS - 1:
                    raise
                backoff = min(
                    START_BACKOFF_BASE_SECONDS * 2**attempt, START_BACKOFF_MAX_SECONDS
                )
                await asyncio.sleep(exc.retry_after or random.uniform(backoff / 2, backoff))

    async def poll(self, identifier: str, offset: int) -> dict[str, Any]:
        """New log text from `offset`, the next offset, whether more log remains, whether a
        process was started and its exit code (or None while running)."""
        response = await self._request("GET", "/poll", identifier, params={"offset": offset})
        return response.json()

    async def stop(self, identifier: str) -> None:
        """Stop the session, ending every process in it. Handled by the pool, not the agent."""
        await self._request(
            "POST",
            "/.management/stopSession",
            identifier,
            params={"api-version": STOP_SESSION_API_VERSION},
        )
