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
GATEWAY_ERROR_STATUS_CODES = frozenset({502, 503, 504})


class SessionExecutionError(RuntimeError):
    """The session pool or the agent in the session rejected a request."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class SessionNotFoundError(SessionExecutionError):
    """The session pool has no session with the requested identifier."""


class SessionUnavailableError(SessionExecutionError):
    """The session pool could not serve the request for now and it may be retried."""

    def __init__(
        self, message: str, status_code: int | None = None, retry_after: float | None = None
    ) -> None:
        super().__init__(message, status_code)
        self.retry_after = retry_after


class SessionPoolBusyError(SessionUnavailableError):
    """The session pool could not allocate a session (HTTP 429), e.g. it is at max sessions."""


class SessionGatewayError(SessionUnavailableError):
    """The pool could not reach the agent in the session (HTTP 502, 503 or 504), e.g. while
    its container is still starting. The agent may still have received the request."""


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
            status_code = response.status_code
            if status_code == 429:
                raise SessionPoolBusyError(message, status_code, _retry_after_seconds(response))
            if status_code in GATEWAY_ERROR_STATUS_CODES:
                raise SessionGatewayError(message, status_code, _retry_after_seconds(response))
            if status_code == 404:
                raise SessionNotFoundError(message, status_code)
            raise SessionExecutionError(message, status_code)
        return response

    async def start(self, identifier: str, command: str, env: dict[str, str]) -> None:
        """Start `command` as a detached process in the session.

        The first request for an identifier allocates its session. While the pool cannot
        allocate one (HTTP 429) or reach its agent (HTTP 502-504) this retries with
        exponential backoff and jitter, honoring `Retry-After`. Retrying cannot start a
        second process: the agent accepts a single start per session.
        """
        gateway_failed = False
        for attempt in range(START_MAX_ATTEMPTS):
            try:
                await self._request(
                    "POST", "/start", identifier, json={"command": command, "env": env}
                )
                return
            except SessionUnavailableError as exc:
                if attempt == START_MAX_ATTEMPTS - 1:
                    raise
                gateway_failed = gateway_failed or isinstance(exc, SessionGatewayError)
                backoff = min(
                    START_BACKOFF_BASE_SECONDS * 2**attempt, START_BACKOFF_MAX_SECONDS
                )
                await asyncio.sleep(exc.retry_after or random.uniform(backoff / 2, backoff))
            except SessionExecutionError as exc:
                # The agent got the start that a gateway error hid from us, so the process
                # is running and the agent rejects the retry as a second start.
                if gateway_failed and exc.status_code == 409:
                    return
                raise

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
