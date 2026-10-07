"""Minimal async client for the Azure Container Apps code interpreter management endpoint."""

import json
from dataclasses import dataclass
from typing import Any

import httpx
from azure.core.credentials_async import AsyncTokenCredential
from azure.identity.aio import DefaultAzureCredential

SESSIONS_TOKEN_SCOPE = "https://dynamicsessions.io/.default"
REQUEST_TIMEOUT_SECONDS = 300.0  # a single execution may run up to 220s


class SessionExecutionError(RuntimeError):
    """The sessions endpoint rejected a request or the executed code failed."""


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    stdout: str
    stderr: str


class SessionsClient:
    """Runs inline Python code in a session of a code interpreter session pool."""

    def __init__(
        self,
        pool_management_endpoint: str,
        api_version: str,
        credential: AsyncTokenCredential | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._endpoint = pool_management_endpoint.rstrip("/")
        self._api_version = api_version
        self._credential = credential or DefaultAzureCredential()
        self._http = http_client or httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS)

    async def __aenter__(self) -> "SessionsClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._http.aclose()
        await self._credential.close()

    async def _headers(self) -> dict[str, str]:
        token = await self._credential.get_token(SESSIONS_TOKEN_SCOPE)
        return {"Authorization": f"Bearer {token.token}"}

    def _params(self, identifier: str) -> dict[str, str]:
        return {"api-version": self._api_version, "identifier": identifier}

    async def execute(self, identifier: str, code: str) -> ExecutionResult:
        response = await self._http.post(
            f"{self._endpoint}/executions",
            params=self._params(identifier),
            headers=await self._headers(),
            json={
                "properties": {
                    "codeInputType": "inline",
                    "executionType": "synchronous",
                    "code": code,
                }
            },
        )
        if response.is_error:
            raise SessionExecutionError(
                f"Session pool returned HTTP {response.status_code}: {response.text}"
            )
        return _parse_execution(response.json())

    async def delete_session(self, identifier: str) -> None:
        response = await self._http.delete(
            f"{self._endpoint}/session",
            params=self._params(identifier),
            headers=await self._headers(),
        )
        if response.is_error and response.status_code != 404:
            raise SessionExecutionError(
                f"Deleting session failed with HTTP {response.status_code}: {response.text}"
            )


def _parse_execution(body: dict[str, Any]) -> ExecutionResult:
    properties = body.get("properties", {})
    result = ExecutionResult(
        status=properties.get("status", "Unknown"),
        stdout=properties.get("stdout", ""),
        stderr=properties.get("stderr", ""),
    )
    if result.status != "Succeeded":
        raise SessionExecutionError(
            f"Code execution ended with status {result.status!r}: "
            f"{json.dumps(properties.get('result'))} {result.stderr}"
        )
    return result
