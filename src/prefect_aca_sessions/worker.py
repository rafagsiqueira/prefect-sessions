import asyncio
import json

import anyio
import anyio.abc
from prefect.client.schemas.objects import FlowRun
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field

from prefect_aca_sessions.client import SessionsClient
from prefect_aca_sessions.snippets import build_poll_code, build_start_code

DEFAULT_API_VERSION = "2025-10-02-preview"
DEFAULT_POLL_INTERVAL_SECONDS = 10
FAILURE_EXIT_CODE = -1


class ACASessionsJobConfiguration(BaseJobConfiguration):
    """Job configuration for flow runs executed in an ACA code interpreter session."""

    pool_management_endpoint: str = Field(
        description=(
            "Management endpoint of the code interpreter session pool, e.g. "
            "https://<region>.dynamicsessions.io/subscriptions/<id>/resourceGroups/<rg>"
            "/sessionPools/<pool>"
        ),
        json_schema_extra=dict(template="{{ pool_management_endpoint }}"),
    )
    api_version: str = Field(
        default=DEFAULT_API_VERSION,
        description="Sessions data-plane API version.",
        json_schema_extra=dict(template="{{ api_version }}"),
    )
    session_identifier: str | None = Field(
        default=None,
        description="Session identifier. Defaults to the flow run ID (one session per run).",
        json_schema_extra=dict(template="{{ session_identifier }}"),
    )
    pip_packages: list[str] = Field(
        default_factory=lambda: ["prefect"],
        description="Packages pip-installed in the session before the flow run starts.",
        json_schema_extra=dict(template="{{ pip_packages }}"),
    )
    poll_interval_seconds: int = Field(
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        ge=1,
        description="Seconds between checks of the flow run process.",
        json_schema_extra=dict(template="{{ poll_interval_seconds }}"),
    )
    delete_session_on_completion: bool = Field(
        default=True,
        description="Delete the session once the flow run process exits.",
        json_schema_extra=dict(template="{{ delete_session_on_completion }}"),
    )


class ACASessionsVariables(BaseVariables):
    pool_management_endpoint: str = Field(
        description="Management endpoint of the code interpreter session pool.",
    )
    api_version: str = Field(default=DEFAULT_API_VERSION, description="Sessions API version.")
    session_identifier: str | None = Field(
        default=None, description="Session identifier. Defaults to the flow run ID."
    )
    pip_packages: list[str] = Field(
        default_factory=lambda: ["prefect"],
        description="Packages pip-installed in the session before the run.",
    )
    poll_interval_seconds: int = Field(
        default=DEFAULT_POLL_INTERVAL_SECONDS, ge=1, description="Seconds between status checks."
    )
    delete_session_on_completion: bool = Field(
        default=True, description="Delete the session after the run."
    )


class ACASessionsWorkerResult(BaseWorkerResult):
    """Result of a flow run executed in an ACA session."""


class ACASessionsWorker(BaseWorker):
    type = "azure-container-apps-sessions"
    job_configuration = ACASessionsJobConfiguration
    job_configuration_variables = ACASessionsVariables
    _description = "Runs flow runs in Azure Container Apps dynamic sessions (code interpreter)."
    _documentation_url = (
        "https://learn.microsoft.com/en-us/azure/container-apps/sessions-code-interpreter"
    )

    async def run(
        self,
        flow_run: FlowRun,
        configuration: ACASessionsJobConfiguration,
        task_status: anyio.abc.TaskStatus | None = None,
    ) -> ACASessionsWorkerResult:
        identifier = configuration.session_identifier or str(flow_run.id)
        logger = self.get_flow_run_logger(flow_run)

        async with SessionsClient(
            configuration.pool_management_endpoint, configuration.api_version
        ) as client:
            try:
                exit_code = await self._execute(client, identifier, configuration, logger, task_status)
            except Exception:
                logger.exception("Flow run failed in session %s", identifier)
                exit_code = FAILURE_EXIT_CODE
            finally:
                if configuration.delete_session_on_completion:
                    await self._delete_quietly(client, identifier, logger)

        return ACASessionsWorkerResult(status_code=exit_code, identifier=identifier)

    async def _execute(self, client, identifier, configuration, logger, task_status) -> int:
        await client.execute(
            identifier,
            build_start_code(
                configuration.command or "", configuration.env, configuration.pip_packages
            ),
        )
        logger.info("Started flow run process in session %s", identifier)
        if task_status is not None:
            task_status.started(identifier)

        offset = 0
        while True:
            await asyncio.sleep(configuration.poll_interval_seconds)
            poll = json.loads((await client.execute(identifier, build_poll_code(offset))).stdout)
            offset = poll["offset"]
            if poll["log"]:
                logger.info(poll["log"].rstrip())
            if poll["exit_code"] is not None:
                return poll["exit_code"]

    @staticmethod
    async def _delete_quietly(client, identifier, logger) -> None:
        try:
            await client.delete_session(identifier)
        except Exception:
            logger.warning("Could not delete session %s", identifier, exc_info=True)
