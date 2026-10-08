import asyncio
import os

import anyio
import anyio.abc
from prefect.client.schemas.objects import FlowRun
from prefect.exceptions import InfrastructureNotFound
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, model_validator

from prefect_aca_sessions.client import SessionNotFoundError, SessionsClient

DEFAULT_POLL_INTERVAL_SECONDS = 10
FAILURE_EXIT_CODE = -1
POOL_MANAGEMENT_ENDPOINT_ENV_VAR = "ACA_SESSIONS_POOL_MANAGEMENT_ENDPOINT"


class ACASessionsJobConfiguration(BaseJobConfiguration):
    """Job configuration for flow runs executed in an ACA custom container session."""

    pool_management_endpoint: str | None = Field(
        default=None,
        description=(
            "Management endpoint of the custom container session pool, e.g. "
            "https://<region>.dynamicsessions.io/subscriptions/<id>/resourceGroups/<rg>"
            f"/sessionPools/<pool>. Defaults to ${POOL_MANAGEMENT_ENDPOINT_ENV_VAR} "
            "on the worker."
        ),
        json_schema_extra=dict(template="{{ pool_management_endpoint }}"),
    )
    session_identifier: str | None = Field(
        default=None,
        description="Session identifier. Defaults to the flow run ID (one session per run).",
        json_schema_extra=dict(template="{{ session_identifier }}"),
    )
    poll_interval_seconds: int = Field(
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        ge=1,
        description="Seconds between checks of the flow run process.",
        json_schema_extra=dict(template="{{ poll_interval_seconds }}"),
    )

    @model_validator(mode="after")
    def _resolve_pool_management_endpoint(self):
        # Resolved in the worker process, so the endpoint can be set once on the worker host.
        self.pool_management_endpoint = self.pool_management_endpoint or os.environ.get(
            POOL_MANAGEMENT_ENDPOINT_ENV_VAR
        )
        if not self.pool_management_endpoint:
            raise ValueError(
                "pool_management_endpoint is not set: provide it as a job variable or set "
                f"{POOL_MANAGEMENT_ENDPOINT_ENV_VAR} in the worker's environment."
            )
        return self


class ACASessionsVariables(BaseVariables):
    pool_management_endpoint: str | None = Field(
        default=None,
        description=(
            "Management endpoint of the custom container session pool. "
            f"Defaults to ${POOL_MANAGEMENT_ENDPOINT_ENV_VAR} on the worker."
        ),
    )
    session_identifier: str | None = Field(
        default=None, description="Session identifier. Defaults to the flow run ID."
    )
    poll_interval_seconds: int = Field(
        default=DEFAULT_POLL_INTERVAL_SECONDS, ge=1, description="Seconds between status checks."
    )


class ACASessionsWorkerResult(BaseWorkerResult):
    """Result of a flow run executed in an ACA session."""


class ACASessionsWorker(BaseWorker):
    type = "azure-container-apps-sessions"
    job_configuration = ACASessionsJobConfiguration
    job_configuration_variables = ACASessionsVariables
    _description = "Runs flow runs in Azure Container Apps dynamic sessions (custom container)."
    _documentation_url = (
        "https://learn.microsoft.com/en-us/azure/container-apps/sessions-custom-container"
    )

    async def run(
        self,
        flow_run: FlowRun,
        configuration: ACASessionsJobConfiguration,
        task_status: anyio.abc.TaskStatus | None = None,
    ) -> ACASessionsWorkerResult:
        identifier = configuration.session_identifier or str(flow_run.id)
        logger = self.get_flow_run_logger(flow_run)

        async with SessionsClient(configuration.pool_management_endpoint) as client:
            await client.start(identifier, configuration.command or "", configuration.env)
            logger.info("Started flow run process in session %s", identifier)
            if task_status is not None:
                task_status.started(identifier)
            try:
                exit_code = await self._wait_for_exit(client, identifier, configuration, logger)
            except Exception:
                logger.exception("Flow run failed in session %s", identifier)
                exit_code = FAILURE_EXIT_CODE

        return ACASessionsWorkerResult(status_code=exit_code, identifier=identifier)

    async def _wait_for_exit(self, client, identifier, configuration, logger) -> int:
        offset = 0
        more = False
        while True:
            if not more:
                await asyncio.sleep(configuration.poll_interval_seconds)
            poll = await client.poll(identifier, offset)
            if not poll.get("started", True):
                # The session was stopped (e.g. by kill_infrastructure) and the pool handed
                # this identifier a fresh session that never ran the flow.
                logger.error("Session %s no longer runs the flow run process", identifier)
                return FAILURE_EXIT_CODE
            offset, more = poll["offset"], poll["more"]
            if poll["log"]:
                logger.info(poll["log"].rstrip())
            if poll["exit_code"] is not None and not more:
                return poll["exit_code"]

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: ACASessionsJobConfiguration,
        grace_seconds: int = 30,
    ) -> None:
        """Stop the flow run's session. The pool ends it at once, so `grace_seconds` is unused."""
        async with SessionsClient(configuration.pool_management_endpoint) as client:
            try:
                await client.stop(infrastructure_pid)
            except SessionNotFoundError as exc:
                raise InfrastructureNotFound(f"Session {infrastructure_pid} not found") from exc
