import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.client.schemas.objects import FlowRun
from prefect.exceptions import InfrastructureNotFound

from prefect_aca_sessions import ACASessionsJobConfiguration, ACASessionsWorker
from prefect_aca_sessions import worker as worker_module
from prefect_aca_sessions.client import SessionExecutionError, SessionNotFoundError


class FakeClient:
    def __init__(self, polls):
        self.polls = list(polls)
        self.started = []
        self.stopped = []

    async def start(self, identifier, command, env):
        self.started.append((identifier, command, env))

    async def poll(self, identifier, offset):
        return self.polls.pop(0)

    async def stop(self, identifier):
        self.stopped.append(identifier)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass


def test_configuration_template_resolves_endpoint():
    template = ACASessionsWorker.get_default_base_job_template()
    props = template["variables"]["properties"]
    assert "pool_management_endpoint" in props
    assert "pool_management_endpoint" not in template["variables"].get("required", [])
    assert template["job_configuration"]["pool_management_endpoint"] == "{{ pool_management_endpoint }}"


async def test_endpoint_falls_back_to_worker_env(monkeypatch):
    monkeypatch.setenv(worker_module.POOL_MANAGEMENT_ENDPOINT_ENV_VAR, "https://from-env")
    template = ACASessionsWorker.get_default_base_job_template()

    config = await ACASessionsJobConfiguration.from_template_and_values(template, {})

    assert config.pool_management_endpoint == "https://from-env"


async def test_job_variable_overrides_worker_env(monkeypatch):
    monkeypatch.setenv(worker_module.POOL_MANAGEMENT_ENDPOINT_ENV_VAR, "https://from-env")
    template = ACASessionsWorker.get_default_base_job_template()

    config = await ACASessionsJobConfiguration.from_template_and_values(
        template, {"pool_management_endpoint": "https://from-deployment"}
    )

    assert config.pool_management_endpoint == "https://from-deployment"


def test_missing_endpoint_raises(monkeypatch):
    monkeypatch.delenv(worker_module.POOL_MANAGEMENT_ENDPOINT_ENV_VAR, raising=False)
    with pytest.raises(ValueError, match=worker_module.POOL_MANAGEMENT_ENDPOINT_ENV_VAR):
        ACASessionsJobConfiguration(command="echo hi")


async def test_run_streams_until_exit(monkeypatch):
    fake = FakeClient(
        [
            {"log": "a\n", "offset": 2, "more": False, "exit_code": None},
            {"log": "b", "offset": 3, "more": True, "exit_code": 0},
            {"log": "c", "offset": 4, "more": False, "exit_code": 0},
        ]
    )
    monkeypatch.setattr(worker_module, "SessionsClient", lambda *a, **k: fake)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(worker_module.asyncio, "sleep", lambda s: real_sleep(0))
    config = ACASessionsJobConfiguration(pool_management_endpoint="https://x", command="echo hi")
    flow_run = SimpleNamespace(id=uuid4(), name="r")
    worker = ACASessionsWorker.__new__(ACASessionsWorker)
    monkeypatch.setattr(worker, "get_flow_run_logger", lambda fr: __import__("logging").getLogger("t"), raising=False)

    result = await worker.run(flow_run, config)

    assert result.status_code == 0
    assert fake.polls == []  # kept polling until the remaining log was drained
    assert result.identifier == str(flow_run.id)
    assert fake.started[0][:2] == (str(flow_run.id), "echo hi")


async def test_run_raises_when_process_cannot_start(monkeypatch):
    class FailingClient(FakeClient):
        async def start(self, identifier, command, env):
            raise SessionExecutionError("HTTP 400")

    monkeypatch.setattr(worker_module, "SessionsClient", lambda *a, **k: FailingClient([]))
    config = ACASessionsJobConfiguration(pool_management_endpoint="https://x", command="echo hi")
    flow_run = SimpleNamespace(id=uuid4(), name="r")
    worker = ACASessionsWorker.__new__(ACASessionsWorker)
    monkeypatch.setattr(worker, "get_flow_run_logger", lambda fr: __import__("logging").getLogger("t"), raising=False)

    with pytest.raises(SessionExecutionError, match="HTTP 400"):
        await worker.run(flow_run, config)


async def test_run_fails_when_session_was_replaced(monkeypatch):
    fake = FakeClient(
        [
            {"log": "a\n", "offset": 2, "more": False, "started": True, "exit_code": None},
            {"log": "", "offset": 0, "more": False, "started": False, "exit_code": None},
        ]
    )
    monkeypatch.setattr(worker_module, "SessionsClient", lambda *a, **k: fake)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(worker_module.asyncio, "sleep", lambda s: real_sleep(0))
    config = ACASessionsJobConfiguration(pool_management_endpoint="https://x", command="echo hi")
    flow_run = SimpleNamespace(id=uuid4(), name="r")
    worker = ACASessionsWorker.__new__(ACASessionsWorker)
    monkeypatch.setattr(worker, "get_flow_run_logger", lambda fr: __import__("logging").getLogger("t"), raising=False)

    result = await worker.run(flow_run, config)

    assert result.status_code == worker_module.FAILURE_EXIT_CODE
    assert fake.polls == []


async def test_kill_infrastructure_stops_the_session(monkeypatch):
    fake = FakeClient([])
    monkeypatch.setattr(worker_module, "SessionsClient", lambda *a, **k: fake)
    config = ACASessionsJobConfiguration(pool_management_endpoint="https://x", command="echo hi")
    worker = ACASessionsWorker.__new__(ACASessionsWorker)

    await worker.kill_infrastructure("session-1", config)

    assert fake.stopped == ["session-1"]


async def test_kill_infrastructure_raises_not_found_for_unknown_session(monkeypatch):
    class MissingClient(FakeClient):
        async def stop(self, identifier):
            raise SessionNotFoundError("HTTP 404")

    monkeypatch.setattr(worker_module, "SessionsClient", lambda *a, **k: MissingClient([]))
    config = ACASessionsJobConfiguration(pool_management_endpoint="https://x", command="echo hi")
    worker = ACASessionsWorker.__new__(ACASessionsWorker)

    with pytest.raises(InfrastructureNotFound):
        await worker.kill_infrastructure("session-1", config)
