import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.client.schemas.objects import FlowRun

from prefect_aca_sessions import ACASessionsJobConfiguration, ACASessionsWorker
from prefect_aca_sessions import worker as worker_module
from prefect_aca_sessions.client import ExecutionResult


class FakeClient:
    def __init__(self, polls):
        self.polls = list(polls)
        self.calls = []
        self.deleted = []

    async def execute(self, identifier, code):
        self.calls.append(identifier)
        if len(self.calls) == 1:
            return ExecutionResult("Succeeded", "123\n", "")
        return ExecutionResult("Succeeded", self.polls.pop(0), "")

    async def delete_session(self, identifier):
        self.deleted.append(identifier)

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


async def test_run_streams_until_exit_and_deletes_session(monkeypatch):
    fake = FakeClient(
        [
            '{"log": "a\\n", "offset": 2, "more": false, "exit_code": null}',
            '{"log": "b", "offset": 3, "more": true, "exit_code": 0}',
            '{"log": "c", "offset": 4, "more": false, "exit_code": 0}',
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
    assert fake.deleted == [str(flow_run.id)]
