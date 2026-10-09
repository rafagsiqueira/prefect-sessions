# prefect-aca-sessions

A Prefect worker that runs flow runs in
[Azure Container Apps dynamic sessions](https://learn.microsoft.com/en-us/azure/container-apps/sessions-custom-container)
(**custom container** session pools). Worker type: `azure-container-apps-sessions`.

> Status: unit-tested only. It has not yet been verified against a real session pool.

## How it works

Custom container pools have no code execution endpoint. The pool management endpoint instead
proxies requests to an HTTP server in your container, so the image runs a small agent
(`python -m prefect_aca_sessions.agent`, port 8080) that this worker talks to.

For each flow run the worker:

1. Authenticates to the pool's management endpoint with a Microsoft Entra token
   (`DefaultAzureCredential`, audience `https://dynamicsessions.io`).
2. Calls `POST /start` on the session (identifier defaults to the flow run ID), which starts
   `prefect flow-run execute` as a detached process in the container.
3. Calls `GET /poll` until the process exits, forwarding its output to the flow run logs.

## Prerequisites

- A self-hosted Prefect server reachable from **both** the worker and the sessions.
- A container image with `prefect` and `prefect-aca-sessions` installed, whose entrypoint is
  `python -m prefect_aca_sessions.agent` (e.g. `FROM python:3.12-slim`,
  `RUN pip install prefect prefect-aca-sessions`). Pin the Prefect version to match your server.
- An Azure Container Apps **custom container session pool**
  (`az containerapp sessionpool create --container-type CustomContainer --target-port 8080 ...`).
  Note its *pool management endpoint*:
  `az containerapp sessionpool show -n <pool> -g <rg> --query properties.poolManagementEndpoint -o tsv`
- An identity for the worker with the **Azure ContainerApps Session Executor** role on the pool.
- Sessions with network egress to your Prefect API.
- Python 3.12+ and [uv](https://docs.astral.sh/uv/).

## 1. Install the worker

The package must be installed in the environment where `prefect` runs, because Prefect
discovers worker types through the `prefect.collections` entry point.

```bash
uv pip install prefect-aca-sessions                               # or, from a checkout: uv pip install -e .
prefect worker start --help                                       # sanity check
```

## 2. Point the CLI at your server

```bash
export PREFECT_API_URL=https://prefect.example.com/api
# export PREFECT_API_KEY=...   # only if your server has basic auth / a gateway key
```

## 3. Create the work pool

```bash
prefect work-pool create aca-sessions --type azure-container-apps-sessions
```

The session pool endpoint can be set in three places. Each one overrides the ones after it:

1. Per deployment, with the `pool_management_endpoint` job variable.
2. As the pool default, in the UI (Work Pools → `aca-sessions` → Edit → **Pool management endpoint**).
3. On the worker host, with the `ACA_SESSIONS_POOL_MANAGEMENT_ENDPOINT` environment variable
   (see step 5).

## 4. Authenticate the worker to Azure

`DefaultAzureCredential` is used, so any of these work:

- **Managed identity** (worker on an Azure VM, ACA, AKS): assign the Session Executor role to it.
- **Service principal**: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`.
- **Local development**: `az login`.

## 5. Start the worker

```bash
export ACA_SESSIONS_POOL_MANAGEMENT_ENDPOINT=https://<region>.dynamicsessions.io/subscriptions/<sub>/resourceGroups/<rg>/sessionPools/<pool>
prefect worker start --pool aca-sessions --type azure-container-apps-sessions
```

Run it under a process manager (systemd, a container, etc.) for production.

## 6. Deploy a flow to the pool

```python
from prefect import flow


@flow(log_prints=True)
def hello():
    print("running in an ACA session")


if __name__ == "__main__":
    hello.from_source(
        source="https://github.com/<you>/<repo>",
        entrypoint="flows.py:hello",
    ).deploy(
        name="hello-aca",
        work_pool_name="aca-sessions",
        job_variables={"poll_interval_seconds": 5},
    )
```

The container has to fetch the flow code itself (git source, or a pull step), because nothing
is shared with the worker's filesystem. Add private-repo credentials via the deployment's
pull steps and `env`.

## Job variables

| Variable | Default | Description |
| --- | --- | --- |
| `pool_management_endpoint` | `$ACA_SESSIONS_POOL_MANAGEMENT_ENDPOINT` on the worker | Session pool management endpoint URL. A flow run fails if neither is set |
| `session_identifier` | flow run ID | Session to run in. A session runs a single flow run process, so a fixed value works for one run only |
| `poll_interval_seconds` | `10` | Seconds between status polls |
| `stop_session_on_exit` | `true` | Stop the session once the flow run process exits, instead of waiting for the pool's cooldown |
| `env` | `{}` | Extra environment variables for the flow run process |
| `command` | `prefect flow-run execute` | Command run in the session |

## Security notes

- The flow run's environment (including `PREFECT_API_URL` and any `PREFECT_API_KEY`) is sent
  to the session inside the execution request. Use a dedicated, least-privilege API key.
- Session identifiers are sensitive; don't share a fixed identifier across tenants.

## Known limitations

- Cancelling a flow run that is still pending stops its session through the pool's
  `stopSession` API (`2025-02-02-preview`). A flow run that is already running is cancelled by
  `prefect flow-run execute` inside the session.
- The worker stops a session only after it sees the flow run process exit. If the worker loses
  track of a run (it restarts, or polling fails), the session ends at the pool's cooldown.
- A session runs one process: starting a second one in the same session is rejected.
- Session lifetime and idle limits are set on the pool and apply to long flow runs.

## Development

```bash
uv sync
uv run pytest
```
