# prefect-aca-sessions

A Prefect worker that runs flow runs in
[Azure Container Apps dynamic sessions](https://learn.microsoft.com/en-us/azure/container-apps/sessions-code-interpreter)
(code interpreter session pools). Worker type: `azure-container-apps-sessions`.

> Status: unit-tested only. It has not yet been verified against a real session pool.

## How it works

The worker is a long-running process that polls a Prefect work pool. For each flow run it:

1. Authenticates to the pool's management endpoint with a Microsoft Entra token
   (`DefaultAzureCredential`, audience `https://dynamicsessions.io`).
2. Starts `prefect flow-run execute` as a detached process inside a session (after
   `pip install`-ing `pip_packages`). The session identifier defaults to the flow run ID.
3. Polls the session, forwards the process output to the flow run logs, and reports the exit code.
4. Deletes the session (unless `delete_session_on_completion` is `false`).

A single `/executions` call is limited to 220 seconds, which is why the flow runs as a
background process that is polled rather than inside one call.

## Prerequisites

- A self-hosted Prefect server reachable from **both** the worker and the sessions.
- An Azure Container Apps **code interpreter session pool**
  (`az containerapp sessionpool create --container-type PythonLTS ...`). Note its
  *pool management endpoint*:
  `az containerapp sessionpool show -n <pool> -g <rg> --query properties.poolManagementEndpoint -o tsv`
- An identity for the worker with the **Azure ContainerApps Session Executor** role on the pool.
- Sessions with network egress to your Prefect API (the session pool's network status must
  be `EgressEnabled`) and to PyPI, or a pool with the packages you need already available.
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
        job_variables={
            "pip_packages": ["prefect", "pandas"],
        },
    )
```

The session has to fetch the flow code itself (git source, or a pull step), because nothing
is shared with the worker's filesystem. Add private-repo credentials via the deployment's
pull steps and `env`.

## Job variables

| Variable | Default | Description |
| --- | --- | --- |
| `pool_management_endpoint` | `$ACA_SESSIONS_POOL_MANAGEMENT_ENDPOINT` on the worker | Session pool management endpoint URL. A flow run fails if neither is set |
| `api_version` | `2025-10-02-preview` | Sessions data-plane API version |
| `session_identifier` | flow run ID | Session to run in. A fixed value reuses one session across runs, so runs can see each other's files |
| `pip_packages` | `["prefect"]` | Installed in the session before the run. Pin the Prefect version to match your server |
| `poll_interval_seconds` | `10` | Seconds between status polls |
| `delete_session_on_completion` | `true` | Delete the session after the run |
| `env` | `{}` | Extra environment variables for the flow run process |
| `command` | `prefect flow-run execute` | Command run in the session |

## Security notes

- The flow run's environment (including `PREFECT_API_URL` and any `PREFECT_API_KEY`) is sent
  to the session inside the execution request. Use a dedicated, least-privilege API key.
- Session identifiers are sensitive; don't share a fixed identifier across tenants.

## Known limitations

- Cancelling a flow run does not stop the process in the session (`kill_infrastructure` is not
  implemented); delete the session to stop it.
- Session lifetime and idle limits are set on the pool and apply to long flow runs.

## Development

```bash
uv sync
uv run pytest
```
