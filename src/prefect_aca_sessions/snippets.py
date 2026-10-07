"""Python snippets executed inside the session to launch and observe the flow run process."""

import json

RUN_DIR = "/mnt/data/prefect-run"


def build_start_code(command: str, env: dict[str, str], pip_packages: list[str]) -> str:
    """Code that starts `command` as a detached process and returns immediately."""
    spec = json.dumps({"command": command, "env": env, "pip": pip_packages})
    return f"""
import json, os, subprocess
spec = json.loads({spec!r})
run_dir = {RUN_DIR!r}
os.makedirs(run_dir, exist_ok=True)
for name in ("exit_code", "output.log"):
    path = os.path.join(run_dir, name)
    if os.path.exists(path):
        os.remove(path)
steps = []
if spec["pip"]:
    steps.append("pip install --quiet " + " ".join(spec["pip"]))
steps.append(spec["command"])
script = " && ".join("(" + s + ")" for s in steps) + "; echo $? > " + run_dir + "/exit_code"
log = open(run_dir + "/output.log", "ab")
proc = subprocess.Popen(
    ["sh", "-c", script],
    env={{**os.environ, **spec["env"]}},
    stdout=log,
    stderr=subprocess.STDOUT,
    start_new_session=True,
)
print(proc.pid)
"""


def build_poll_code(log_offset: int) -> str:
    """Code printing JSON: new log text, next log offset and the exit code (or null)."""
    return f"""
import json, os
run_dir = {RUN_DIR!r}
offset = {log_offset}
exit_path = run_dir + "/exit_code"
# read the exit code first so a run finishing mid-poll never loses its last log lines
exit_code = int(open(exit_path).read().strip()) if os.path.exists(exit_path) else None
with open(run_dir + "/output.log", "rb") as f:
    f.seek(offset)
    chunk = f.read()
print(json.dumps({{
    "log": chunk.decode("utf-8", "replace"),
    "offset": offset + len(chunk),
    "exit_code": exit_code,
}}))
"""

