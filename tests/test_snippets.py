import json
import subprocess
import sys
import time

from prefect_aca_sessions import snippets


def run_code(code: str, run_dir: str) -> str:
    code = code.replace(snippets.RUN_DIR, run_dir)
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout


def poll(run_dir: str, offset: int = 0) -> dict:
    return json.loads(run_code(snippets.build_poll_code(offset), run_dir))


def wait_for_exit(run_dir: str) -> dict:
    for _ in range(50):
        result = poll(run_dir)
        if result["exit_code"] is not None:
            return result
        time.sleep(0.1)
    raise AssertionError("process did not finish")


def test_start_code_runs_command_with_env_and_reports_exit_code(tmp_path):
    run_dir = str(tmp_path)
    code = snippets.build_start_code("echo $GREETING; exit 3", {"GREETING": "hello"}, [])

    run_code(code, run_dir)
    result = wait_for_exit(run_dir)

    assert result["exit_code"] == 3
    assert result["log"] == "hello\n"


def test_poll_returns_only_log_after_offset(tmp_path):
    run_dir = str(tmp_path)
    run_code(snippets.build_start_code("echo one", {}, []), run_dir)
    first = wait_for_exit(run_dir)

    again = poll(run_dir, first["offset"])

    assert again["log"] == ""
    assert again["offset"] == first["offset"]


def test_poll_returns_long_log_in_chunks_without_splitting_characters(tmp_path):
    run_dir = str(tmp_path)
    text = "é" * snippets.MAX_LOG_CHUNK_BYTES  # 2 bytes each, odd chunk boundary below
    (tmp_path / "output.log").write_text("x" + text)
    (tmp_path / "exit_code").write_text("0")

    log, offset, polls = "", 0, 0
    while True:
        result = poll(run_dir, offset)
        log, offset, polls = log + result["log"], result["offset"], polls + 1
        if not result["more"]:
            break

    assert log == "x" + text
    assert polls > 1
    assert result["exit_code"] == 0
