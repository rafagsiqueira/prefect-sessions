"""HTTP agent run inside a custom container session image.

Custom container session pools proxy requests to the container's HTTP port, so the worker
cannot execute code directly. This agent exposes the two operations the worker needs:

- ``POST /start`` with ``{"command": str, "env": {str: str}}`` starts a detached process.
- ``GET /poll?offset=N`` returns ``{"log", "offset", "more", "exit_code"}``.

``GET /``, ``/health``, ``/healthz`` and ``/ready`` return 200 for container probes.

Run it as the image entrypoint: ``python -m prefect_aca_sessions.agent`` (port 8080).
"""

import json
import os
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DEFAULT_PORT = 8080
HEALTH_PATHS = frozenset({"/", "/health", "/healthz", "/ready"})
# Bytes of log returned per poll.
MAX_LOG_CHUNK_BYTES = 65536
MAX_UTF8_CHAR_BYTES = 4


class RunState:
    """The single flow run process of this session and its log file."""

    def __init__(self, run_dir: str) -> None:
        self._log_path = os.path.join(run_dir, "output.log")
        self._lock = threading.Lock()
        self._started = False
        self._exit_code: int | None = None

    @property
    def is_started(self) -> bool:
        with self._lock:
            return self._started

    def start(self, command: str, env: dict[str, str]) -> int:
        with self._lock:
            if self._started:
                raise RuntimeError("A process was already started in this session")
            self._started = True
        log = open(self._log_path, "ab")
        proc = subprocess.Popen(
            ["sh", "-c", command],
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        threading.Thread(target=self._wait, args=(proc, log), daemon=True).start()
        return proc.pid

    def _wait(self, proc: subprocess.Popen, log) -> None:
        code = proc.wait()
        log.close()
        with self._lock:
            self._exit_code = code

    def poll(self, offset: int) -> dict:
        # read the exit code first so a run finishing mid-poll never loses its last log lines
        with self._lock:
            exit_code = self._exit_code
        if not os.path.exists(self._log_path):
            return {"log": "", "offset": offset, "more": False, "exit_code": exit_code}
        with open(self._log_path, "rb") as f:
            f.seek(offset)
            chunk = f.read(MAX_LOG_CHUNK_BYTES)
            more = bool(f.read(1))
        if more:
            chunk = _trim_partial_character(chunk)
        return {
            "log": chunk.decode("utf-8", "replace"),
            "offset": offset + len(chunk),
            "more": more,
            "exit_code": exit_code,
        }


def _trim_partial_character(chunk: bytes) -> bytes:
    """Drop trailing bytes of a multi-byte character cut by the chunk boundary."""
    for trim in range(MAX_UTF8_CHAR_BYTES):
        candidate = chunk[: len(chunk) - trim]
        try:
            candidate.decode("utf-8")
        except UnicodeDecodeError:
            continue
        return candidate
    return chunk


def make_handler(state: RunState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, status: int, body: dict) -> None:
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            url = urlparse(self.path)
            if url.path in HEALTH_PATHS:
                return self._reply(200, {"status": "ok"})
            if url.path == "/poll":
                try:
                    offset = int(parse_qs(url.query).get("offset", ["0"])[0])
                except ValueError:
                    return self._reply(400, {"error": "offset must be an integer"})
                return self._reply(200, state.poll(max(offset, 0)))
            self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/start":
                return self._reply(404, {"error": "not found"})
            try:
                length = int(self.headers.get("Content-Length", "0"))
                spec = json.loads(self.rfile.read(length))
                command, env = spec["command"], spec.get("env", {})
                if not isinstance(command, str) or not isinstance(env, dict):
                    raise TypeError("command must be a string and env an object")
            except (ValueError, KeyError, TypeError) as exc:
                return self._reply(400, {"error": f"invalid request: {exc}"})
            try:
                pid = state.start(command, {str(k): str(v) for k, v in env.items()})
            except RuntimeError as exc:
                return self._reply(409, {"error": str(exc)})
            self._reply(200, {"pid": pid})

        def log_message(self, format: str, *args) -> None:
            pass

    return Handler


def main() -> None:
    port = int(os.environ.get("PORT", DEFAULT_PORT))
    state = RunState(tempfile.mkdtemp(prefix="prefect-run-"))
    ThreadingHTTPServer(("0.0.0.0", port), make_handler(state)).serve_forever()


if __name__ == "__main__":
    main()
