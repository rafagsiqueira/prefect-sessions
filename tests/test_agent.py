import json
import threading
import time
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from prefect_aca_sessions.agent import RunState, make_handler


@pytest.fixture
def base_url(tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(RunState(str(tmp_path))))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


def post_start(base_url, body):
    request = Request(f"{base_url}/start", data=json.dumps(body).encode(), method="POST")
    return json.load(urlopen(request))


def poll(base_url, offset=0):
    return json.load(urlopen(f"{base_url}/poll?offset={offset}"))


def wait_for_exit(base_url):
    for _ in range(100):
        result = poll(base_url)
        if result["exit_code"] is not None:
            return result
        time.sleep(0.05)
    raise AssertionError("process did not exit")


def test_runs_command_with_env_and_reports_output_and_exit_code(base_url):
    post_start(base_url, {"command": "echo $GREETING; exit 3", "env": {"GREETING": "hi"}})

    result = wait_for_exit(base_url)

    assert result["log"] == "hi\n"
    assert result["exit_code"] == 3


def test_poll_before_start_has_no_exit_code(base_url):
    assert poll(base_url) == {"log": "", "offset": 0, "more": False, "exit_code": None}


def test_second_start_is_rejected(base_url):
    post_start(base_url, {"command": "true"})

    with pytest.raises(HTTPError) as exc:
        post_start(base_url, {"command": "true"})

    assert exc.value.code == 409


def test_invalid_start_body_is_rejected(base_url):
    with pytest.raises(HTTPError) as exc:
        post_start(base_url, {"env": {}})

    assert exc.value.code == 400
