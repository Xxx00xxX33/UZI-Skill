from __future__ import annotations

import json
import errno
import queue
import threading
import time
from pathlib import Path
from typing import Any

import requests

from telegram_uzi_bot import (
    LeaseStore,
    UziTelegramBot,
    bot_was_mentioned,
    extract_request,
    parse_stock_request,
)


def test_parse_stock_request_removes_bot_mention():
    query = parse_stock_request("@uzi_robot 600519.SH", "uzi_robot")
    assert query == "600519.SH"


def test_extract_request_uses_entities_for_mention():
    message = {
        "text": "@uzi_robot 002273.SZ",
        "entities": [
            {"type": "mention", "offset": 0, "length": 10},
        ],
    }
    assert bot_was_mentioned(message, "uzi_robot") is True
    assert extract_request(message, "uzi_robot") == "002273.SZ"


def test_extract_request_supports_analyze_command():
    message = {
        "text": "/analyze@uzi_robot 贵州茅台",
        "entities": [
            {"type": "bot_command", "offset": 0, "length": 18},
        ],
    }
    assert extract_request(message, "uzi_robot") == "贵州茅台"


def test_lease_store_cleanup_removes_expired_file(tmp_path: Path):
    store = LeaseStore(tmp_path / "leases.json", public_dir=tmp_path, ttl_seconds=1)
    report = tmp_path / "report.html"
    report.write_text("ok", encoding="utf-8")

    lease = store.create(report)
    public_file = tmp_path / lease.filename
    assert public_file.exists()

    payload = json.loads((tmp_path / "leases.json").read_text(encoding="utf-8"))
    first_key = next(iter(payload))
    payload[first_key]["expires_at"] = time.time() - 1
    (tmp_path / "leases.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    store = LeaseStore(tmp_path / "leases.json", public_dir=tmp_path, ttl_seconds=1)
    store.cleanup()
    assert not public_file.exists()


def test_lease_store_keeps_metadata_outside_public_dir(tmp_path: Path):
    public_dir = tmp_path / "public"
    store = LeaseStore(tmp_path / "leases.json", public_dir=public_dir, ttl_seconds=60)
    report = tmp_path / "report.html"
    report.write_text("ok", encoding="utf-8")

    lease = store.create(report)

    assert (public_dir / lease.filename).exists()
    assert not (tmp_path / lease.filename).exists()


def test_choose_python_bin_prefers_configured_path(tmp_path: Path):
    configured = tmp_path / "bin" / "python"
    configured.parent.mkdir(parents=True)
    configured.write_text("", encoding="utf-8")

    chosen = UziTelegramBot._choose_python_bin(str(configured))

    assert chosen == configured.resolve()


def test_choose_python_bin_preserves_configured_venv_symlink(tmp_path: Path):
    target = tmp_path / "python-real"
    target.write_text("", encoding="utf-8")
    configured = tmp_path / "bin" / "python"
    configured.parent.mkdir(parents=True)
    configured.symlink_to(target)

    chosen = UziTelegramBot._choose_python_bin(str(configured))

    assert chosen == configured


def test_choose_python_bin_falls_back_to_runtime(monkeypatch):
    monkeypatch.setattr(
        "telegram_uzi_bot.DEFAULT_VENV_PYTHON", Path("/tmp/does-not-exist")
    )
    monkeypatch.setattr("telegram_uzi_bot.sys.executable", "/usr/bin/python3")

    chosen = UziTelegramBot._choose_python_bin("")

    assert chosen == Path("/usr/bin/python3").resolve()


def test_choose_python_bin_preserves_venv_symlink_path(monkeypatch, tmp_path: Path):
    target = tmp_path / "python-real"
    target.write_text("", encoding="utf-8")
    venv_python = tmp_path / "venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.symlink_to(target)

    monkeypatch.setattr("telegram_uzi_bot.DEFAULT_VENV_PYTHON", venv_python)

    chosen = UziTelegramBot._choose_python_bin("")

    assert chosen == venv_python


def test_send_message_uses_reply_parameters():
    bot = object.__new__(UziTelegramBot)
    captured: dict[str, object] = {}

    def fake_api(method: str, *, params=None, json_body=None):
        captured["method"] = method
        captured["json_body"] = json_body
        return {"message_id": 1}

    setattr(bot, "_api", fake_api)

    bot.send_message(123, "hello", reply_to_message_id=456, message_thread_id=789)

    assert captured["method"] == "sendMessage"
    assert captured["json_body"] == {
        "chat_id": 123,
        "text": "hello",
        "disable_web_page_preview": False,
        "reply_parameters": {
            "message_id": 456,
            "allow_sending_without_reply": True,
        },
        "message_thread_id": 789,
    }


def test_safe_send_message_swallows_send_errors():
    bot = object.__new__(UziTelegramBot)

    def failing_send_message(*args, **kwargs):
        raise RuntimeError("send failed")

    setattr(bot, "send_message", failing_send_message)

    bot._safe_send_message(123, "hello", reply_to_message_id=456)


def test_safe_send_message_redacts_token_from_errors(capsys):
    token = "123456:SECRET"
    bot = object.__new__(UziTelegramBot)
    bot.token = token

    def failing_send_message(*args, **kwargs):
        raise RuntimeError(f"send failed for {token}")

    setattr(bot, "send_message", failing_send_message)

    bot._safe_send_message(123, "hello", reply_to_message_id=456)

    stderr = capsys.readouterr().err
    assert token not in stderr
    assert "<redacted>" in stderr


def test_build_session_disables_env_proxy_by_default(monkeypatch):
    monkeypatch.delenv("UZI_TELEGRAM_USE_ENV_PROXY", raising=False)

    session = UziTelegramBot._build_session()

    assert session.trust_env is False
    assert session.headers["User-Agent"] == "uzi-telegram-bot/1.0"


def test_build_session_can_opt_in_env_proxy(monkeypatch):
    monkeypatch.setenv("UZI_TELEGRAM_USE_ENV_PROXY", "1")

    session = UziTelegramBot._build_session()

    assert session.trust_env is True


def test_locate_report_from_output_supports_plain_label_and_full_width_colon(
    tmp_path: Path,
):
    report = tmp_path / "full-report-standalone.html"
    report.write_text("ok", encoding="utf-8")
    bot = object.__new__(UziTelegramBot)

    stdout = f"处理完成\n报告路径： {report}\n"

    assert bot.locate_report_from_output(stdout) == report


def test_locate_report_from_output_falls_back_to_report_filename_match(tmp_path: Path):
    report = tmp_path / "full-report-standalone.html"
    report.write_text("ok", encoding="utf-8")
    bot = object.__new__(UziTelegramBot)

    stdout = f"saved report at {report}\n"

    assert bot.locate_report_from_output(stdout) == report


def test_run_uzi_enables_fast_path_envs_by_default(monkeypatch, tmp_path: Path):
    captured: dict[str, Any] = {}
    report = tmp_path / "full-report-standalone.html"
    report.write_text("ok", encoding="utf-8")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "secret-token")
    monkeypatch.setenv("TELEGRAM_GROUP_ID", "-100123")
    monkeypatch.setenv("UZI_SKIP_BONUS_FETCHERS", "0")
    monkeypatch.setenv("UZI_STAGE2_SKIP_OPTIONAL_RENDERS", "0")

    bot = object.__new__(UziTelegramBot)
    bot.python_bin = Path("/usr/bin/python3")
    bot.analysis_depth = "medium"
    bot.analysis_timeout = 123
    bot.include_bonus_fetchers = False
    bot.render_extra_assets = False

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs.get("env")
        captured["timeout"] = kwargs.get("timeout")

        class Result:
            returncode = 0
            stdout = f"报告路径: {report}\n"
            stderr = ""

        return Result()

    monkeypatch.setattr("telegram_uzi_bot.subprocess.run", fake_run)

    path = bot.run_uzi("600519.SH")

    env: dict[str, str] = captured["env"]
    assert path == report
    assert captured["timeout"] == 123
    assert env["UZI_NO_UPDATE_CHECK"] == "1"
    assert env["UZI_SKIP_BONUS_FETCHERS"] == "1"
    assert env["UZI_STAGE2_SKIP_OPTIONAL_RENDERS"] == "1"
    assert "TELEGRAM_BOT_TOKEN" not in env
    assert "TELEGRAM_GROUP_ID" not in env


def test_run_uzi_allows_opt_in_full_extras(monkeypatch, tmp_path: Path):
    captured: dict[str, Any] = {}
    report = tmp_path / "full-report-standalone.html"
    report.write_text("ok", encoding="utf-8")
    monkeypatch.setenv("UZI_SKIP_BONUS_FETCHERS", "1")
    monkeypatch.setenv("UZI_STAGE2_SKIP_OPTIONAL_RENDERS", "1")

    bot = object.__new__(UziTelegramBot)
    bot.python_bin = Path("/usr/bin/python3")
    bot.analysis_depth = "medium"
    bot.analysis_timeout = 123
    bot.include_bonus_fetchers = True
    bot.render_extra_assets = True

    def fake_run(command, **kwargs):
        captured["env"] = kwargs.get("env")

        class Result:
            returncode = 0
            stdout = f"报告路径: {report}\n"
            stderr = ""

        return Result()

    monkeypatch.setattr("telegram_uzi_bot.subprocess.run", fake_run)

    bot.run_uzi("600519.SH")

    env: dict[str, str] = captured["env"]
    assert "UZI_SKIP_BONUS_FETCHERS" not in env
    assert "UZI_STAGE2_SKIP_OPTIONAL_RENDERS" not in env


def test_api_uses_poll_timeout_for_get_updates():
    captured: dict[str, object] = {}

    bot = object.__new__(UziTelegramBot)
    session = requests.Session()

    def fake_post(url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs.get("params")
        captured["json"] = kwargs.get("json")
        captured["timeout"] = kwargs.get("timeout")
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"ok": true, "result": []}'
        response.encoding = "utf-8"
        return response

    setattr(session, "post", fake_post)
    bot.session = session
    bot.base_url = "https://api.telegram.org/botTOKEN"
    bot.poll_timeout = 75

    result = bot._api("getUpdates", params={"timeout": 75})

    assert result == []
    assert captured["timeout"] == 85


def test_api_uses_default_timeout_for_non_polling_calls():
    captured: dict[str, object] = {}

    bot = object.__new__(UziTelegramBot)
    session = requests.Session()

    def fake_post(url, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"ok": true, "result": {"message_id": 1}}'
        response.encoding = "utf-8"
        return response

    setattr(session, "post", fake_post)
    bot.session = session
    bot.base_url = "https://api.telegram.org/botTOKEN"
    bot.poll_timeout = 75

    result = bot._api("sendMessage", json_body={"chat_id": 1, "text": "hello"})

    assert result == {"message_id": 1}
    assert captured["timeout"] == 60


def test_api_redacts_token_from_http_errors():
    token = "123456:SECRET"
    bot = object.__new__(UziTelegramBot)
    session = requests.Session()

    def fake_post(url, **kwargs):
        response = requests.Response()
        response.status_code = 401
        response.url = url
        response._content = b'{"ok": false, "description": "Unauthorized"}'
        response.encoding = "utf-8"
        return response

    setattr(session, "post", fake_post)
    bot.session = session
    bot.base_url = f"https://api.telegram.org/bot{token}"
    bot.token = token
    bot.poll_timeout = 75

    try:
        bot._api("sendMessage", json_body={"chat_id": 1, "text": "hello"})
    except RuntimeError as exc:
        error = str(exc)
    else:
        raise AssertionError("expected Telegram HTTP error")

    assert token not in error
    assert "<redacted>" in error


def test_api_redacts_token_from_api_payload_errors():
    token = "123456:SECRET"
    bot = object.__new__(UziTelegramBot)
    session = requests.Session()

    def fake_post(url, **kwargs):
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"ok": false, "description": "bad token 123456:SECRET"}'
        response.encoding = "utf-8"
        return response

    setattr(session, "post", fake_post)
    bot.session = session
    bot.base_url = f"https://api.telegram.org/bot{token}"
    bot.token = token
    bot.poll_timeout = 75

    try:
        bot._api("sendMessage", json_body={"chat_id": 1, "text": "hello"})
    except RuntimeError as exc:
        error = str(exc)
    else:
        raise AssertionError("expected Telegram API payload error")

    assert token not in error
    assert "<redacted>" in error


def test_worker_loop_redacts_token_from_failure_message():
    token = "123456:SECRET"
    bot = object.__new__(UziTelegramBot)
    bot.token = token
    stop_event = threading.Event()

    class StopAfterTaskQueue(queue.Queue[dict[str, Any]]):
        def task_done(self):
            super().task_done()
            stop_event.set()

    bot.jobs = StopAfterTaskQueue()
    bot.jobs.put(
        {
            "chat_id": 123,
            "message_id": 456,
            "message_thread_id": None,
            "query": "600519.SH",
        }
    )
    sent_messages: list[str] = []

    bot.stop_event = stop_event

    def failing_run_uzi(query: str):
        raise RuntimeError(f"analysis failed for {token}")

    def fake_safe_send_message(chat_id, text, **kwargs):
        sent_messages.append(text)

    setattr(bot, "run_uzi", failing_run_uzi)
    setattr(bot, "_safe_send_message", fake_safe_send_message)

    bot.worker_loop()

    failure_message = sent_messages[-1]
    assert token not in failure_message
    assert "<redacted>" in failure_message


def test_poll_loop_redacts_token_from_stderr(monkeypatch, capsys, tmp_path: Path):
    token = "123456:SECRET"
    bot = object.__new__(UziTelegramBot)
    bot.token = token
    bot.bot_username = "uzi_robot"
    bot.allowed_group_id = -100123
    bot.python_bin = Path("/usr/bin/python3")
    bot.can_read_all_group_messages = True
    bot.public_base_url = "https://unit-test.trycloudflare.com"
    bot.lease_store = LeaseStore(
        tmp_path / "leases.json", public_dir=tmp_path / "public", ttl_seconds=60
    )

    class FakeThread:
        def __init__(self, target=None, daemon=None):
            self.target = target
            self.daemon = daemon

        def start(self):
            return None

    bot.stop_event = threading.Event()

    setattr(bot, "delete_webhook", lambda: None)
    setattr(bot, "ensure_public_endpoint", lambda: None)
    setattr(bot, "cleanup_loop", lambda: None)
    setattr(bot, "worker_loop", lambda: None)

    def failing_poll_updates():
        bot.stop_event.set()
        raise RuntimeError(token)

    setattr(bot, "poll_updates", failing_poll_updates)
    setattr(bot, "shutdown", lambda: None)
    monkeypatch.setattr("telegram_uzi_bot.threading.Thread", FakeThread)

    bot.run()

    stderr = capsys.readouterr().err
    assert token not in stderr
    assert "<redacted>" in stderr


def test_ensure_public_endpoint_falls_back_to_ephemeral_port(
    monkeypatch, tmp_path: Path
):
    bot = object.__new__(UziTelegramBot)
    bot.httpd = None
    bot.tunnel_proc = None
    bot.public_base_url = None
    bot.public_dir = tmp_path
    bot.port = 8988

    created_ports: list[int] = []

    class FakeServer:
        def __init__(self, host: str, port: int):
            self.server_address = (host, 43123 if port == 0 else port)

        def serve_forever(self):
            return None

    def fake_server(address, handler):
        assert address[0] == "127.0.0.1"
        port = int(address[1])
        created_ports.append(port)
        if port == 8988:
            raise OSError(errno.EADDRINUSE, "Address already in use")
        return FakeServer(str(address[0]), port)

    started_threads: list[object] = []

    class FakeThread:
        def __init__(self, target=None, daemon=None):
            started_threads.append((target, daemon))

        def start(self):
            return None

    monkeypatch.setattr("telegram_uzi_bot.ThreadingHTTPServer", fake_server)
    monkeypatch.setattr("telegram_uzi_bot.threading.Thread", FakeThread)
    monkeypatch.setattr(bot, "ensure_cloudflared", lambda: Path("/tmp/cloudflared"))

    class FakeStream:
        def __init__(self):
            self._lines = iter(
                ["trycloudflare ready https://unit-test.trycloudflare.com\n"]
            )

        def readline(self):
            return next(self._lines, "")

    class FakeProc:
        def __init__(self):
            self.stderr = FakeStream()
            self.stdout = FakeStream()

        def poll(self):
            return None

    monkeypatch.setattr(
        "telegram_uzi_bot.subprocess.Popen", lambda *args, **kwargs: FakeProc()
    )

    bot.ensure_public_endpoint()

    assert created_ports == [8988, 0]
    assert bot.port == 43123
    assert bot.httpd is not None
    assert started_threads
    assert bot.public_base_url == "https://unit-test.trycloudflare.com"


def test_ensure_public_endpoint_shuts_down_local_server_on_tunnel_failure(
    monkeypatch, tmp_path: Path
):
    bot = object.__new__(UziTelegramBot)
    bot.httpd = None
    bot.tunnel_proc = None
    bot.public_base_url = None
    bot.public_dir = tmp_path
    bot.port = 8988

    class FakeServer:
        def __init__(self):
            self.server_address = ("127.0.0.1", 8988)
            self.shutdown_called = False
            self.server_close_called = False

        def serve_forever(self):
            return None

        def shutdown(self):
            self.shutdown_called = True

        def server_close(self):
            self.server_close_called = True

    fake_server_instance = FakeServer()

    class FakeThread:
        def __init__(self, target=None, daemon=None):
            self.target = target
            self.daemon = daemon

        def start(self):
            return None

    class FakeStream:
        def readline(self):
            return ""

    class FakeProc:
        def __init__(self):
            self.stderr = FakeStream()
            self.stdout = FakeStream()
            self.terminated = False
            self.killed = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout=None):
            return 0

        def kill(self):
            self.killed = True

    now_values = iter([0, 31])
    fake_proc = FakeProc()

    monkeypatch.setattr(
        "telegram_uzi_bot.ThreadingHTTPServer",
        lambda address, handler: fake_server_instance,
    )
    monkeypatch.setattr("telegram_uzi_bot.threading.Thread", FakeThread)
    monkeypatch.setattr(bot, "ensure_cloudflared", lambda: Path("/tmp/cloudflared"))
    monkeypatch.setattr(
        "telegram_uzi_bot.subprocess.Popen", lambda *args, **kwargs: fake_proc
    )
    monkeypatch.setattr("telegram_uzi_bot.now_ts", lambda: next(now_values))

    try:
        bot.ensure_public_endpoint()
    except RuntimeError as exc:
        assert "public URL" in str(exc)
    else:
        raise AssertionError("expected tunnel startup failure")

    assert fake_server_instance.shutdown_called is True
    assert fake_server_instance.server_close_called is True
    assert fake_proc.terminated is True
    assert fake_proc.killed is False
    assert bot.httpd is None
    assert bot.tunnel_proc is None
