from __future__ import annotations

import json
import errno
import time
from pathlib import Path

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
        def __init__(self, port: int):
            self.server_address = ("0.0.0.0", 43123 if port == 0 else port)

        def serve_forever(self):
            return None

    def fake_server(address, handler):
        port = int(address[1])
        created_ports.append(port)
        if port == 8988:
            raise OSError(errno.EADDRINUSE, "Address already in use")
        return FakeServer(port)

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
