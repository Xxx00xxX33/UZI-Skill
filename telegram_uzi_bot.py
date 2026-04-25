#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from typing import Any

import requests


ROOT_DIR = Path(__file__).parent.resolve()
PUBLIC_DIR = ROOT_DIR / ".telegram-public-reports"
LEASES_PATH = ROOT_DIR / ".telegram-report-leases.json"
OFFSET_PATH = ROOT_DIR / ".telegram-bot-offset.json"
DEFAULT_VENV_PYTHON = ROOT_DIR / ".venv" / "bin" / "python"
CLOUDFLARED_DOWNLOAD_URL = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/"
    "cloudflared-linux-amd64"
)
DEFAULT_PORT = 8988
DEFAULT_TTL_SECONDS = 24 * 3600
DEFAULT_POLL_TIMEOUT = 30
DEFAULT_ANALYSIS_TIMEOUT = 45 * 60
DEFAULT_DEPTH = "medium"
DEFAULT_API_TIMEOUT = 60

REPORT_PATH_LINE_RE = re.compile(r"^(?:\s*📄\s*)?报告路径[:：]\s*(.+?)\s*$")
REPORT_PATH_FALLBACK_RE = re.compile(
    r"(?P<path>(?:[A-Za-z]:[\\/]|/)[^\n\r]*full-report-standalone\.html)"
)


def load_dotenv() -> None:
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = val


def now_ts() -> float:
    return time.time()


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def redact_secret(text: str, secret: str) -> str:
    if not secret:
        return text
    return text.replace(secret, "<redacted>")


def parse_group_id(raw: str) -> int:
    return int(raw.strip())


def parse_stock_request(text: str, bot_username: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        return ""

    mention = f"@{bot_username.lower()}"
    cleaned = re.sub(re.escape(mention), " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"/analyze(?:@\w+)?", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""

    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    return lines[0] if lines else cleaned


def bot_was_mentioned(message: dict[str, Any], bot_username: str) -> bool:
    bot_username = bot_username.lower()
    text = message.get("text") or message.get("caption") or ""
    entities = message.get("entities") or message.get("caption_entities") or []
    for entity in entities:
        if entity.get("type") == "mention":
            start = entity.get("offset", 0)
            end = start + entity.get("length", 0)
            if text[start:end].lower() == f"@{bot_username}":
                return True
        if entity.get("type") == "bot_command":
            start = entity.get("offset", 0)
            end = start + entity.get("length", 0)
            command = text[start:end].lower()
            if command in {"/analyze", f"/analyze@{bot_username}"}:
                return True
    return f"@{bot_username}" in text.lower()


def extract_request(message: dict[str, Any], bot_username: str) -> str:
    if not bot_was_mentioned(message, bot_username):
        return ""
    text = message.get("text") or message.get("caption") or ""
    return parse_stock_request(text, bot_username)


@dataclass
class Lease:
    filename: str
    expires_at: float
    source_report: str


class LeaseStore:
    def __init__(self, path: Path, public_dir: Path, ttl_seconds: int):
        self.path = path
        self.public_dir = public_dir
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._data: dict[str, Lease] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.public_dir.mkdir(parents=True, exist_ok=True)
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        for key, value in payload.items():
            if not isinstance(value, dict):
                continue
            filename = str(value.get("filename") or "")
            expires_at = float(value.get("expires_at") or 0)
            source_report = str(value.get("source_report") or "")
            if filename and expires_at > 0:
                self._data[key] = Lease(
                    filename=filename,
                    expires_at=expires_at,
                    source_report=source_report,
                )

    def _persist(self) -> None:
        payload = {
            key: {
                "filename": lease.filename,
                "expires_at": lease.expires_at,
                "source_report": lease.source_report,
            }
            for key, lease in self._data.items()
        }
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def create(self, report_path: Path) -> Lease:
        token = uuid.uuid4().hex
        filename = f"{token}.html"
        target = self.public_dir / filename
        shutil.copy2(report_path, target)
        lease = Lease(
            filename=filename,
            expires_at=now_ts() + self.ttl_seconds,
            source_report=str(report_path),
        )
        with self._lock:
            self._data[token] = lease
            self._persist()
        return lease

    def cleanup(self) -> None:
        current = now_ts()
        removed = []
        with self._lock:
            for key, lease in list(self._data.items()):
                if lease.expires_at <= current:
                    target = self.public_dir / lease.filename
                    if target.exists():
                        target.unlink()
                    removed.append(key)
            for key in removed:
                self._data.pop(key, None)
            if removed:
                self._persist()


class PublicReportHandler(SimpleHTTPRequestHandler):
    def list_directory(self, path: Any) -> BytesIO | None:
        self.send_error(404, "Not Found")
        return None


class UziTelegramBot:
    def __init__(self) -> None:
        load_dotenv()
        self.token = os.environ["TELEGRAM_BOT_TOKEN"]
        self.allowed_group_id = parse_group_id(os.environ["TELEGRAM_GROUP_ID"])
        self.port = int(os.environ.get("UZI_TELEGRAM_PORT", DEFAULT_PORT))
        self.ttl_seconds = int(
            float(os.environ.get("UZI_TELEGRAM_LINK_TTL_HOURS", "24")) * 3600
        )
        self.poll_timeout = int(
            os.environ.get("UZI_TELEGRAM_POLL_TIMEOUT", DEFAULT_POLL_TIMEOUT)
        )
        self.analysis_timeout = int(
            os.environ.get("UZI_TELEGRAM_ANALYSIS_TIMEOUT", DEFAULT_ANALYSIS_TIMEOUT)
        )
        self.analysis_depth = os.environ.get("UZI_TELEGRAM_DEPTH", DEFAULT_DEPTH)
        self.include_bonus_fetchers = env_flag(
            "UZI_TELEGRAM_INCLUDE_BONUS_FETCHERS", default=False
        )
        self.render_extra_assets = env_flag(
            "UZI_TELEGRAM_RENDER_EXTRA_ASSETS", default=False
        )
        configured_python = os.environ.get("UZI_TELEGRAM_PYTHON", "").strip()
        self.python_bin = self._choose_python_bin(configured_python)
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.session = self._build_session()
        self.public_dir = PUBLIC_DIR
        self.public_dir.mkdir(parents=True, exist_ok=True)
        self.lease_store = LeaseStore(
            LEASES_PATH,
            public_dir=self.public_dir,
            ttl_seconds=self.ttl_seconds,
        )
        self.httpd: ThreadingHTTPServer | None = None
        self.tunnel_proc: subprocess.Popen[str] | None = None
        self.public_base_url: str | None = None
        self.stop_event = threading.Event()
        self.jobs: queue.Queue[dict[str, Any]] = queue.Queue()
        self.offset = self._load_offset()
        self.me = self._get_me()
        self.bot_username = str(self.me["username"])
        self.can_read_all_group_messages = bool(
            self.me.get("can_read_all_group_messages", False)
        )

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        session.trust_env = env_flag("UZI_TELEGRAM_USE_ENV_PROXY", default=False)
        session.headers.update({"User-Agent": "uzi-telegram-bot/1.0"})
        return session

    @staticmethod
    def _choose_python_bin(configured_python: str) -> Path:
        if configured_python:
            return Path(configured_python).expanduser()
        if DEFAULT_VENV_PYTHON.exists():
            return DEFAULT_VENV_PYTHON
        return Path(sys.executable).resolve()

    @staticmethod
    def _cloudflared_install_targets() -> list[Path]:
        configured = os.environ.get("UZI_TELEGRAM_CLOUDFLARED", "").strip()
        targets: list[Path] = []
        if configured:
            targets.append(Path(configured).expanduser())
        targets.append(ROOT_DIR / ".bin" / "cloudflared")
        home = Path.home()
        if str(home) not in {"", "/"}:
            targets.append(home / ".local" / "bin" / "cloudflared")

        unique_targets: list[Path] = []
        seen: set[str] = set()
        for target in targets:
            key = str(target)
            if key in seen:
                continue
            seen.add(key)
            unique_targets.append(target)
        return unique_targets

    def _load_offset(self) -> int | None:
        if not OFFSET_PATH.exists():
            return None
        try:
            payload = json.loads(OFFSET_PATH.read_text(encoding="utf-8"))
        except Exception:
            return None
        value = payload.get("offset")
        return int(value) if isinstance(value, int) else None

    def _save_offset(self, offset: int) -> None:
        OFFSET_PATH.write_text(
            json.dumps({"offset": offset}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _api(
        self,
        method: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        timeout = (
            max(DEFAULT_API_TIMEOUT, self.poll_timeout + 10)
            if method == "getUpdates"
            else DEFAULT_API_TIMEOUT
        )
        try:
            response = self.session.post(
                f"{self.base_url}/{method}",
                params=params,
                json=json_body,
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            safe_error = self._redact(str(exc))
            raise RuntimeError(
                f"Telegram API {method} HTTP error: {safe_error}"
            ) from exc
        payload = response.json()
        if not payload.get("ok"):
            safe_payload = self._redact(json.dumps(payload, ensure_ascii=False))
            raise RuntimeError(f"Telegram API {method} failed: {safe_payload}")
        return payload["result"]

    def _redact(self, text: str) -> str:
        return redact_secret(text, getattr(self, "token", ""))

    def _get_me(self) -> dict[str, Any]:
        return self._api("getMe")

    def delete_webhook(self) -> None:
        self._api("deleteWebhook", json_body={"drop_pending_updates": False})

    def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        message_thread_id: int | None = None,
    ) -> None:
        body: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": False,
        }
        if reply_to_message_id is not None:
            body["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        if message_thread_id is not None:
            body["message_thread_id"] = message_thread_id
        self._api("sendMessage", json_body=body)

    def _safe_send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        message_thread_id: int | None = None,
    ) -> None:
        try:
            self.send_message(
                chat_id,
                text,
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id,
            )
        except Exception as exc:
            print(
                f"send_message failed for chat {chat_id}: {type(exc).__name__}: {self._redact(str(exc))}",
                file=sys.stderr,
                flush=True,
            )

    def _download_cloudflared(self, target: Path) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
        response = requests.get(CLOUDFLARED_DOWNLOAD_URL, timeout=DEFAULT_API_TIMEOUT)
        try:
            response.raise_for_status()
            temp_path.write_bytes(response.content)
            temp_path.chmod(0o755)
            temp_path.replace(target)
        finally:
            response.close()
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        return target

    def ensure_cloudflared(self) -> Path:
        existing = shutil.which("cloudflared")
        if existing:
            return Path(existing)

        for candidate in self._cloudflared_install_targets():
            if candidate.exists() and os.access(candidate, os.X_OK):
                return candidate

        last_error: Exception | None = None
        for candidate in self._cloudflared_install_targets():
            try:
                return self._download_cloudflared(candidate)
            except Exception as exc:
                last_error = exc

        detail = f": {last_error}" if last_error else ""
        raise RuntimeError(f"cloudflared installation failed{detail}")

    def ensure_public_endpoint(self) -> None:
        if self.httpd is None:
            handler = partial(PublicReportHandler, directory=str(self.public_dir))
            try:
                self.httpd = ThreadingHTTPServer(("127.0.0.1", self.port), handler)
            except OSError as exc:
                if exc.errno != 98 or self.port == 0:
                    raise
                self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            self.port = int(self.httpd.server_address[1])
            thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            thread.start()

        if (
            self.tunnel_proc is not None
            and self.tunnel_proc.poll() is None
            and self.public_base_url
        ):
            return

        cloudflared_bin = self.ensure_cloudflared()
        self.tunnel_proc = subprocess.Popen(
            [str(cloudflared_bin), "tunnel", "--url", f"http://localhost:{self.port}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = now_ts() + 30
        public_url = None
        while now_ts() < deadline:
            if self.tunnel_proc.stderr is None:
                break
            line = self.tunnel_proc.stderr.readline()
            if not line:
                time.sleep(0.1)
                continue
            match = re.search(r"(https://[a-zA-Z0-9\-]+\.trycloudflare\.com)", line)
            if match:
                public_url = match.group(1)
                break
        if not public_url:
            if self.tunnel_proc is not None and self.tunnel_proc.poll() is None:
                self.tunnel_proc.terminate()
                try:
                    self.tunnel_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.tunnel_proc.kill()
            self.tunnel_proc = None
            if self.httpd is not None:
                self.httpd.shutdown()
                self.httpd.server_close()
                self.httpd = None
            raise RuntimeError("cloudflared tunnel did not return a public URL")
        self.public_base_url = public_url

    def build_public_link(self, report_path: Path) -> str:
        self.ensure_public_endpoint()
        lease = self.lease_store.create(report_path)
        return f"{self.public_base_url}/{lease.filename}"

    def locate_report_from_output(self, stdout: str) -> Path:
        report_path = None
        for line in stdout.splitlines():
            match = REPORT_PATH_LINE_RE.match(line.strip())
            if match:
                report_path = match.group(1).strip()
        if not report_path:
            fallback_match = REPORT_PATH_FALLBACK_RE.search(stdout)
            if fallback_match:
                report_path = fallback_match.group("path").strip()
        if not report_path:
            raise RuntimeError("report path not found in UZI output")
        path = Path(report_path)
        if not path.exists():
            raise RuntimeError(f"report file missing: {path}")
        return path

    def run_uzi(self, query: str) -> Path:
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("TELEGRAM_"):
                env.pop(key, None)
        env["UZI_NO_UPDATE_CHECK"] = "1"
        if not self.include_bonus_fetchers:
            env["UZI_SKIP_BONUS_FETCHERS"] = "1"
        else:
            env.pop("UZI_SKIP_BONUS_FETCHERS", None)
        if not self.render_extra_assets:
            env["UZI_STAGE2_SKIP_OPTIONAL_RENDERS"] = "1"
        else:
            env.pop("UZI_STAGE2_SKIP_OPTIONAL_RENDERS", None)
        command = [
            str(self.python_bin),
            str(ROOT_DIR / "run.py"),
            query,
            "--no-browser",
            "--depth",
            self.analysis_depth,
        ]
        result = subprocess.run(
            command,
            cwd=ROOT_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=self.analysis_timeout,
        )
        if result.returncode != 0:
            summary = (result.stdout or "") + "\n" + (result.stderr or "")
            raise RuntimeError(
                summary.strip()[-1200:] or f"UZI exited with {result.returncode}"
            )
        return self.locate_report_from_output(result.stdout)

    def enqueue_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        if int(chat.get("id", 0)) != self.allowed_group_id:
            return
        query = extract_request(message, self.bot_username)
        if not query:
            return
        self.jobs.put(
            {
                "chat_id": int(chat["id"]),
                "message_id": int(message["message_id"]),
                "message_thread_id": message.get("message_thread_id"),
                "query": query,
                "from_name": (message.get("from") or {}).get("first_name") or "用户",
            }
        )

    def poll_updates(self) -> None:
        params: dict[str, Any] = {
            "timeout": self.poll_timeout,
            "allowed_updates": json.dumps(["message"]),
        }
        if self.offset is not None:
            params["offset"] = self.offset
        updates = self._api("getUpdates", params=params)
        for update in updates:
            update_id = int(update["update_id"])
            self.enqueue_update(update)
            self.offset = update_id + 1
            self._save_offset(self.offset)

    def worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                job = self.jobs.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._safe_send_message(
                    job["chat_id"],
                    f"已收到 {job['query']}，正在生成 UZI 报告，通常需要 5-20 分钟。",
                    reply_to_message_id=job["message_id"],
                    message_thread_id=job.get("message_thread_id"),
                )
                report_path = self.run_uzi(job["query"])
                link = self.build_public_link(report_path)
                ttl_hours = max(1, self.ttl_seconds // 3600)
                self._safe_send_message(
                    job["chat_id"],
                    f"{job['query']} 的 UZI 报告已生成：\n{link}\n\n链接有效期约 {ttl_hours} 小时。",
                    reply_to_message_id=job["message_id"],
                    message_thread_id=job.get("message_thread_id"),
                )
            except Exception as exc:
                safe_error = self._redact(str(exc))
                self._safe_send_message(
                    job["chat_id"],
                    f"生成 {job['query']} 报告失败：{type(exc).__name__}: {safe_error[:700]}",
                    reply_to_message_id=job["message_id"],
                    message_thread_id=job.get("message_thread_id"),
                )
            finally:
                self.jobs.task_done()

    def cleanup_loop(self) -> None:
        while not self.stop_event.is_set():
            self.lease_store.cleanup()
            self.stop_event.wait(60)

    def shutdown(self) -> None:
        self.stop_event.set()
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
        if self.tunnel_proc is not None and self.tunnel_proc.poll() is None:
            self.tunnel_proc.terminate()
            try:
                self.tunnel_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.tunnel_proc.kill()

    def run(self) -> None:
        self.delete_webhook()
        self.ensure_public_endpoint()
        cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
        worker_thread = threading.Thread(target=self.worker_loop, daemon=True)
        cleanup_thread.start()
        worker_thread.start()
        print(
            f"Telegram bot @{self.bot_username} is listening on group {self.allowed_group_id}"
        )
        print(f"Python interpreter: {self.python_bin}")
        print(f"Bot can_read_all_group_messages: {self.can_read_all_group_messages}")
        if not self.can_read_all_group_messages:
            print(
                "Warning: Telegram may not deliver every plain group mention unless privacy/admin settings allow it. "
                f"Using /analyze@{self.bot_username} <ticker> is the safest fallback."
            )
        print(f"Public base URL: {self.public_base_url}")
        try:
            while not self.stop_event.is_set():
                try:
                    self.poll_updates()
                    self.lease_store.cleanup()
                except Exception as exc:
                    safe_error = self._redact(str(exc))
                    print(
                        f"poll_updates failed: {type(exc).__name__}: {safe_error}",
                        file=sys.stderr,
                        flush=True,
                    )
                    if self.stop_event.wait(5):
                        break
        finally:
            self.shutdown()


def main() -> int:
    bot = UziTelegramBot()

    def _handle_signal(signum: int, frame: Any) -> None:
        bot.shutdown()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    bot.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
