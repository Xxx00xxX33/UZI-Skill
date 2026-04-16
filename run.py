#!/usr/bin/env python3
"""UZI-Skill 一键运行入口 — 适用于 Claude Code / Codex / Cursor / 命令行 / 任何 agent。

用法:
    python run.py 002273.SZ                   # 本地分析，浏览器打开
    python run.py 600519.SH --remote          # 分析完用 Cloudflare Tunnel 映射公网
    python run.py AAPL --no-browser           # 不打开浏览器（Codex/CI 环境）
    python run.py 贵州茅台 --remote            # 中文名 + 远程查看

参数:
    第一个参数: 股票代码或中文名
    --remote     分析完后启动 HTTP 服务 + Cloudflare Tunnel，生成公网链接
    --no-browser 不自动打开浏览器（适合无 GUI 的服务器/Codex 环境）
    --port PORT  HTTP 服务端口（默认 8976）

运行完会输出:
    1. HTML 报告本地路径
    2. 如果 --remote: 一个 https://xxx.trycloudflare.com 公网链接
"""
import os
import sys
import argparse
import subprocess
import shutil
import threading
import time
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler

# ─── 编码修复 ───
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ─── 路径设置 ───
ROOT_DIR = Path(__file__).parent.resolve()
SCRIPTS_DIR = ROOT_DIR / "skills" / "deep-analysis" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))
os.chdir(str(SCRIPTS_DIR))


def detect_environment() -> dict:
    """检测当前运行环境。"""
    env = {
        "has_browser": True,
        "has_cloudflared": shutil.which("cloudflared") is not None,
        "is_codex": os.environ.get("CODEX") == "1" or os.environ.get("OPENAI_API_KEY") is not None,
        "is_ci": os.environ.get("CI") is not None,
        "is_docker": Path("/.dockerenv").exists(),
        "is_ssh": "SSH_CONNECTION" in os.environ,
        "platform": sys.platform,
    }
    # 无 GUI 环境自动 no-browser
    if env["is_codex"] or env["is_ci"] or env["is_docker"] or env["is_ssh"]:
        env["has_browser"] = False
    # Linux 无 DISPLAY 也不开浏览器
    if sys.platform == "linux" and not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        env["has_browser"] = False
    return env


def check_dependencies():
    """检查并安装缺失依赖。"""
    required = ["akshare", "requests"]
    missing = []
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"⚠️  缺少依赖: {', '.join(missing)}")
        print(f"   正在自动安装...")
        req_file = ROOT_DIR / "requirements.txt"
        if req_file.exists():
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(req_file), "-q"],
                           check=False)
        else:
            subprocess.run([sys.executable, "-m", "pip", "install"] + missing + ["-q"],
                           check=False)
        print("   ✓ 依赖安装完成\n")


def serve_report(report_path: Path, port: int = 8976) -> HTTPServer:
    """启动 HTTP 服务器托管报告目录。"""
    report_dir = report_path.parent
    os.chdir(str(report_dir))

    handler = SimpleHTTPRequestHandler
    httpd = HTTPServer(("0.0.0.0", port), handler)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    filename = report_path.name
    print(f"\n📡 本地 HTTP 服务已启动:")
    print(f"   http://localhost:{port}/{filename}")
    return httpd


def start_cloudflare_tunnel(port: int = 8976):
    """启动 Cloudflare Tunnel，返回公网 URL。"""
    if not shutil.which("cloudflared"):
        print("\n⚠️  未检测到 cloudflared，正在尝试安装...")
        if sys.platform == "win32":
            print("   请手动安装: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/")
            print("   或: winget install Cloudflare.cloudflared")
            return None
        elif sys.platform == "darwin":
            subprocess.run(["brew", "install", "cloudflared"], check=False)
        else:
            # Linux
            subprocess.run(["bash", "-c",
                            "curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /tmp/cloudflared && chmod +x /tmp/cloudflared && sudo mv /tmp/cloudflared /usr/local/bin/"],
                           check=False)

        if not shutil.which("cloudflared"):
            print("   ❌ cloudflared 安装失败，跳过远程映射")
            return None
        print("   ✓ cloudflared 安装成功")

    print(f"\n🌐 正在启动 Cloudflare Tunnel (端口 {port})...")

    proc = subprocess.Popen(
        ["cloudflared", "tunnel", "--url", f"http://localhost:{port}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # 从 stderr 里抓公网 URL（cloudflared 输出在 stderr）
    public_url = None
    start_time = time.time()
    while time.time() - start_time < 30:
        line = proc.stderr.readline()
        if not line:
            time.sleep(0.1)
            continue
        if "trycloudflare.com" in line or "cfargotunnel.com" in line:
            import re
            match = re.search(r"(https://[a-zA-Z0-9\-]+\.trycloudflare\.com)", line)
            if match:
                public_url = match.group(1)
                break

    if public_url:
        print(f"   ✅ 公网地址: {public_url}")
        print(f"   📱 手机扫码或发送链接即可查看报告")
        print(f"   ⏹  按 Ctrl+C 停止服务")
    else:
        print(f"   ⚠️  Tunnel 启动中... 请检查 cloudflared 输出")

    return public_url


def main():
    parser = argparse.ArgumentParser(
        description="游资（UZI）Skills · 个股深度分析",
        epilog="示例: python run.py 贵州茅台 --remote",
    )
    parser.add_argument("ticker", nargs="?", default="002273.SZ",
                        help="股票代码或中文名 (如 600519.SH / AAPL / 贵州茅台)")
    parser.add_argument("--remote", action="store_true",
                        help="分析完后用 Cloudflare Tunnel 映射公网链接")
    parser.add_argument("--no-browser", action="store_true",
                        help="不自动打开浏览器")
    parser.add_argument("--port", type=int, default=8976,
                        help="HTTP 服务端口 (默认 8976)")
    args = parser.parse_args()

    env = detect_environment()

    print()
    print("━" * 50)
    print("🎯 游资（UZI）Skills v2.2 · 深度分析引擎")
    print(f"   目标: {args.ticker}")
    print(f"   环境: {'Codex' if env['is_codex'] else 'Docker' if env['is_docker'] else 'SSH' if env['is_ssh'] else '本地'}")
    print(f"   浏览器: {'✓' if env['has_browser'] and not args.no_browser else '✗ (headless)'}")
    print(f"   Cloudflare: {'✓ 已安装' if env['has_cloudflared'] else '✗ 未安装'}")
    if args.remote:
        print(f"   远程模式: ✓ (完成后映射公网)")
    print("━" * 50)
    print()

    # 检查依赖
    check_dependencies()

    # 运行分析（抑制 run_real_test 内部的自动开浏览器）
    os.environ["UZI_NO_AUTO_OPEN"] = "1"
    from run_real_test import main as run_analysis
    run_analysis(args.ticker)

    # 找到生成的报告
    from datetime import datetime
    from lib.market_router import parse_ticker
    ti = parse_ticker(args.ticker)
    date = datetime.now().strftime("%Y%m%d")
    report_dir = SCRIPTS_DIR / "reports" / f"{ti.full}_{date}"
    standalone = report_dir / "full-report-standalone.html"

    if not standalone.exists():
        # 尝试找最新的报告
        reports_root = SCRIPTS_DIR / "reports"
        if reports_root.exists():
            dirs = sorted(reports_root.glob(f"{ti.full}_*"), reverse=True)
            for d in dirs:
                candidate = d / "full-report-standalone.html"
                if candidate.exists():
                    standalone = candidate
                    report_dir = d
                    break

    if not standalone.exists():
        print(f"\n❌ 报告文件未找到: {standalone}")
        return

    print(f"\n{'━' * 50}")
    print(f"📄 报告路径: {standalone}")
    print(f"   大小: {standalone.stat().st_size // 1024} KB")

    # 打开浏览器（本地模式）
    if env["has_browser"] and not args.no_browser and not args.remote:
        import webbrowser
        webbrowser.open(standalone.as_uri())
        print(f"   🌐 已在浏览器中打开")

    # 远程模式: HTTP server + Cloudflare Tunnel
    if args.remote:
        httpd = serve_report(standalone, args.port)
        filename = standalone.name
        public_url = start_cloudflare_tunnel(args.port)

        if public_url:
            full_url = f"{public_url}/{filename}"
            print(f"\n{'━' * 50}")
            print(f"📱 远程查看地址:")
            print(f"   {full_url}")
            print(f"{'━' * 50}")
            print(f"\n发送这个链接到手机就能看报告。")
            print(f"按 Ctrl+C 停止服务。\n")

            # 如果有浏览器也打开
            if env["has_browser"] and not args.no_browser:
                import webbrowser
                webbrowser.open(full_url)

            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n⏹  服务已停止")
                httpd.shutdown()
        else:
            # cloudflared 失败，至少提供本地 HTTP
            print(f"\n   本地访问: http://localhost:{args.port}/{filename}")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n⏹  服务已停止")
                httpd.shutdown()
    elif not env["has_browser"] or args.no_browser:
        # 无浏览器环境，提示用户
        print(f"\n💡 提示: 当前环境无法打开浏览器")
        print(f"   方式 1: 下载文件到本地打开")
        print(f"   方式 2: python run.py {args.ticker} --remote  ← 生成公网链接，手机就能看")

    print(f"{'━' * 50}")
    print(f"✅ 完成!")


if __name__ == "__main__":
    main()
