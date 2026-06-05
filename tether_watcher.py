#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Watcher — 事件驱动消息处理器
2 秒轮询 /tmp/tether_notify.json 的修改时间，
发现新消息就走 Gateway API 处理，Gateway 不可用时回退到 hermes -z。

独立于 tether_server.py 运行，没有耦合。

同时监控 /tmp/tether_handoff.json，发现 handoff 消息就通过 hermes -z
子进程处理（handoff 需要完整的 agent session 来执行工具和回复）。
"""
import json, os, subprocess, threading, time, urllib.request

# 设置本机请求绕过 HTTP 代理（MacBook 上可能配了 Clash 环境变量）
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

NOTIFY_FILE = "/tmp/tether_notify.json"
HANDOFF_FILE = "/tmp/tether_handoff.json"
TETHER_URL = "http://127.0.0.1:9001"
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8642")
GATEWAY_SESSION = "tether-watcher"
GATEWAY_API_KEY = ""
HANDOFF_RESULT_FILE = "/tmp/tether_handoff_result.json"
POLL_INTERVAL = 2

# 从环境变量或 ~/.hermes/.env 读取 Gateway API Key
API_KEY_ENV = os.environ.get("API_SERVER_KEY", "")
if API_KEY_ENV:
    GATEWAY_API_KEY = API_KEY_ENV.strip()
else:
    _env_path = os.path.expanduser("~/.hermes/.env")
    if os.path.isfile(_env_path):
        try:
            with open(_env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("API_SERVER_KEY="):
                        GATEWAY_API_KEY = line.split("=", 1)[1].strip()
                        break
        except Exception:
            pass

# 自愈相关
_last_restart_time = 0.0
_SELF_HEAL_INTERVAL = 15  # 每15秒检查一次 tether 健康


def log(msg):
    print(f"[watcher] {msg}", flush=True)


def _tether_healthy():
    """检查本地 Tether 服务是否健康（HTTP GET /health → 200）"""
    try:
        req = urllib.request.Request(f"{TETHER_URL}/health")
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status == 200
    except Exception:
        return False


GATEWAY_PORT = 8642
_last_restart_time = 0  # 重启防抖计时（同时用于 Gateway 和 Tether 的自愈）


def _is_gateway_alive():
    """快速探测 Gateway 是否存活（HTTP /health 返回 200 才算活）"""
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{GATEWAY_PORT}/health")
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status == 200
    except Exception:
        return False


def _ensure_gateway_alive():
    """如果 Gateway 挂了，尝试自动重启 hermes-gateway.service

    包含重启防抖：连续两次重启间隔至少 30 秒，防止无限重启循环。
    重启后等待 5 秒再检查，若未就绪再等两轮（最长 ~15 秒）。
    """
    global _last_restart_time

    if _is_gateway_alive():
        return True

    now = time.time()
    if now - _last_restart_time < 30:
        log(f"⏳ Gateway 不可达但距上次重启仅 {now - _last_restart_time:.0f}s，跳过本次")
        return False

    log(f"⚠️ Gateway 不可达，尝试重启 hermes-gateway.service…")
    _last_restart_time = now
    try:
        r = subprocess.run(
            ["systemctl", "--user", "restart", "hermes-gateway"],
            capture_output=True, text=True, timeout=15,
        )
        if r.returncode != 0:
            log(f"❌ systemctl restart 失败: {r.stderr.strip()[:80]}")
            return False

        time.sleep(5)
        if _is_gateway_alive():
            log("✅ Gateway 已恢复（重启后 5 秒）")
            return True

        for i in range(2):
            time.sleep(5)
            if _is_gateway_alive():
                log(f"✅ Gateway 已恢复（重启后 {5 + 5*(i+1)} 秒）")
                return True

        log("❌ Gateway 重启后仍未响应，继续走 hermes -z 保底")
        return False
    except Exception as e:
        log(f"❌ Gateway 重启异常: {str(e)[:80]}")
        return False


def _restart_tether():
    """重启本地 tether.service，带防抖（<30s 跳过）"""
    global _last_restart_time
    now = time.time()
    if now - _last_restart_time < 30:
        log(f"⏭️ 重启跳过：距上次重启 {now - _last_restart_time:.0f}s < 30s")
        return False
    _last_restart_time = now
    log("🔄 尝试重启 tether.service...")
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", "tether.service"],
            capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        log(f"❌ 重启命令失败: {e}")
        return False

    for attempt in range(3):
        if attempt > 0:
            time.sleep(5)
        if _tether_healthy():
            log(f"✅ 重启成功（第{attempt+1}次检查后）")
            return True
        log(f"⏳ 等待 tether 恢复（第{attempt+1}次检查，未就绪）")
    log("❌ 重启确认失败，tether 仍不可用")
    return False


def _self_heal():
    """自愈巡检：Gateway 健康检查 + 自动重启"""
    _ensure_gateway_alive()
    if not _tether_healthy():
        log("⚠️ Tether 不可达（由 systemd Restart=always 负责恢复）")


def _tether_get(path):
    try:
        req = urllib.request.Request(f"{TETHER_URL}{path}")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read()), None
    except Exception as e:
        return None, str(e)


def _gateway_headers():
    h = {"Content-Type": "application/json"}
    if GATEWAY_API_KEY:
        h["Authorization"] = f"Bearer {GATEWAY_API_KEY}"
    return h


def _gateway_chat(message, timeout=300):
    """通过 Gateway API 处理消息，返回 (output, error)"""
    url = f"{GATEWAY_URL}/api/sessions/{GATEWAY_SESSION}/chat"
    headers = _gateway_headers()
    payload = json.dumps({"message": message}).encode()

    for attempt in range(2):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode())
            content = result.get("message", {}).get("content", "")
            return content.strip(), None
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            err = str(e)
            if attempt == 0:
                log(f"Gateway 请求失败 (重试): {err[:80]}")
                time.sleep(2)
                continue
            return None, err
        except Exception as e:
            return None, str(e)
    return None, "Gateway 失败（重试耗尽）"


def _ensure_gateway_session():
    """确保 Gateway session 存在"""
    url = f"{GATEWAY_URL}/api/sessions"
    headers = _gateway_headers()
    payload = json.dumps({"title": GATEWAY_SESSION, "id": GATEWAY_SESSION}).encode()
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        urllib.request.urlopen(req, timeout=10)
        log("Gateway session 已创建")
    except urllib.error.HTTPError as e:
        if e.code != 409:
            log(f"Gateway session 创建失败: HTTP {e.code}")
    except Exception as e:
        log(f"Gateway 不可达: {str(e)[:60]}")


def _find_hermes():
    for c in [os.path.expanduser("~/.hermes/hermes-agent/venv/bin/hermes"), "hermes"]:
        if c == "hermes":
            try:
                subprocess.run(["which", "hermes"], capture_output=True, check=True)
                return "hermes"
            except Exception:
                continue
        elif os.path.isfile(c):
            return c
    return None


def process_messages():
    # 处理消息前先确保 Gateway 存活
    _ensure_gateway_alive()

    data, err = _tether_get("/messages?ack=1")
    if err or not data:
        log(f"取消息失败: {err}")
        return

    msgs = data.get("messages", [])
    if not msgs:
        return

    log(f"📬 {len(msgs)} 条新消息")
    for msg in msgs:
        mid = msg.get("id", "?")[:8]
        sender = msg.get("sender", "unknown")
        content = msg.get("message", "")
        log(f"▶ 处理 {mid} from={sender}: {content[:80]}")

        prompt = (
            f"[Tether Agent-to-Agent] 来自 {sender}：\n{content}\n\n"
            f"这是另一台 Hermes agent 通过 Tether 发来的消息。\n"
            f"1) 如果需要执行任务，使用 terminal 等工具完成\n"
            f"2) 如需回复对方，使用 curl POST 到对方 Tether 的 /message 端点\n"
            f"3) 你的回复输出会被记录但不会自动转发"
        )

        processed = False

        # 优先走 Gateway
        output, err = _gateway_chat(prompt, timeout=300)
        if err is None and output is not None:
            has_out = bool(output)
            log(f"✅ {mid} 处理完成 (Gateway, {len(output) if has_out else 0} chars)")
            processed = True
        else:
            log(f"Gateway 失败 ({err or 'no output'}), 回退子进程")

        # Gateway 失败则走子进程
        if not processed:
            hermes = _find_hermes()
            if hermes:
                try:
                    r = subprocess.run(
                        [hermes, "-z", prompt],
                        capture_output=True, text=True, timeout=300
                    )
                    if r.returncode == 0 and r.stdout.strip():
                        log(f"✅ {mid} 处理完成 (子进程, {len(r.stdout.strip())} chars)")
                    else:
                        log(f"⚠️ {mid} 处理完成但无输出")
                except subprocess.TimeoutExpired:
                    log(f"⏰ {mid} 超时 (300s)")
            else:
                log("❌ 找不到 hermes CLI")

    log("✅ 本轮处理完成")


def _write_handoff_result(sender, summary, output):
    """将 handoff 处理结果写入临时文件，供主 session 检测"""
    try:
        now = __import__("datetime").datetime.now().isoformat()
        with open(HANDOFF_RESULT_FILE, "w") as f:
            json.dump({"sender": sender, "summary": summary,
                       "output": output, "time": now}, f)
    except Exception:
        pass


def process_handoffs():
    """检查 handoff 文件，有内容就通过 hermes -z 处理（需要工具执行能力）

    通过子线程运行 hermes -z，不阻塞主循环（info 消息处理不受影响）。
    处理完成后删除 handoff 文件而非写入 {}，避免每次轮询无效 IO。
    """
    if not os.path.isfile(HANDOFF_FILE):
        return

    try:
        with open(HANDOFF_FILE) as f:
            handoff = json.load(f)
    except (json.JSONDecodeError, OSError):
        return

    sender = handoff.get("sender", "")
    summary = handoff.get("summary", "")
    if not sender or not summary:
        # 空 handoff 文件，删掉避免重复 stat
        try:
            os.remove(HANDOFF_FILE)
        except OSError:
            pass
        return

    log(f"📋 发现 handoff from={sender}: {summary[:60]}...")

    # 先删除 handoff 文件，防止重复处理
    try:
        os.remove(HANDOFF_FILE)
    except OSError:
        pass

    prompt = (
        f"[Tether Handoff] 来自 {sender} 的接力消息：\n{summary}\n\n"
        f"这是另一台 Hermes agent 发来的 handoff 消息，需要你主动处理。\n"
        f"1) 分析消息内容并执行必要的操作（修改文件、重启服务、curl 回复等）\n"
        f"2) 如需回复对方，使用 terminal 执行 curl POST 到对方 Tether 的 /message 端点\n"
        f"3) 处理完成后输出总结"
    )

    hermes = _find_hermes()
    if not hermes:
        log("❌ 找不到 hermes CLI，handoff 跳过")
        return

    # 子线程运行 hermes -z，不阻塞主循环
    def _run_handoff():
        try:
            r = subprocess.run(
                [hermes, "-z", prompt],
                capture_output=True, text=True, timeout=300
            )
            output = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else ""
            if output:
                log(f"✅ Handoff 处理完成 ({len(output)} chars)")
                # 将处理摘要写入结果文件，供 Gateway session 读取
                _write_handoff_result(sender, summary[:40], output[:500])
            else:
                log(f"⚠️ Handoff 完成但无输出 (rc={r.returncode})")
        except subprocess.TimeoutExpired:
            log(f"⏰ Handoff 超时 (300s)")
        except Exception as e:
            log(f"❌ Handoff 异常: {str(e)[:80]}")

    t = threading.Thread(target=_run_handoff, daemon=True)
    t.start()
    log("🔀 Handoff 已交给子线程处理，继续轮询")


def main():
    # 启动时先确保 Gateway 存活
    _ensure_gateway_alive()

    _ensure_gateway_session()

    log(f"Watcher 已启动 (间隔={POLL_INTERVAL}s, 自愈={_SELF_HEAL_INTERVAL}s)")
    last_mtime = 0
    last_heal_time = 0.0

    if not os.path.isfile(NOTIFY_FILE):
        try:
            with open(NOTIFY_FILE, "w") as f:
                json.dump({"time": "", "preview": "", "count": 0}, f)
        except Exception:
            pass

    while True:
        now = time.time()
        try:
            # 消息处理（notify 文件变化检测）
            if os.path.isfile(NOTIFY_FILE):
                mtime = os.path.getmtime(NOTIFY_FILE)
                if mtime > last_mtime:
                    last_mtime = mtime
                    process_messages()

            # handoff 文件检查（每次轮询都检查，不依赖 mtime）
            # handoff 消息不带 notify，由独立文件传递
            process_handoffs()

        except Exception as e:
            log(f"轮询异常: {e}")

        # 自愈巡检（每 _SELF_HEAL_INTERVAL 秒执行一次）
        if now - last_heal_time >= _SELF_HEAL_INTERVAL:
            last_heal_time = now
            _self_heal()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
