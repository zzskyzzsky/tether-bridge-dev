#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Watcher — 事件驱动消息处理器
2 秒轮询 /tmp/tether_notify.json 的修改时间，
发现新消息就走 Gateway API 处理，Gateway 不可用时回退到 hermes -z。

独立于 tether_server.py 运行，没有耦合。
"""
import json, os, subprocess, time, urllib.request

# 设置本机请求绕过 HTTP 代理（MacBook 上可能配了 Clash 环境变量）
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

NOTIFY_FILE = "/tmp/tether_notify.json"
HANDOFF_FILE = "/tmp/tether_handoff.json"
TETHER_URL = "http://127.0.0.1:9001"
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8642")
GATEWAY_SESSION = "tether-watcher"
GATEWAY_API_KEY = ""
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
    """
    global _last_restart_time

    if _is_gateway_alive():
        return True

    # 防抖：上次重启距今不足 30 秒则跳过
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

        # 等 5 秒再检查（给 Gateway 启动时间）
        time.sleep(5)
        if _is_gateway_alive():
            log("✅ Gateway 已恢复（重启后 5 秒）")
            return True

        # 再等两轮（最长 ~15 秒）
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

    # 重启确认：先等5s，再等两轮5s（最长~15s）
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
    """自愈巡检：Gateway 健康检查 + 自动重启（Tether 由 systemd Restart=always 管理）"""
    _ensure_gateway_alive()
    # 注意：不主动重启 tether.service — watcher 的 BindsTo=tether.service
    # 会导致自愈重启 tether 时连带杀死自己，形成级联循环
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
        msg_type = msg.get("type", "info")
        log(f"▶ 处理 {mid} from={sender} type={msg_type}: {content[:60]}")

        prompt = (
            f"[Tether Agent-to-Agent] 来自 {sender}：\n{content}\n\n"
            f"这是另一台 Hermes agent 通过 Tether 发来的消息。\n"
            f"1) 如果需要执行任务，使用 terminal 等工具完成\n"
            f"2) 如需回复对方，使用 curl POST 到对方 Tether 的 /message 端点\n"
            f"3) 处理完成后输出总结"
        )

        processed = False

        if msg_type == "handoff" or msg_type == "auto":
            # handoff/auto 消息：直接走 hermes -z（需要工具执行能力，Gateway 只输出文本不执行操作）
            hermes = _find_hermes()
            if hermes:
                try:
                    r = subprocess.run(
                        [hermes, "-z", prompt],
                        capture_output=True, text=True, timeout=300
                    )
                    if r.returncode == 0 and r.stdout.strip():
                        log(f"✅ {mid} 处理完成 (hermes -z, {len(r.stdout.strip())} chars)")
                    else:
                        log(f"⚠️ {mid} 处理完成但无输出 (rc={r.returncode})")
                    processed = True
                except subprocess.TimeoutExpired:
                    log(f"⏰ {mid} 超时 (300s)")
                except Exception as e:
                    log(f"❌ {mid} 子进程异常: {str(e)[:80]}")
            else:
                log("❌ 找不到 hermes CLI")

        if not processed:
            # info 消息：优先走 Gateway（更快更便宜），失败回退 hermes -z
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

    if not os.path.isfile(HANDOFF_FILE):
        try:
            with open(HANDOFF_FILE, "w") as f:
                json.dump({}, f)
        except Exception:
            pass

    while True:
        now = time.time()
        try:
            # 消息处理（每次轮询都尝试，不再依赖 mtime 变化）
            # process_messages 内部有 if not msgs: return 的快速返回，没有多余开销
            # 去掉 mtime 门控：避免 watcher 重启后因 mtime 未变而漏处理积压消息
            process_messages()

        except Exception as e:
            log(f"轮询异常: {e}")

        # 自愈巡检（每 _SELF_HEAL_INTERVAL 秒执行一次）
        if now - last_heal_time >= _SELF_HEAL_INTERVAL:
            last_heal_time = now
            _self_heal()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
