#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Watcher — 事件驱动消息处理器
2 秒轮询 /tmp/tether_notify.json 的修改时间，
发现新消息就走 Gateway API 处理，Gateway 不可用时回退到 hermes -z。

独立于 tether_server.py 运行，没有耦合。
"""
import json, os, subprocess, time, urllib.request

NOTIFY_FILE = "/tmp/tether_notify.json"
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


def log(msg):
    print(f"[watcher] {msg}", flush=True)


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


def main():
    _ensure_gateway_session()

    log(f"Watcher 已启动 (间隔={POLL_INTERVAL}s)")
    last_mtime = 0

    if not os.path.isfile(NOTIFY_FILE):
        try:
            with open(NOTIFY_FILE, "w") as f:
                json.dump({"time": "", "preview": "", "count": 0}, f)
        except Exception:
            pass

    while True:
        try:
            if os.path.isfile(NOTIFY_FILE):
                mtime = os.path.getmtime(NOTIFY_FILE)
                if mtime > last_mtime:
                    last_mtime = mtime
                    process_messages()
        except Exception as e:
            log(f"轮询异常: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
