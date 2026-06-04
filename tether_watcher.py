#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Watcher — 事件驱动消息处理器
2 秒轮询 /tmp/tether_notify.json 的修改时间，
发现新消息就调 hermes -z 处理。

独立于 tether_server.py 运行，没有耦合。
"""
import json, os, subprocess, time
import urllib.request

NOTIFY_FILE = "/tmp/tether_notify.json"
TETHER_URL = "http://127.0.0.1:9001"
POLL_INTERVAL = 2

def log(msg):
    print(f"[watcher] {msg}", flush=True)

def _tether_get(path):
    try:
        req = urllib.request.Request(f"{TETHER_URL}{path}")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read()), None
    except Exception as e:
        return None, str(e)

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
    hermes = _find_hermes()
    if not hermes:
        log("❌ 找不到 hermes CLI")
        return

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
            f"2) 如需回复对方，使用 curl POST 到 http://对方IP:9001/message\n"
            f"3) 你的回复输出会被记录，但不会自动转发"
        )
        try:
            r = subprocess.run(
                [hermes, "-z", prompt],
                capture_output=True, text=True, timeout=300
            )
            if r.returncode == 0 and r.stdout.strip():
                log(f"✅ {mid} 处理完成 ({len(r.stdout.strip())} chars)")
            else:
                log(f"⚠️ {mid} 处理完成但无输出")
        except subprocess.TimeoutExpired:
            log(f"⏰ {mid} 超时 (300s)")

    log("✅ 本轮处理完成")

def main():
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
