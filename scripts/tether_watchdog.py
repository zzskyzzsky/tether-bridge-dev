#!/usr/bin/env python3
"""
Tether Watchdog — 定时巡检保底机制
每 3 分钟跑一次，检查 Tether /status，有积压就推钉钉通知。
不替代 Layer 1/2，纯安全网。
"""
import os, sys, json, time, subprocess, urllib.request
from datetime import datetime, timezone

# 配置
TETHER_HOST = os.environ.get("TETHER_HOST", "127.0.0.1")
TETHER_PORT = os.environ.get("TETHER_PORT", "9001")
TETHER_TOKEN = os.environ.get("TETHER_TOKEN", "")
NOTIFY_FILE = "/tmp/tether_notify.json"
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8642")
API_SERVER_KEY = os.environ.get("API_SERVER_KEY", "")

# 从配置文件兜底读取（cron 环境下没有 systemd 传入的环境变量）
def _read_config_value(key_prefix, file_paths):
    """从多个配置文件读取值，返回第一个找到的值或 None"""
    for fpath in file_paths:
        fpath = os.path.expanduser(fpath)
        if not os.path.isfile(fpath):
            continue
        try:
            with open(fpath) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith(key_prefix):
                        return line.split("=", 1)[1].strip()
        except Exception:
            pass
    return None

if not TETHER_TOKEN:
    # 先从 .env
    val = _read_config_value("TETHER_TOKEN=", ["~/.hermes/tether/.env"])
    if not val:
        # 从 tether.service 读取（格式: Environment=TETHER_TOKEN=xxx）
        svc = os.path.expanduser("~/.hermes/tether/tether.service")
        if os.path.isfile(svc):
            try:
                with open(svc) as f:
                    for line in f:
                        if "Environment=TETHER_TOKEN=" in line:
                            val = line.split("Environment=TETHER_TOKEN=", 1)[1].strip()
                            break
            except Exception:
                pass
    if val:
        TETHER_TOKEN = val.split("\\n")[0].strip()

if not API_SERVER_KEY:
    val = _read_config_value("API_SERVER_KEY=", [
        "~/.hermes/.env",
    ])
    if val:
        API_SERVER_KEY = val
HOSTNAME = os.popen("hostname").read().strip() or "unknown"
STALE_THRESHOLD = 300  # 5 分钟

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[watchdog] {msg}", flush=True)

def check_status():
    """查 Tether /status，返回 (has_pending, has_stale, detail)"""
    url = f"http://{TETHER_HOST}:{TETHER_PORT}/status"
    headers = {}
    if TETHER_TOKEN:
        headers["X-Tether-Token"] = TETHER_TOKEN
    try:
        import urllib.request
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        log(f"Tether 状态查询失败: {e}")
        return False, False, {"pending": 0, "unacked": 0, "error": str(e)}

    pending = data.get("messages_pending", 0)
    unacked = data.get("messages_unacked_outgoing", 0)
    now = time.time()

    has_pending = pending > 0
    has_stale = False

    if unacked > 0:
        # 检查出站消息是否超过 STALE_THRESHOLD 未 ACK
        try:
            import sqlite3
            db_path = os.path.expanduser("~/.hermes/tether/tether.db")
            if os.path.isfile(db_path):
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    "SELECT sent_at FROM outgoing_messages WHERE acked=0 ORDER BY sent_at ASC LIMIT 1"
                )
                row = cur.fetchone()
                conn.close()
                if row:
                    sent_ts = row[0]
                    # Parse ISO timestamp
                    try:
                        sent_dt = datetime.fromisoformat(sent_ts)
                        sent_epoch = sent_dt.timestamp()
                        if now - sent_epoch > STALE_THRESHOLD:
                            has_stale = True
                    except:
                        has_stale = True  # can't parse, assume stale
        except Exception as e:
            log(f"SQLite 查询失败: {e}")

    detail = {"pending": pending, "unacked": unacked, "stale_unacked": has_stale}
    return has_pending, has_stale, detail


def write_notify(message):
    """写入 NOTIFY_FILE，供 agent 自检"""
    try:
        data = {
            "new_messages": True,
            "timestamp": now_iso(),
            "source": "watchdog",
            "message": message[:120],
        }
        with open(NOTIFY_FILE, "w") as f:
            json.dump(data, f)
        log(f"NOTIFY_FILE 已写入: {message[:60]}")
    except Exception as e:
        log(f"NOTIFY_FILE 写入失败: {e}")


def send_dingtalk_via_gateway(message):
    """通过 Gateway session 推钉钉通知"""
    if not API_SERVER_KEY:
        log("Gateway API Key 未设置，跳过钉钉通知")
        write_notify(message)
        return

    session_id = "tether-watchdog"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_SERVER_KEY.strip()}",
    }

    # 确保 session 存在
    try:
        url = f"{GATEWAY_URL}/api/sessions"
        payload = json.dumps({"title": session_id, "id": session_id}).encode()
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # session 可能存在

    prompt = (
        "[System Notification Tether Watchdog]\n"
        f"巡检发现异常：{message}\n\n"
        "=== 请执行 ===\n"
        "1. 使用 send_message 工具向钉钉发送通知到 home channel\n"
        f"2. 通知内容：「[Tether巡检] {message}」\n"
        "3. 回复 'done'\n\n"
        "注意：直接执行 send_message，不需要分析消息内容。"
    )

    try:
        chat_url = f"{GATEWAY_URL}/api/sessions/{session_id}/chat"
        payload2 = json.dumps({"message": prompt}).encode()
        req = urllib.request.Request(chat_url, data=payload2, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=90) as resp:
            result = json.loads(resp.read())
        log(f"钉钉通知已推送: {result.get('message',{}).get('content','')[:60]}")
    except Exception as e:
        log(f"钉钉通知推送失败: {e}")
        write_notify(message)


def main():
    log(f"巡检开始 (host={HOSTNAME})")
    has_pending, has_stale, detail = check_status()

    messages = []
    if has_pending:
        messages.append(f"有 {detail['pending']} 条待处理消息")
    if has_stale:
        messages.append(f"有 {detail['unacked']} 条出站消息超过 5 分钟未 ACK")

    if messages:
        summary = "；".join(messages)
        log(f"⚠️ 发现异常: {summary}")
        send_dingtalk_via_gateway(summary)
    else:
        log(f"正常 (pending={detail['pending']}, unacked={detail['unacked']})")


if __name__ == "__main__":
    main()
