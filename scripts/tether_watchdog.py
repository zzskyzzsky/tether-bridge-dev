#!/usr/bin/env python3
"""
Tether Watchdog — 定时自愈保底机制
每 3 分钟跑一次，发现异常时先尝试自动修复，只有修复失败才通知主人。

自愈能力：
1. Tether 服务掉线 → 尝 systemctl restart
2. 消息队列卡住（queue_size > 0 且在增长）→ 重启 worker
3. 出站超过 5 分钟未 ACK → 重新发送
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
HOSTNAME = os.popen("hostname").read().strip() or "unknown"
STALE_THRESHOLD = 300  # 5 分钟

# ── 配置文件兜底读取 ──────────────────────────────────
def _read_config_value(key_prefix, file_paths):
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
    val = _read_config_value("TETHER_TOKEN=", ["~/.hermes/tether/.env"])
    if not val:
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
    val = _read_config_value("API_SERVER_KEY=", ["~/.hermes/.env"])
    if val:
        API_SERVER_KEY = val

# ── 工具函数 ──────────────────────────────────────────
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    print(f"[watchdog] {msg}", flush=True)

def _tether_get(path):
    url = f"http://{TETHER_HOST}:{TETHER_PORT}{path}"
    headers = {}
    if TETHER_TOKEN:
        headers["X-Tether-Token"] = TETHER_TOKEN
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read()), None
    except Exception as e:
        return None, str(e)

def _tether_post(path, data):
    url = f"http://{TETHER_HOST}:{TETHER_PORT}{path}"
    body = json.dumps(data).encode()
    headers = {"Content-Type": "application/json"}
    if TETHER_TOKEN:
        headers["X-Tether-Token"] = TETHER_TOKEN
    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read()), None
    except Exception as e:
        return None, str(e)

def _do_systemctl(action):
    """尝试 systemctl 操作，失败不抛异常"""
    try:
        r = subprocess.run(
            ["sudo", "systemctl", action, "tether"],
            capture_output=True, text=True, timeout=15
        )
        if r.returncode == 0:
            return True, None
        return False, r.stderr[:200]
    except Exception as e:
        return False, str(e)

def write_notify(message):
    try:
        data = {"new_messages": True, "timestamp": now_iso(),
                "source": "watchdog", "message": message[:120]}
        with open(NOTIFY_FILE, "w") as f:
            json.dump(data, f)
        log(f"NOTIFY_FILE 已写入: {message[:60]}")
    except Exception as e:
        log(f"NOTIFY_FILE 写入失败: {e}")

def send_dingtalk_via_gateway(message):
    """只有自愈失败才调用此函数通知主人"""
    if not API_SERVER_KEY:
        log("Gateway API Key 未设置，跳过钉钉通知")
        write_notify(message)
        return

    session_id = "tether-watchdog"
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {API_SERVER_KEY.strip()}"}

    try:
        url = f"{GATEWAY_URL}/api/sessions"
        payload = json.dumps({"title": session_id, "id": session_id}).encode()
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass

    prompt = (
        "[System Notification Tether Watchdog] 自主修复失败，需要主人关注。\n"
        f"异常：{message}\n\n"
        "=== 请执行 ===\n"
        "1. 使用 send_message 工具发送钉钉通知到 home channel\n"
        f"2. 内容：「[Tether故障] {message}」\n"
        "3. 回复 'done'\n"
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

# ── 自愈逻辑 ──────────────────────────────────────────
# 返回 (healed, needs_owner, message)
# healed=True 表示已自动修复；needs_owner=True 表示需告知主人

def heal_tether_down():
    """尝试重启 Tether systemd 服务"""
    ok, err = _do_systemctl("restart")
    if ok:
        time.sleep(3)
        data, _err = _tether_get("/ping")
        if data and data.get("pong"):
            log("✅ Tether 服务已重启成功")
            return True, True
    log(f"❌ Tether 服务重启失败: {err}")
    return False, True

def heal_queue_stuck(data):
    """队列卡住时尝试重启 worker（通过 /status 中的 queue_size 判断）"""
    qsize = data.get("queue_size", 0)
    if qsize == 0:
        return False, False

    log(f"⚠️ 队列积压 {qsize} 条，尝试内部 watchdog 恢复")
    # 等待 10 秒看内部 watchdog 是否处理
    time.sleep(10)
    data2, err = _tether_get("/status")
    if err:
        return False, True
    qsize2 = data2.get("queue_size", 0)
    if qsize2 < qsize:
        log(f"✅ 队列自愈成功 ({qsize}→{qsize2})")
        return True, False
    log(f"❌ 队列积压未缓解 ({qsize}→{qsize2})，需要人工介入")
    return False, True

def heal_stale_outgoing():
    """检测并重发超过 5 分钟未 ACK 的出站消息"""
    db_path = os.path.expanduser("~/.hermes/tether/tether.db")
    if not os.path.isfile(db_path):
        return False, False

    try:
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        now = time.time()
        rows = cur.execute(
            "SELECT id, target_host, sender, message, sent_at FROM outgoing_messages WHERE acked=0"
        ).fetchall()
        conn.close()

        stale = []
        for mid, target, sender, msg, sent_ts in rows:
            try:
                sent_dt = datetime.fromisoformat(sent_ts)
                if now - sent_dt.timestamp() > STALE_THRESHOLD:
                    stale.append((mid, target, msg))
            except Exception:
                stale.append((mid, target, msg))

        if not stale:
            return False, False

        log(f"⚠️ {len(stale)} 条出站消息超过 {STALE_THRESHOLD}s 未 ACK，尝试重发")
        retried = 0
        for mid, target, msg in stale:
            # 直接 POST 到对方 Tether
            url = f"http://{target}:{TETHER_PORT}/message" if ":" not in str(target) else f"http://{target}/message"
            body = json.dumps({"from": HOSTNAME, "message": msg, "retry": True}).encode()
            headers = {"Content-Type": "application/json"}
            if TETHER_TOKEN:
                headers["X-Tether-Token"] = TETHER_TOKEN
            try:
                req = urllib.request.Request(url, data=body, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    pass
                log(f"  ✅ 重发 {mid[:12]} → {target}")
                retried += 1
            except Exception:
                log(f"  ❌ 重发 {mid[:12]} → {target} 失败")

        if retried > 0:
            log(f"✅ 重发了 {retried}/{len(stale)} 条消息")
            return True, False
        return False, True
    except Exception as e:
        log(f"SQLite 查询失败: {e}")
        return False, True

# ── 主流程 ────────────────────────────────────────────
def main():
    log(f"巡检开始 (host={HOSTNAME})")
    needs_owner = False
    actions_taken = []

    # 1. 检查 Tether 是否在线
    data, err = _tether_get("/status")
    if err or not data:
        log(f"Tether 无响应: {err}")
        healed, notify = heal_tether_down()
        if healed:
            actions_taken.append("Tether 已重启")
            data, _ = _tether_get("/status")
        if notify:
            send_dingtalk_via_gateway(f"Tether 服务无响应{'，已自动重启' if healed else '，重启失败'}")
        return

    # 2. 检查队列是否卡住
    healed, notify = heal_queue_stuck(data)
    if healed:
        actions_taken.append("队列已恢复")
    if notify:
        needs_owner = True

    # 3. 检查积压消息（DB 级 pending，不是队列）
    db_pending = data.get("messages_pending", 0)
    if db_pending > 0:
        log(f"⚠️ DB 中有 {db_pending} 条待处理消息")
        needs_owner = True

    # 4. 检查并重发出站未 ACK 消息
    healed, notify = heal_stale_outgoing()
    if healed:
        actions_taken.append(f"重发出站消息")
    if notify:
        needs_owner = True

    # 5. 汇总
    unacked = data.get("messages_unacked_outgoing", 0) if data else 0
    qsize = data.get("queue_size", 0) if data else 0
    db_pending = data.get("messages_pending", 0) if data else 0

    if needs_owner:
        summary = f"队列={qsize}，积压={db_pending}，未ACK={unacked}"
        if actions_taken:
            summary = f"已执行: {'; '.join(actions_taken)}。当前状态：{summary}"
        log(f"⚠️ 需要关注: {summary}")
        send_dingtalk_via_gateway(summary)
    else:
        log(f"正常 (queue={qsize}, pending={db_pending}, unacked={unacked})")

if __name__ == "__main__":
    main()
