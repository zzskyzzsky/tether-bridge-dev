#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Server v3 — 极简消息中转
只做三件事：收消息、存 SQLite、写通知文件。
支持 type=handoff 消息：存 SQLite + 写 handoff 文件，但不触发 Watcher。
"""
import json, os, socket, sqlite3, time, uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

HOSTNAME = socket.gethostname()
LISTEN_PORT = 9001
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
NOTIFY_FILE = "/tmp/tether_notify.json"
HANDOFF_FILE = "/tmp/tether_handoff.json"
HEARTBEAT_FILE = "/tmp/tether_watcher_heartbeat.json"
HEARTBEAT_TIMEOUT = 15  # 心跳超过 15 秒判定 watcher 死亡

def _db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY, sender TEXT NOT NULL,
        message TEXT NOT NULL, received_at TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'info',
        acked INTEGER NOT NULL DEFAULT 0
    )""")
    # 兼容旧表（v3 升级）
    try:
        conn.execute("ALTER TABLE messages ADD COLUMN type TEXT NOT NULL DEFAULT 'info'")
    except sqlite3.OperationalError:
        pass
    conn.execute("""CREATE TABLE IF NOT EXISTS outgoing_messages (
        id TEXT PRIMARY KEY, target_host TEXT NOT NULL,
        sender TEXT NOT NULL, message TEXT NOT NULL,
        sent_at TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0
    )""")
    return conn

def _now():
    return datetime.now(timezone.utc).isoformat()

def _write_notify(preview, count):
    try:
        data = {"time": _now(), "preview": preview[:80], "count": count}
        with open(NOTIFY_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "hostname": HOSTNAME}), 200

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"pong": True, "hostname": HOSTNAME, "time": _now()})

@app.route("/status", methods=["GET"])
def status():
    watcher_alive = False
    watcher_pid = None
    watcher_lag = None

    try:
        if os.path.isfile(HEARTBEAT_FILE):
            with open(HEARTBEAT_FILE) as f:
                hb = json.load(f)
            hb_time = hb.get("timestamp", 0)
            now = time.time()
            watcher_lag = now - hb_time
            watcher_alive = watcher_lag < HEARTBEAT_TIMEOUT
            watcher_pid = hb.get("pid")
    except Exception:
        pass

    with _db() as conn:
        pending = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0 AND type='info'").fetchone()[0]
        unacked = conn.execute("SELECT COUNT(*) FROM outgoing_messages WHERE acked=0").fetchone()[0]
    return jsonify({
        "hostname": HOSTNAME, "messages_pending": pending,
        "messages_unacked_outgoing": unacked, "time": _now(),
        "tether_alive": True,  # 能响应 /status 说明 tether_server 自己活着
        "watcher_alive": watcher_alive,
        "watcher_pid": watcher_pid,
        "watcher_lag": round(watcher_lag, 1) if watcher_lag is not None else None,
    })

@app.route("/message", methods=["POST"])
def receive():
    data = request.get_json(silent=True) or {}
    sender = data.get("from") or data.get("sender", "unknown")
    content = data.get("message") or data.get("content", "")
    msg_type = data.get("type", "info")
    if not content.strip():
        return jsonify({"status": "ok", "dropped": "empty"})

    msg_id = str(uuid.uuid4())
    with _db() as conn:
        conn.execute(
            "INSERT INTO messages (id, sender, message, received_at, type, acked) VALUES (?,?,?,?,?,0)",
            (msg_id, sender, content, _now(), msg_type))

    if msg_type == "auto_reply":
        # auto_reply：存但立即 acked（只是 audit trail，不需要 watcher 处理）
        with _db() as conn:
            conn.execute("UPDATE messages SET acked=1 WHERE id=?", (msg_id,))
    elif msg_type == "handoff":
        # handoff：存 SQLite + 写 handoff 文件，不触发 Watcher notify
        # watcher 在主轮询中独立检查 handoff 文件
        try:
            with open(HANDOFF_FILE, "w") as f:
                json.dump({
                    "msg_id": msg_id,
                    "sender": sender,
                    "summary": content[:200],
                    "timestamp": _now(),
                }, f)
        except Exception:
            pass
    elif msg_type == "auto_reply":
        # auto_reply 只是确认回执，不需要 watcher 排队处理
        # 立即 ack（线上面 INSERT 的 acked=0 是 info/handoff 的默认值）
        conn = _db()
        conn.execute("UPDATE messages SET acked=1 WHERE id=?", (msg_id,))
        conn.close()
        # 检查是否有 in_reply_to，自动 ack 对方的 outgoing_messages
        in_reply_to = data.get("in_reply_to")
        if in_reply_to:
            conn = _db()
            conn.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (in_reply_to,))
            conn.close()
            print(f"✅ auto_reply in_reply_to={in_reply_to[:8]} → outgoing acked")
    else:
        # info 消息：正常触发 Watcher
        with _db() as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0 AND type='info'").fetchone()[0]
        _write_notify(content, cnt)
    return jsonify({"status": "ok", "message_id": msg_id})

@app.route("/messages", methods=["GET"])
def get_messages():
    auto_ack = request.args.get("ack", "1") == "1"
    msg_type = request.args.get("type", "info")  # 默认只返回 info 消息，传 all 返回全部
    with _db() as conn:
        if msg_type == "all":
            rows = conn.execute(
                "SELECT id,sender,message,type,received_at FROM messages WHERE acked=0"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id,sender,message,type,received_at FROM messages WHERE acked=0 AND type=?",
                (msg_type,)
            ).fetchall()
        if auto_ack and rows:
            # 仅 ACK 当前查询范围内的消息（handoff 不会被 info 查询误 ACK）
            ids = [r["id"] for r in rows]
            conn.execute(f"UPDATE messages SET acked=1 WHERE id IN ({','.join('?' for _ in ids)})", ids)
    msgs = [dict(r) for r in rows]
    return jsonify({"messages": msgs, "count": len(msgs)})

@app.route("/ack", methods=["POST"])
def ack():
    data = request.get_json(silent=True) or {}
    msg_ids = data.get("message_ids", [])
    if msg_ids:
        with _db() as conn:
            conn.execute(f"UPDATE outgoing_messages SET acked=1 WHERE id IN ({','.join('?' for _ in msg_ids)})", msg_ids)
    return jsonify({"status": "ok", "acked": len(msg_ids)})


@app.route("/pending", methods=["GET"])
def pending_outgoing():
    """返回本机已发送但尚未被对方 ack 的消息列表"""
    with _db() as conn:
        rows = conn.execute(
            "SELECT id, target_host, sender, message, sent_at, acked FROM outgoing_messages WHERE acked=0 ORDER BY sent_at DESC"
        ).fetchall()
    msgs = [dict(r) for r in rows]
    return jsonify({"messages": msgs, "count": len(msgs)})

def main():
    global LISTEN_PORT
    import argparse
    parser = argparse.ArgumentParser(description="Tether Server v3")
    parser.add_argument("--port", type=int, default=LISTEN_PORT)
    args = parser.parse_args()
    LISTEN_PORT = args.port

    with _db() as conn:
        pass  # init tables

    print(f"🌉 Tether v3 — {HOSTNAME} :{LISTEN_PORT}")
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
