#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Server v3 — 极简消息中转
只做三件事：收消息、存 SQLite、写通知文件。
支持 type=handoff 消息：存 SQLite + 写 handoff 文件，但不触发 Watcher。
"""
import json, os, socket, sqlite3, uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

HOSTNAME = socket.gethostname()
LISTEN_PORT = 9001
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
NOTIFY_FILE = "/tmp/tether_notify.json"
HANDOFF_FILE = "/tmp/tether_handoff.json"

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
    with _db() as conn:
        pending = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0").fetchone()[0]
        unacked = conn.execute("SELECT COUNT(*) FROM outgoing_messages WHERE acked=0").fetchone()[0]
    return jsonify({
        "hostname": HOSTNAME, "messages_pending": pending,
        "messages_unacked_outgoing": unacked, "time": _now(),
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

    if msg_type == "handoff":
        # handoff：仍然存 SQLite 和 handoff 文件（作为诊断备份），
        # 但同时也写 notify 文件，让 watcher 统一处理（不再分流）
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

    # 所有消息类型都写 notify 文件（让 watcher 统一处理）
    # handoff 类型的消息由 watcher 识别后走 hermes -z（有工具执行能力）
    with _db() as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0").fetchone()[0]
    _write_notify(content, cnt)
    return jsonify({"status": "ok", "message_id": msg_id})

@app.route("/messages", methods=["GET"])
def get_messages():
    auto_ack = request.args.get("ack", "1") == "1"
    msg_type = request.args.get("type", "all")  # 默认返回全部消息（不再按类型分流）
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
