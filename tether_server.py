#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Server v3 — 极简消息中转
只做三件事：收消息、存 SQLite、写通知文件。
无 Worker 线程、无 Gateway 集成、无 ACK 协议、无模板系统。
"""
import json, os, socket, sqlite3, uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

HOSTNAME = socket.gethostname()
LISTEN_PORT = 9001
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
NOTIFY_FILE = "/tmp/tether_notify.json"

def _db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY, sender TEXT NOT NULL,
        message TEXT NOT NULL, received_at TEXT NOT NULL,
        acked INTEGER NOT NULL DEFAULT 0
    )""")
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
    content = data.get("message", "")
    if not content.strip():
        return jsonify({"status": "ok", "dropped": "empty"})

    msg_id = str(uuid.uuid4())
    with _db() as conn:
        conn.execute("INSERT INTO messages VALUES (?,?,?,?,0)",
                     (msg_id, sender, content, _now()))
    with _db() as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0").fetchone()[0]
    _write_notify(content, cnt)
    return jsonify({"status": "ok", "message_id": msg_id})

@app.route("/messages", methods=["GET"])
def get_messages():
    auto_ack = request.args.get("ack", "1") == "1"
    with _db() as conn:
        rows = conn.execute(
            "SELECT id,sender,message,received_at FROM messages WHERE acked=0"
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
