#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Relay — 双向消息中继服务器
部署在 VPS 上，作为 TP 和 mac 之间的统一消息中转。

核心逻辑：
1. 收到 /message POST → 存 DB + 写 notify
2. 修改 sender 为 relay hostname（如 "relay"）
3. 转发给对端（RELAY_PEER）
4. 返回 ok

TP 和 mac 的 watcher 都配置 TETHER_PEER_HOST=VPS 公网 IP。
所有出站消息先到 VPS，VPS 统一转发给对端。
形成闭环：TP ↔ VPS ↔ mac

不依赖 Tailscale P2P，不依赖控制服务器。
纯 HTTP 转发，应用层透明。
"""
import json
import os
import socket
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from urllib.request import Request, urlopen

app = Flask(__name__)

HOSTNAME = socket.gethostname()
LISTEN_PORT = int(os.environ.get("RELAY_PORT", "9001"))

# TP 的 Tether Server（mac 发来的消息要转发给 TP）
RELAY_TP = os.environ.get("RELAY_TP", "http://100.102.54.90:9001")
# mac 的 Tether Server（TP 发来的消息要转发给 mac）
RELAY_MAC = os.environ.get("RELAY_MAC", "http://100.81.192.38:9001")

# 中继使用的 sender 标识 — 必须是 IP 或域名，让 watcher 能 POST 到对端
RELAY_SENDER = os.environ.get("RELAY_SENDER", "154.8.143.218 (relay)")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether_relay.db")
NOTIFY_FILE = "/tmp/tether_relay_notify.json"
HEARTBEAT_FILE = "/tmp/tether_relay_heartbeat.json"
HEARTBEAT_TIMEOUT = 15

# 统计
message_count = 0
last_health = {"status": "starting", "backend": "connecting", "uptime": 0, "messages": 0}


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


def forward_to_peer(data, sender_id):
    """将消息转发给对端 — 根据 sender_id 决定发往 TP 还是 mac"""
    global message_count
    target = RELAY_TP if sender_id == "tp" else RELAY_MAC
    try:
        payload = json.dumps(data, ensure_ascii=False).encode()
        req = Request(
            target + "/message",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        resp = urlopen(req, timeout=10)
        message_count += 1
        return True
    except Exception as e:
        print(f"⚠️ Forward to {target} failed: {e}", file=sys.stderr)
        return False


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "hostname": HOSTNAME,
        "relay_tp": RELAY_TP,
        "relay_mac": RELAY_MAC,
        "messages": message_count,
        "uptime": round(time.time() - last_health.get("_start", time.time())),
    }), 200


@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"pong": True, "hostname": HOSTNAME, "time": _now()})


@app.route("/status", methods=["GET"])
def status():
    watcher_alive = False
    try:
        if os.path.isfile(HEARTBEAT_FILE):
            with open(HEARTBEAT_FILE) as f:
                hb = json.load(f)
            now = time.time()
            watcher_alive = (now - hb.get("timestamp", 0)) < HEARTBEAT_TIMEOUT
    except Exception:
        pass

    with _db() as conn:
        pending = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE acked=0"
        ).fetchone()[0]
    return jsonify({
        "hostname": HOSTNAME,
        "relay_tp": RELAY_TP,
        "relay_mac": RELAY_MAC,
        "messages_pending": pending,
        "messages_total": message_count,
        "watcher_alive": watcher_alive,
        "time": _now(),
    })


@app.route("/message", methods=["POST"])
def receive():
    """接收消息 → 存 DB → 修改 sender → 转发对端"""
    global message_count
    data = request.get_json(silent=True) or {}
    sender = data.get("from") or data.get("sender", "unknown")
    content = data.get("message") or data.get("content", "")
    msg_type = data.get("type", "info")

    if not content.strip():
        return jsonify({"status": "ok", "dropped": "empty"})

    msg_id = str(uuid.uuid4())

    # 修改 sender 为 relay 标识（让对端的 watcher 发到 TETHER_PEER_HOST）
    data["sender"] = RELAY_SENDER
    data["from"] = RELAY_SENDER
    if "from_nick" in data:
        data["from_nick"] = "relay"
    # TTL 防死循环：relay 转发时设置 ttl=1，对端 watcher 收到后 TTL-1
    # 若 TTL=0 则停止转发，避免消息循环
    data["ttl"] = 1

    with _db() as conn:
        conn.execute(
            "INSERT INTO messages (id, sender, message, received_at, type, acked) "
            "VALUES (?,?,?,?,?,0)",
            (msg_id, data["sender"], content, _now(), msg_type),
        )

    if msg_type == "handoff":
        HANDOFF_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether_handoff.json")
        try:
            with open(HANDOFF_FILE, "w") as f:
                json.dump({"msg_id": msg_id, "sender": RELAY_SENDER,
                           "summary": content[:200], "timestamp": _now()}, f)
        except Exception:
            pass
    else:
        with _db() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE acked=0 AND type='info'"
            ).fetchone()[0]
        _write_notify(content, cnt)

    # 转发给对端
    ok = forward_to_peer(data)

    return jsonify({"status": "ok", "message_id": msg_id, "forwarded": ok})


@app.route("/messages", methods=["GET"])
def get_messages():
    auto_ack = request.args.get("ack", "1") == "1"
    msg_type = request.args.get("type", "info")
    with _db() as conn:
        if msg_type == "all":
            rows = conn.execute(
                "SELECT id,sender,message,type,received_at FROM messages WHERE acked=0"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id,sender,message,type,received_at FROM messages WHERE acked=0 AND type=?",
                (msg_type,),
            ).fetchall()
        if auto_ack and rows:
            ids = [r["id"] for r in rows]
            conn.execute(
                f"UPDATE messages SET acked=1 WHERE id IN ({','.join('?' for _ in ids)})",
                ids,
            )
    msgs = [dict(r) for r in rows]
    return jsonify({"messages": msgs, "count": len(msgs)})


@app.route("/ack", methods=["POST"])
def ack():
    data = request.get_json(silent=True) or {}
    msg_ids = data.get("message_ids", [])
    if msg_ids:
        with _db() as conn:
            conn.execute(
                f"UPDATE outgoing_messages SET acked=1 WHERE id IN ({','.join('?' for _ in msg_ids)})",
                msg_ids,
            )
    return jsonify({"status": "ok", "acked": len(msg_ids)})


@app.route("/pending", methods=["GET"])
def pending_outgoing():
    with _db() as conn:
        rows = conn.execute(
            "SELECT id, target_host, sender, message, sent_at, acked "
            "FROM outgoing_messages WHERE acked=0 ORDER BY sent_at DESC"
        ).fetchall()
    msgs = [dict(r) for r in rows]
    return jsonify({"messages": msgs, "count": len(msgs)})


def health_loop():
    """后台心跳 + 对端连通性检查"""
    global last_health
    last_health["_start"] = time.time()
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            peer_host = RELAY_PEER.replace("http://", "").replace("https://", "").split(":")[0]
            peer_port = 9001
            s.connect((peer_host, peer_port))
            status = "connected"
            s.close()
        except Exception:
            status = "disconnected"
        last_health.update({
            "status": "running",
            "backend": status,
            "messages": message_count,
        })
        time.sleep(15)


def main():
    global LISTEN_PORT, RELAY_PEER, RELAY_SENDER
    import argparse
    parser = argparse.ArgumentParser(description="Tether Relay v3 — 双向中继")
    parser.add_argument("--port", type=int, default=LISTEN_PORT)
    parser.add_argument("--peer", type=str, default=RELAY_PEER,
                        help="对端 Tether Server 地址 (http://IP:PORT)")
    parser.add_argument("--sender", type=str, default=RELAY_SENDER,
                        help="中继使用的 sender 标识")
    args = parser.parse_args()
    LISTEN_PORT = args.port
    RELAY_PEER = args.peer
    RELAY_SENDER = args.sender

    # 初始化 DB
    with _db() as conn:
        pass

    print(f"🌉 Tether Relay v3 — {HOSTNAME} :{LISTEN_PORT}")
    print(f"   Relay peer: {RELAY_PEER}")
    print(f"   Relay sender: {RELAY_SENDER}")

    # 启动心跳
    threading.Thread(target=health_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
