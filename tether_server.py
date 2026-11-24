#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Server v3 — 极简消息中转
只做三件事：收消息、存 SQLite、写通知文件。
支持 type=handoff 消息：存 SQLite + 写 handoff 文件，但不触发 Watcher。
"""
import json, os, socket, sqlite3, time, uuid, hashlib
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

HOSTNAME = socket.gethostname()
LISTEN_PORT = 9003
# DB_PATH 可用环境变量覆盖（仅测试用；生产走默认路径）
DB_PATH = os.environ.get("TETHER_DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tether.db")
# 逻辑对端身份：relay 会把 sender 统一改写成中继 IP，报文里的 sender 不可用来判作者/去重，
# 只能由配置给出（两端一致）。未配置时退化为报文 sender。
PEER_KEY = os.environ.get("TETHER_PEER_KEY", "").strip()
# 去重窗：普通 120 秒；watcher 出站 auto-reply 自带 X-Tether-Auto-Reply: 1 → 600 秒
DEDUP_WINDOW_DEFAULT = 120
DEDUP_WINDOW_AUTO_REPLY = 600
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
    # 收件口去重状态（v3.13）：key = sha256(peer|content)，见 receive()
    conn.execute("""CREATE TABLE IF NOT EXISTS dedup_seen (
        key TEXT PRIMARY KEY, ts REAL NOT NULL, window_s INTEGER NOT NULL
    )""")
    # 去重命中诊断（600 秒窗命中必须留痕，否则没人知道长窗吞没吞正常消息）
    conn.execute("""CREATE TABLE IF NOT EXISTS dedup_log (
        ts REAL NOT NULL, key TEXT NOT NULL, window_s INTEGER NOT NULL,
        action TEXT NOT NULL, preview TEXT
    )""")
    return conn

@contextmanager
def _db_conn():
    """Open → yield → commit/rollback → **close**.

    `with sqlite3.connect(...) as conn` only manages the transaction; the connection
    itself survives until GC, and CPython's sqlite3 connections sit in reference cycles
    that a GC pass does not reliably collect. Leaked handles pin a read snapshot, so WAL
    never checkpoints and the file grows until the process hits EMFILE.
    """
    with closing(_db()) as conn, conn:
        yield conn

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

    with _db_conn() as conn:
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

    # ── 收件口内容去重（v3.13）─────────────────────────────────────────
    # 目的：吃掉 tether_send 重试（同内容、不同 uuid）与逐字重发的连发。
    # 键 = sha256(peer|content)：peer 由配置给出（relay 会把报文 sender 改写成中继 IP，
    # 用它做键等于只按内容哈希）。窗口两档：auto-reply 600 秒，其余 120 秒，头缺失取短。
    # ⚠️ 这条挡不住「措辞每轮不同的回声」——那由 watcher 出站成本闸兜底。
    is_auto_reply = (request.headers.get("X-Tether-Auto-Reply", "").strip() == "1")
    window_s = DEDUP_WINDOW_AUTO_REPLY if is_auto_reply else DEDUP_WINDOW_DEFAULT
    peer = PEER_KEY or sender
    dkey = hashlib.sha256(f"{peer}|{content}".encode("utf-8", errors="replace")).hexdigest()
    now_ts = time.time()
    with _db_conn() as conn:
        # 顺手清掉过期状态，表不会无限长
        conn.execute("DELETE FROM dedup_seen WHERE ts < ?", (now_ts - 3600,))
        row = conn.execute("SELECT ts, window_s FROM dedup_seen WHERE key=?", (dkey,)).fetchone()
        hit = bool(row) and (now_ts - row["ts"] <= max(row["window_s"], window_s))
        if hit:
            conn.execute(
                "INSERT INTO dedup_log (ts,key,window_s,action,preview) VALUES (?,?,?,?,?)",
                (now_ts, dkey, window_s, "dropped",
                 content.replace("\n", " ")[:40]))
        else:
            conn.execute(
                "INSERT INTO dedup_seen (key,ts,window_s) VALUES (?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET ts=excluded.ts, window_s=excluded.window_s",
                (dkey, now_ts, window_s))
    if hit:
        return jsonify({"status": "ok", "dropped": "dup", "window_s": window_s})

    msg_id = str(uuid.uuid4())
    with _db_conn() as conn:
        conn.execute(
            "INSERT INTO messages (id, sender, message, received_at, type, acked) VALUES (?,?,?,?,?,0)",
            (msg_id, sender, content, _now(), msg_type))

    # 统一处理 in_reply_to（无论消息类型，自动确认对方的出站消息）
    in_reply_to = data.get("in_reply_to")
    if in_reply_to:
        with _db_conn() as conn:
            conn.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (in_reply_to,))

    if msg_type == "auto_reply":
        # auto_reply：旧格式立即 acked，仅审计轨迹，不触发 watcher
        with _db_conn() as conn:
            conn.execute("UPDATE messages SET acked=1 WHERE id=?", (msg_id,))
    elif msg_type == "handoff":
        # handoff：存 SQLite + 写 handoff 文件，不触发 Watcher notify
        # watcher 在主轮询中独立检查 handoff 文件
        try:
            with open(HANDOFF_FILE, "w") as f:
                json.dump({
                    "msg_id": msg_id,
                    "sender": sender,
                    # ⚠️ 必须写全文：watcher 把该字段当作 prompt 全文使用（不是真的摘要）。
                    # 历史上这里写的是 content[:200]，导致长 handoff 后文被静默丢弃。
                    "summary": content,
                    "timestamp": _now(),
                }, f)
        except Exception:
            pass
    else:
        # info 消息：正常触发 Watcher
        with _db_conn() as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0 AND type='info'").fetchone()[0]
        _write_notify(content, cnt)
    return jsonify({"status": "ok", "message_id": msg_id})

def _has_ttl(conn):
    """ttl 列由 scripts/migrate_origin_ttl.py 加，迁移前后都要能跑。"""
    return any(r[1] == "ttl" for r in conn.execute("PRAGMA table_info(messages)"))


@app.route("/messages", methods=["GET"])
def get_messages():
    auto_ack = request.args.get("ack", "1") == "1"
    msg_type = request.args.get("type", "info")  # 默认只返回 info 消息，传 all 返回全部
    with _db_conn() as conn:
        # ttl 是诊断列，必须原样返回给对端（迁移未跑时自动降级为不返回）
        cols = "id,sender,message,type,received_at,ttl" if _has_ttl(conn) \
            else "id,sender,message,type,received_at"
        if msg_type == "all":
            rows = conn.execute(
                f"SELECT {cols} FROM messages WHERE acked=0"
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {cols} FROM messages WHERE acked=0 AND type=?",
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
        with _db_conn() as conn:
            conn.execute(f"UPDATE outgoing_messages SET acked=1 WHERE id IN ({','.join('?' for _ in msg_ids)})", msg_ids)
    return jsonify({"status": "ok", "acked": len(msg_ids)})


@app.route("/pending", methods=["GET"])
def pending_outgoing():
    """返回本机已发送但尚未被对方 ack 的消息列表"""
    with _db_conn() as conn:
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

    with _db_conn() as conn:
        pass  # init tables

    print(f"🌉 Tether v3 — {HOSTNAME} :{LISTEN_PORT}")
    app.run(host="0.0.0.0", port=LISTEN_PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
