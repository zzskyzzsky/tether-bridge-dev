#!/home/zzsky/.hermes/tether/venv/bin/python3
"""Tether Web v2 — 统一展示 Tether 消息（入站 + 出站）

纯只读，不修改任何已有代码。读取 tether.db 直接展示。
"""
import json, os, socket, sqlite3
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, send_file

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether_web.html")
PORT = 9002
BJT = timezone(timedelta(hours=8))  # 北京时间

# 主机名 → 显示名映射
HOSTNAME = socket.gethostname().lower()
_HOST_MAP = {
    "zzsky-mbp": "mac",
    "zzskytpg3": "tp",
}
LOCAL_NAME = _HOST_MAP.get(HOSTNAME, HOSTNAME)
# 对端 = 非本机的那个
PEER_NAME = "tp" if LOCAL_NAME == "mac" else "mac"
RELAY_NAMES = {"relay", "154.8.143.218"}


def _query(sql, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=3)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _fmt_bj(iso_str):
    """将 UTC ISO 时间转为北京时间 YYMMDD-HHMMSS"""
    if not iso_str:
        return ""
    try:
        if "+" in iso_str or "Z" in iso_str:
            # 去除末尾的 +00:00 / Z
            iso_str = iso_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_str)
        else:
            # 无时区信息，假设是 UTC
            dt = datetime.strptime(iso_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        bj = dt.astimezone(BJT)
        return bj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_str[:17] if len(iso_str) >= 17 else iso_str


def _parse_sender(sender_str):
    """从 sender 字段提取显示名"""
    s_lower = sender_str.lower()
    if any(r in s_lower for r in RELAY_NAMES):
        return "relay"
    # mac 侧识别：主机名、昵称、或裸 mac
    if "zzsky-mbp" in s_lower or "zzsky-mac" in s_lower or "mac-弟弟" in s_lower or "mac-小钉" in s_lower or sender_str.strip() in ("mac",):
        return "mac"
    # tp 侧识别：主机名、昵称、或裸 tp（不含 http:// 误匹配）
    if "zzskytpg3" in s_lower or "zzskytpg" in s_lower or "tp-哥哥" in s_lower or "tp-小钉" in s_lower or sender_str.strip() in ("tp",):
        return "tp"
    return sender_str[:15]


def _route_str(from_name, to_name):
    """构建 sender → receiver 字符串"""
    if from_name == "relay":
        # 来源不明，经过 relay
        return f"relay → {to_name}"
    return f"{from_name} → {to_name}"


def _parse_target(target_host):
    """从 target_host 解析显示名"""
    t_lower = target_host.lower()
    if any(r in t_lower for r in RELAY_NAMES):
        return "relay"
    if "zzsky-mbp" in t_lower or "zzsky-mac" in t_lower:
        return "mac"
    if "zzskytpg3" in t_lower or "zzskytpg" in t_lower:
        return "tp"
    # 已知 IP
    if t_lower == "100.81.192.38":
        return "mac"
    if t_lower == "100.102.54.90":
        return "tp"
    return target_host[:15]


def _route_str_full(from_name, to_name, via_relay):
    """构建完整路由字符串"""
    if via_relay:
        return f"{from_name} → relay → {to_name}"
    return f"{from_name} → {to_name}"


def _collect_messages():
    """合并入站和出站消息，统一格式"""
    results = []

    # 入站消息
    in_msgs = _query(
        "SELECT id, sender, message, received_at AS time, type, acked "
        "FROM messages ORDER BY received_at ASC"
    )
    for m in in_msgs:
        frm = _parse_sender(m["sender"])
        to = LOCAL_NAME
        via_relay = frm == "relay"
        if via_relay:
            # relay 消息：原始发送方是对端（PEER_NAME）
            route = f"{PEER_NAME} → relay → {to}"
        else:
            route = _route_str_full(frm, to, False)
        results.append({
            "id": m["id"],
            "dir": "in",
            "from": frm,
            "to": to,
            "route": route,
            "via_relay": via_relay,
            "time": m["time"],
            "time_bj": _fmt_bj(m["time"]),
            "type": m["type"] or "info",
            "message": m["message"],
        })

    # 出站消息
    out_msgs = _query(
        "SELECT id, target_host, sender, message, sent_at AS time, acked "
        "FROM outgoing_messages ORDER BY sent_at ASC"
    )
    for m in out_msgs:
        frm = LOCAL_NAME
        to = _parse_target(m["target_host"])
        if to == "relay":
            # 发送到 relay：显示 LOCAL → relay → PEER
            route = f"{frm} → relay → {PEER_NAME}"
            via_relay = True
        else:
            route = _route_str_full(frm, to, False)
            via_relay = False
        results.append({
            "id": m["id"],
            "dir": "out",
            "from": frm,
            "to": to,
            "route": route,
            "via_relay": via_relay,
            "time": m["time"],
            "time_bj": _fmt_bj(m["time"]),
            "type": "info",
            "message": m["message"],
        })

    # 按时间排序（升序）
    results.sort(key=lambda x: x["time"] or "")
    return results


@app.route("/")
def index():
    if os.path.isfile(HTML_PATH):
        return send_file(HTML_PATH)
    return "<h1>Tether 消息</h1><p>tether_web.html not found</p>", 200


@app.route("/api/messages")
def get_messages():
    try:
        msgs = _collect_messages()
        return jsonify({
            "messages": msgs,
            "count": len(msgs),
            "hostname": HOSTNAME,
            "local_name": LOCAL_NAME,
        })
    except Exception as e:
        return jsonify({"error": str(e), "messages": []})


@app.route("/api/health")
def health():
    try:
        _query("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return jsonify({
        "hostname": HOSTNAME,
        "local_name": LOCAL_NAME,
        "tether_web": "ok",
        "database": "ok" if db_ok else "error",
    })


def main():
    print(f"🌐 Tether Web v2 — http://0.0.0.0:{PORT}  (local={LOCAL_NAME})")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
