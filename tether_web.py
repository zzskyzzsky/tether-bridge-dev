#!/home/zzsky/.hermes/tether/venv/bin/python3
"""Tether Web v2 — 统一展示 Tether 消息（入站 + 出站 + 对端）

纯只读，不修改任何已有代码。读取 tether.db 直接展示。
"""
import json, os, socket, sqlite3, urllib.request
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, send_file

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether_web.html")
PORT = 9002
BJT = timezone(timedelta(hours=8))

HOSTNAME = socket.gethostname().lower()
_HOST_MAP = {"zzsky-mbp": "mac", "zzskytpg3": "tp"}
LOCAL_NAME = _HOST_MAP.get(HOSTNAME, HOSTNAME)
PEER_NAME = "tp" if LOCAL_NAME == "mac" else "mac"
RELAY_NAMES = {"relay", "154.8.143.218"}
PEER_WEB_URL = os.environ.get("TETHER_WEB_PEER_URL", "")

def _query(sql, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=3)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def _fmt_bj(iso_str):
    if not iso_str:
        return ""
    try:
        if "+" in iso_str or "Z" in iso_str:
            iso_str = iso_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_str)
        else:
            dt = datetime.strptime(iso_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        bj = dt.astimezone(BJT)
        return bj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_str[:17]

def _parse_sender(sender_str):
    if not sender_str:
        return "?"
    s_lower = sender_str.lower()
    if any(r in s_lower for r in RELAY_NAMES):
        return "relay"
    host_part = sender_str.split()[0].strip() if " " in sender_str else sender_str.strip()
    if "zzsky-mbp" in s_lower or "zzsky-mac" in s_lower or "mac-弟弟" in s_lower or "mac-小钉" in s_lower or host_part in ("mac",):
        return "mac"
    if "zzskytpg3" in s_lower or "zzskytpg" in s_lower or "tp-哥哥" in s_lower or "tp-小钉" in s_lower or host_part in ("tp",):
        return "tp"
    if host_part in ("mac", "zzsky-mbp"):
        return "mac"
    if host_part in ("tp", "zzskytpg3"):
        return "tp"
    return host_part[:15]

def _route_str_full(from_name, to_name, via_relay):
    if via_relay:
        return f"{from_name} → relay → {to_name}"
    return f"{from_name} → {to_name}"

def _collect_messages():
    results = []
    in_msgs = _query("SELECT id, sender, message, received_at AS time, type, acked FROM messages ORDER BY received_at ASC")
    for m in in_msgs:
        frm = _parse_sender(m["sender"])
        to = LOCAL_NAME
        via_relay = frm == "relay"
        route = f"{PEER_NAME} → relay → {to}" if via_relay else _route_str_full(frm, to, False)
        results.append({"id": m["id"], "dir": "in", "from": frm, "to": to, "route": route, "via_relay": via_relay, "time": m["time"], "time_bj": _fmt_bj(m["time"]), "type": m["type"] or "info", "message": m["message"], "source": "local"})

    out_msgs = _query("SELECT id, target_host, sender, message, sent_at AS time, acked FROM outgoing_messages ORDER BY sent_at ASC")
    for m in out_msgs:
        frm = LOCAL_NAME
        to_raw = m["target_host"].lower()
        if any(r in to_raw for r in RELAY_NAMES):
            to = "relay"
            route = f"{frm} → relay → {PEER_NAME}"
            via_relay = True
        else:
            to = "mac" if "mbp" in to_raw or "100.81" in to_raw else "tp" if "tpg" in to_raw or "100.102" in to_raw else m["target_host"][:10]
            route = _route_str_full(frm, to, False)
            via_relay = False
        results.append({"id": m["id"], "dir": "out", "from": frm, "to": to, "route": route, "via_relay": via_relay, "time": m["time"], "time_bj": _fmt_bj(m["time"]), "type": "info", "message": m["message"], "source": "local"})

    if PEER_WEB_URL:
        try:
            req = urllib.request.Request(f"{PEER_WEB_URL}/api/messages")
            with urllib.request.urlopen(req, timeout=5) as resp:
                peer_data = json.loads(resp.read().decode())
            for pm in peer_data.get("messages", []):
                if pm["source"] == "local":
                    pm["source"] = "peer"
                if not any(r["id"] == pm["id"] for r in results):
                    results.append(pm)
        except Exception:
            pass

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
        return jsonify({"messages": msgs, "count": len(msgs), "hostname": HOSTNAME, "local_name": LOCAL_NAME})
    except Exception as e:
        return jsonify({"error": str(e), "messages": []})

@app.route("/api/health")
def health():
    try:
        _query("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return jsonify({"hostname": HOSTNAME, "local_name": LOCAL_NAME, "tether_web": "ok", "database": "ok" if db_ok else "error"})

def main():
    print(f"🌐 Tether Web v2 — http://0.0.0.0:{PORT}  (local={LOCAL_NAME})")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
