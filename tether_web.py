#!/home/zzsky/.hermes/tether/venv/bin/python3
"""Tether Web v3 — 统一展示 Tether 消息 + Tailscale 状态

纯只读（除 /api/clear 外），不修改任何已有代码。
"""
import json, os, socket, sqlite3, subprocess, urllib.request
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_file

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
RELAY_HOST = "154.8.143.218"

_TAILSCALE_PEERS_CACHE = None
_TAILSCALE_CACHE_TIME = 0

def _get_tailscale_status():
    """获取 Tailscale 网络状态：各节点 IP、连接方式（直连/relay/DERP）"""
    global _TAILSCALE_PEERS_CACHE, _TAILSCALE_CACHE_TIME
    now = datetime.now().timestamp()
    if _TAILSCALE_PEERS_CACHE and now - _TAILSCALE_CACHE_TIME < 15:
        return _TAILSCALE_PEERS_CACHE
    peers = {"tp": {"ip": "", "via": "unknown"}, "mac": {"ip": "", "via": "unknown"}, "vps": {"ip": "", "via": "unknown"}}
    try:
        result = subprocess.run(["tailscale", "status"], capture_output=True, text=True, timeout=10)
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) < 2:
                continue
            ip = parts[0]
            host = parts[1].lower()
            extra = " ".join(parts[2:]) if len(parts) > 2 else ""
            if "zzskytpg3" in host or host == "zzskytpg3":
                peers["tp"]["ip"] = ip
                peers["tp"]["via"] = "direct"
            elif "zzsky-mbp" in host:
                peers["mac"]["ip"] = ip
                if "relay" in extra:
                    import re
                    m = re.search(r'relay\s+"([^"]+)"', extra)
                    peers["mac"]["via"] = f'relay({m.group(1)})' if m else "relay"
                else:
                    peers["mac"]["via"] = "direct"
            elif "vps" in host:
                peers["vps"]["ip"] = ip
                peers["vps"]["via"] = "direct"
    except Exception:
        pass
    _TAILSCALE_PEERS_CACHE = peers
    _TAILSCALE_CACHE_TIME = now
    return peers

def _query(sql, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=3)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def _query_one(sql, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=3)
    row = conn.execute(sql, params).fetchone()
    conn.close()
    return row[0] if row else 0

def _execute(sql, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=3)
    conn.execute(sql, params)
    conn.commit()
    conn.close()

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

def _normalize_name(name):
    if not name:
        return "?"
    n = name.strip().lower()
    # Relay
    if any(r in n for r in RELAY_NAMES) or "vps" in n or "100.109.129" in n:
        return "relay"
    # Mac
    if any(x in n for x in ("zzsky-mbp", "zzsky-mac", "mac-弟弟", "mac-小钉")) or n in ("mac", "100.81.192.38"):
        return "mac"
    # TP
    if any(x in n for x in ("zzskytpg3", "zzskytpg", "zzskytp", "tp-哥哥", "tp-小钉", "tp-thinkpad")) or n in ("tp", "100.102.54.90", "localhost", "127.0.0.1", "owner"):
        return "tp"
    if "tp" in n:
        return "tp"
    # Known unknowns
    if n in ("unknown", "?", ""):
        return "?"
    # Fallback: return short safe label
    return n[:8]

def _route_str_full(from_name, to_name, via_relay):
    if via_relay:
        return f"{from_name} → relay → {to_name}"
    return f"{from_name} → {to_name}"

def _direction(from_name, to_name):
    if from_name == "tp" and to_name in ("mac", "relay"):
        return "tp-to-mac"
    if from_name == "mac" and to_name in ("tp", "relay"):
        return "mac-to-tp"
    if from_name == "relay" and to_name == "tp":
        return "mac-to-tp"
    if from_name == "relay" and to_name == "mac":
        return "tp-to-mac"
    return "other"

def _collect_messages(no_peer=False):
    results = []
    in_msgs = _query("SELECT id, sender, message, received_at AS time, type, acked FROM messages ORDER BY received_at ASC")
    for m in in_msgs:
        frm = _normalize_name(m["sender"])
        to = LOCAL_NAME
        if frm == to:
            continue
        via_relay = frm == "relay"
        route = f"{PEER_NAME} → relay → {to}" if via_relay else _route_str_full(frm, to, False)
        results.append({"id": m["id"], "dir": "in", "from": frm, "to": to, "route": route, "via_relay": via_relay, "time": m["time"], "time_bj": _fmt_bj(m["time"]), "type": m["type"] or "info", "message": m["message"], "source": "local", "direction": _direction(frm, to)})

    out_msgs = _query("SELECT id, target_host, sender, message, sent_at AS time, acked FROM outgoing_messages ORDER BY sent_at ASC")
    for m in out_msgs:
        frm = LOCAL_NAME
        to_raw = m["target_host"].lower()
        if any(r in to_raw for r in RELAY_NAMES):
            to = "relay"
            route = f"{frm} → relay → {PEER_NAME}"
            via_relay = True
        else:
            to = _normalize_name(m["target_host"])
            route = _route_str_full(frm, to, False)
            via_relay = False
        if to == frm:
            continue
        results.append({"id": m["id"], "dir": "out", "from": frm, "to": to, "route": route, "via_relay": via_relay, "time": m["time"], "time_bj": _fmt_bj(m["time"]), "type": "info", "message": m["message"], "source": "local", "direction": _direction(frm, to)})

    if PEER_WEB_URL and not no_peer:
        try:
            req = urllib.request.Request(f"{PEER_WEB_URL}/api/messages?no_peer=1")
            with urllib.request.urlopen(req, timeout=5) as resp:
                peer_data = json.loads(resp.read().decode())
            local_keys = {(r["message"][:100], r["time"]) for r in results}
            for pm in peer_data.get("messages", []):
                key = (pm["message"][:100], pm["time"])
                if key in local_keys or pm.get("from") == pm.get("to"):
                    continue
                pm["source"] = "peer"
                # 对 peer 消息重新 normalize（Mac 可能跑的旧代码）
                pm["from"] = _normalize_name(pm.get("from", ""))
                pm["to"] = _normalize_name(pm.get("to", ""))
                pm["route"] = _route_str_full(pm["from"], pm["to"], pm.get("via_relay", False))
                pm["direction"] = _direction(pm["from"], pm["to"])
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
    no_peer = request.args.get("no_peer", "0") == "1"
    try:
        all_msgs = _collect_messages(no_peer=no_peer)
        total = len(all_msgs)
        display = all_msgs[-100:]
        return jsonify({"messages": display, "count": len(display), "total": total, "hostname": HOSTNAME, "local_name": LOCAL_NAME})
    except Exception as e:
        return jsonify({"error": str(e), "messages": []})

@app.route("/api/status")
def get_status():
    ts = _get_tailscale_status()
    data = {
        "hostname": HOSTNAME,
        "local_name": LOCAL_NAME,
        "tailscale": ts,
    }
    return jsonify(data)

@app.route("/api/clear", methods=["POST"])
def clear_messages():
    try:
        _execute("DELETE FROM messages")
        _execute("DELETE FROM outgoing_messages")
        _execute("VACUUM")
        return jsonify({"status": "ok", "message": "所有消息已清空"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/health")
def health():
    try:
        _query("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return jsonify({"hostname": HOSTNAME, "local_name": LOCAL_NAME, "tether_web": "ok", "database": "ok" if db_ok else "error"})

def main():
    print(f"🌐 Tether Web v3 — http://0.0.0.0:{PORT}  (local={LOCAL_NAME})")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()


