#!/bin/bash
# Tether Bridge — MacBook 一键部署脚本
# 在 MacBook 上执行，完成 Tether 服务安装配置
# 用法: bash setup_tether_macbook.sh

set -e

echo "🌉 Tether Bridge — MacBook 部署"
echo "================================"

# 1. 创建目录
mkdir -p ~/.hermes/tether/venv

# 2. 创建 Python venv 并安装 Flask
echo ""
echo "📦 [1/5] 安装 Flask..."
python3 -m venv ~/.hermes/tether/venv
~/.hermes/tether/venv/bin/pip install flask -q
echo "    ✅ Flask OK"

# 3. 写入 tether_server.py
echo ""
echo "📝 [2/5] 写入服务端代码..."
cat > ~/.hermes/tether/tether_server.py << 'PYEOF_SERVER'
#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Bridge — Hermes 实例间通信服务
在 Tailscale IP:9001 上监听，提供消息传递和任务执行 API。
每台机器跑一份，互通。
"""
import json
import os
import socket
import subprocess
import threading
import time
import uuid
import argparse
from datetime import datetime, timezone

from flask import Flask, request, jsonify

app = Flask(__name__)

# ============================================================
# 配置
# ============================================================
HOSTNAME = socket.gethostname()
TAILSCALE_IP = None
LISTEN_PORT = 9001
AUTH_TOKEN = None  # 可选，设了就要匹配才响应

# 内存存储：收到的消息 / 任务结果
received_messages = []
tasks = {}  # task_id -> {"status", "result", "created_at", "finished_at"}
tasks_lock = threading.Lock()

# ============================================================
# 认证装饰器（可选）
# ============================================================
def require_auth(f):
    def wrapper(*args, **kwargs):
        if AUTH_TOKEN:
            token = request.headers.get("X-Tether-Token", "")
            if token != AUTH_TOKEN:
                return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

# ============================================================
# API
# ============================================================

@app.route("/ping", methods=["GET"])
@require_auth
def ping():
    return jsonify({"pong": True, "hostname": HOSTNAME, "time": _now_iso()})

@app.route("/status", methods=["GET"])
@require_auth
def status():
    try:
        load_avg = os.getloadavg()
    except OSError:
        load_avg = [0, 0, 0]
    return jsonify({
        "hostname": HOSTNAME,
        "tailscale_ip": TAILSCALE_IP,
        "uptime": _uptime(),
        "load_avg": list(load_avg),
        "messages_pending": len(received_messages),
        "tasks_in_flight": sum(1 for t in tasks.values() if t["status"] in ("running", "pending")),
        "time": _now_iso(),
    })

@app.route("/message", methods=["POST"])
@require_auth
def receive_message():
    data = request.get_json(silent=True) or {}
    msg = {
        "id": str(uuid.uuid4()),
        "from": data.get("from", "unknown"),
        "message": data.get("message", ""),
        "in_reply_to": data.get("in_reply_to"),
        "received_at": _now_iso(),
    }
    received_messages.append(msg)
    print(f"[tether] 收到消息 from={msg['from']}: {msg['message'][:80]}")
    return jsonify({"status": "ok", "message_id": msg["id"]})

@app.route("/messages", methods=["GET"])
@require_auth
def get_messages():
    """获取所有待处理消息，可选择清空"""
    clear = request.args.get("clear", "0") == "1"
    msgs = list(received_messages)
    if clear:
        received_messages.clear()
    return jsonify({"messages": msgs, "count": len(msgs)})

@app.route("/task", methods=["POST"])
@require_auth
def submit_task():
    """提交一个 shell 命令任务，异步执行"""
    data = request.get_json(silent=True) or {}
    task_id = str(uuid.uuid4())
    command = data.get("command", "")
    workdir = data.get("workdir")
    timeout = data.get("timeout", 300)

    if not command:
        return jsonify({"error": "command is required"}), 400

    task = {
        "status": "pending",
        "command": command,
        "workdir": workdir,
        "timeout": timeout,
        "created_at": _now_iso(),
        "finished_at": None,
        "result": None,
    }
    with tasks_lock:
        tasks[task_id] = task

    # 异步执行
    t = threading.Thread(target=_run_task, args=(task_id,), daemon=True)
    t.start()

    return jsonify({"status": "accepted", "task_id": task_id})

@app.route("/task/<task_id>", methods=["GET"])
@require_auth
def get_task(task_id):
    with tasks_lock:
        task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "task not found"}), 404
    return jsonify({
        "task_id": task_id,
        "status": task["status"],
        "command": task["command"],
        "created_at": task["created_at"],
        "finished_at": task["finished_at"],
        "result": task["result"],
    })

@app.route("/task", methods=["PUT"])
@require_auth
def wait_task():
    """同步模式：提交任务并等待完成（最长 timeout 秒）"""
    data = request.get_json(silent=True) or {}
    command = data.get("command", "")
    workdir = data.get("workdir")
    timeout = data.get("timeout", 300)

    if not command:
        return jsonify({"error": "command is required"}), 400

    task_id = str(uuid.uuid4())
    task = {
        "status": "running",
        "command": command,
        "workdir": workdir,
        "timeout": timeout,
        "created_at": _now_iso(),
        "finished_at": None,
        "result": None,
    }
    with tasks_lock:
        tasks[task_id] = task

    _run_task_sync(task_id, command, workdir, timeout)

    with tasks_lock:
        result = dict(tasks[task_id])

    return jsonify({
        "task_id": task_id,
        "status": result["status"],
        "result": result["result"],
    })

# ============================================================
# 内部
# ============================================================

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _uptime():
    try:
        with open("/proc/uptime") as f:
            return float(f.read().split()[0])
    except Exception:
        return 0

def _run_task(task_id):
    with tasks_lock:
        task = tasks[task_id]
        task["status"] = "running"
        cmd = task["command"]
        workdir = task["workdir"]
        timeout = task["timeout"]

    print(f"[tether] 开始执行任务 {task_id}: {cmd[:80]}")
    result = _execute(cmd, workdir, timeout)

    with tasks_lock:
        task["status"] = "completed" if result["exit_code"] == 0 else "failed"
        task["finished_at"] = _now_iso()
        task["result"] = result

    status = "完成" if result["exit_code"] == 0 else f"失败(exit={result['exit_code']})"
    print(f"[tether] 任务 {task_id} {status} ({result.get('duration_seconds', 0):.1f}s)")

def _run_task_sync(task_id, command, workdir, timeout):
    print(f"[tether] 同步执行任务 {task_id}: {command[:80]}")
    result = _execute(command, workdir, timeout)
    with tasks_lock:
        tasks[task_id]["status"] = "completed" if result["exit_code"] == 0 else "failed"
        tasks[task_id]["finished_at"] = _now_iso()
        tasks[task_id]["result"] = result

def _execute(command, workdir, timeout):
    start = time.time()
    try:
        r = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            cwd=workdir,
            timeout=timeout,
        )
        exit_code = r.returncode
        stdout = r.stdout[:50000]  # 截断防止过大
        stderr = r.stderr[:10000]
    except subprocess.TimeoutExpired:
        exit_code = -1
        stdout = ""
        stderr = f"[tether] 任务超时（{timeout}s）"
    except Exception as e:
        exit_code = -2
        stdout = ""
        stderr = str(e)

    duration = time.time() - start
    return {
        "exit_code": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "duration_seconds": round(duration, 2),
    }

# ============================================================
# 启动
# ============================================================

def _resolve_tailscale_ip():
    """尝试获取 Tailscale IP"""
    try:
        r = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            return r.stdout.strip()
    except Exception:
        pass
    # fallback: 解析 hostname.tailscale.network
    try:
        ip = socket.gethostbyname(f"{HOSTNAME}.tailscale.network")
        if ip:
            return ip
    except Exception:
        pass
    return "0.0.0.0"

def main():
    global TAILSCALE_IP, LISTEN_PORT, AUTH_TOKEN

    parser = argparse.ArgumentParser(description="Tether Bridge — Hermes 实例间通信")
    parser.add_argument("--port", type=int, default=LISTEN_PORT, help=f"监听端口（默认 {LISTEN_PORT}）")
    parser.add_argument("--bind", default=None, help="绑定地址（默认自动检测 Tailscale IP）")
    parser.add_argument("--token", default=os.environ.get("TETHER_TOKEN"), help="认证 token（可选）")
    parser.add_argument("--debug", action="store_true", help="调试模式")
    args = parser.parse_args()

    LISTEN_PORT = args.port
    AUTH_TOKEN = args.token

    if args.bind:
        bind_addr = args.bind
    else:
        tailscale_ip = _resolve_tailscale_ip()
        TAILSCALE_IP = tailscale_ip
        bind_addr = tailscale_ip

    print(f"🌉 Tether Bridge — {HOSTNAME}")
    print(f"   监听: {bind_addr}:{LISTEN_PORT}")
    auth_status = "启用" if AUTH_TOKEN else "未设置（不推荐）"
    print(f"   认证: {auth_status}")
    print(f"   Tailscale IP: {TAILSCALE_IP}")

    app.run(host=bind_addr, port=LISTEN_PORT, debug=args.debug, use_reloader=False)

if __name__ == "__main__":
    main()
PYEOF_SERVER
echo "    ✅ tether_server.py"

# 4. 写入 tether_client.py
echo ""
echo "📝 [3/5] 写入客户端代码..."
cat > ~/.hermes/tether/tether_client.py << 'PYEOF_CLIENT'
#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether CLI — Hermes 实例间通信客户端

用法:
  tether ping <host>
  tether status <host>
  tether msg <host> <text>
  tether msgs <host>
  tether task <host> <command>
  tether exec <host> <command>
"""
import json
import os
import socket
import sys
import urllib.request
import urllib.error

DEFAULT_PORT = 9001
AUTH_TOKEN = os.environ.get("TETHER_TOKEN", "")

def _url(host, path):
    if ":" in host:
        return f"http://{host}{path}"
    return f"http://{host}:{DEFAULT_PORT}{path}"

def _headers():
    h = {"Content-Type": "application/json"}
    if AUTH_TOKEN:
        h["X-Tether-Token"] = AUTH_TOKEN
    return h

def _get(host, path):
    req = urllib.request.Request(_url(host, path), headers=_headers(), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "body": e.read().decode()[:200]}
    except urllib.error.URLError as e:
        return {"error": f"连接失败: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}

def _post(host, path, data=None):
    body = json.dumps(data or {}).encode()
    req = urllib.request.Request(_url(host, path), data=body, headers=_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "body": e.read().decode()[:200]}
    except urllib.error.URLError as e:
        return {"error": f"连接失败: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}

def cmd_ping(args):
    if not args:
        print("用法: tether ping <host>")
        return 1
    r = _get(args[0], "/ping")
    if r.get("pong"):
        print(f"✅ {r['hostname']} — {r['time']}")
    else:
        print(f"❌ {r}")
    return 0

def cmd_status(args):
    if not args:
        print("用法: tether status <host>")
        return 1
    r = _get(args[0], "/status")
    if "hostname" in r:
        print(f"主机:   {r['hostname']}")
        print(f"IP:     {r.get('tailscale_ip', '?')}")
        print(f"运行:   {r.get('uptime', 0):.0f}s")
        print(f"负载:   {', '.join(f'{x:.2f}' for x in r.get('load_avg', ['?']))}")
        print(f"待处理消息: {r.get('messages_pending', 0)}")
        print(f"执行中任务: {r.get('tasks_in_flight', 0)}")
    else:
        print(f"❌ {r}")
    return 0

def cmd_msg(args):
    if len(args) < 2:
        print("用法: tether msg <host> <text>")
        return 1
    host = args[0]
    text = " ".join(args[1:])
    r = _post(host, "/message", {"from": socket.gethostname(), "message": text})
    if r.get("status") == "ok":
        print(f"✅ 消息已发送 (id={r['message_id']})")
    else:
        print(f"❌ {r}")
    return 0

def cmd_msgs(args):
    if not args:
        print("用法: tether msgs <host>")
        return 1
    clear = "--keep" not in args
    r = _get(args[0], f"/messages?clear={'1' if clear else '0'}")
    if "messages" in r:
        if r["count"] == 0:
            print("📭 没有待处理消息")
        else:
            print(f"📬 {r['count']} 条待处理消息:")
            for m in r["messages"]:
                print(f"  [{m['id'][:8]}] from={m['from']}: {m['message']}")
    else:
        print(f"❌ {r}")
    return 0

def cmd_task(args):
    if len(args) < 2:
        print("用法: tether task <host> <command>")
        return 1
    host = args[0]
    cmd = " ".join(args[1:])
    r = _post(host, "/task", {"command": cmd, "timeout": 300})
    if r.get("status") == "accepted":
        print(f"✅ 任务已提交 (task_id={r['task_id']})")
    else:
        print(f"❌ {r}")
    return 0

def cmd_exec(args):
    if len(args) < 2:
        print("用法: tether exec <host> <command>")
        return 1
    host = args[0]
    cmd = " ".join(args[1:])
    r = _post(host, "/task", {"command": cmd, "timeout": 300})
    if r.get("status") != "accepted":
        print(f"❌ {r}")
        return 1
    task_id = r["task_id"]
    print(f"⏳ 等待任务完成 (task_id={task_id})...")
    import time
    for _ in range(60):
        time.sleep(2)
        s = _get(host, f"/task/{task_id}")
        if s.get("status") in ("completed", "failed"):
            stdout = s.get("result", {}).get("stdout", "")
            stderr = s.get("result", {}).get("stderr", "")
            exit_code = s.get("result", {}).get("exit_code", -1)
            duration = s.get("result", {}).get("duration_seconds", 0)
            if stdout:
                print(stdout)
            if stderr:
                print(f"[stderr]\n{stderr}", file=sys.stderr)
            print(f"耗时: {duration}s | 退出码: {exit_code}")
            return 0 if exit_code == 0 else 1
    print("⌛ 超时")
    return 1

def main():
    if len(sys.argv) < 3:
        print(__doc__.strip())
        return 1

    cmd = sys.argv[1]
    args = sys.argv[2:]

    cmds = {
        "ping": cmd_ping,
        "status": cmd_status,
        "msg": cmd_msg,
        "msgs": cmd_msgs,
        "task": cmd_task,
        "exec": cmd_exec,
    }

    handler = cmds.get(cmd)
    if not handler:
        print(f"未知命令: {cmd}")
        print("可用: ping, status, msg, msgs, task, exec")
        return 1

    return handler(args)

if __name__ == "__main__":
    sys.exit(main())
PYEOF_CLIENT
echo "    ✅ tether_client.py"

# 5. 创建 CLI symlink and set permissions
echo ""
echo "🔗 [4/5] 配置 CLI 命令..."
chmod +x ~/.hermes/tether/tether_server.py ~/.hermes/tether/tether_client.py
ln -sf ~/.hermes/tether/tether_client.py ~/.local/bin/tether
echo "    ✅ tether 命令已安装"

# 6. 设置认证 Token
echo ""
echo "🔑 [5/5] 配置认证 Token..."
if ! grep -q "TETHER_TOKEN" ~/.hermes/.env 2>/dev/null; then
    echo "TETHER_TOKEN=6c84e624e6cae2b760b4d4ed7bd7f2f8" >> ~/.hermes/.env
    echo "    ✅ Token 已添加到 ~/.hermes/.env"
else
    echo "    ⏭️  Token 已存在，跳过"
fi

# 7. 创建 systemd 服务
echo ""
echo "⚙️  可选的：安装 systemd 服务（需 sudo）"
echo "    执行以下命令："
echo ""
echo "    sudo tee /etc/systemd/system/tether.service > /dev/null << 'EOF'"
echo "[Unit]"
echo "Description=Tether Bridge — Hermes 实例间通信"
echo "After=network-online.target tailscaled.service"
echo "Wants=network-online.target"
echo ""
echo "[Service]"
echo "Type=simple"
echo "User=$(whoami)"
echo "ExecStart=/home/$(whoami)/.hermes/tether/tether_server.py --port 9001"
echo "Restart=always"
echo "RestartSec=5"
echo "Environment=TETHER_TOKEN=6c84e624e6cae2b760b4d4ed7bd7f2f8"
echo "StandardOutput=journal"
echo "StandardError=journal"
echo ""
echo "[Install]"
echo "WantedBy=multi-user.target"
echo "EOF"
echo ""
echo "    sudo systemctl daemon-reload"
echo "    sudo systemctl enable tether.service"
echo "    sudo systemctl start tether.service"
echo "    sudo systemctl status tether.service"
echo ""

# 8. 测试
echo ""
echo "🧪 测试 Tether..."
~/.hermes/tether/tether_server.py --bind 127.0.0.1 --port 19001 &
TETHER_PID=$!
sleep 2
TETHER_TOKEN=6c84e624e6cae2b760b4d4ed7bd7f2f8 ~/.hermes/tether/tether_client.py ping 127.0.0.1:19001 2>&1 || echo "    ⚠️ 测试失败，请检查"
kill $TETHER_PID 2>/dev/null

echo ""
echo "✅ Tether 部署完成！"
echo ""
echo "📋 下一步："
echo "  1. 安装 systemd 服务（见上方命令）"
echo "  2. 测试连通 ThinkPad: tether ping 100.102.54.90"
echo "  3. 现在向 ThinkPad 发消息: tether msg 100.102.54.90 \"MacBook 已就绪\""
