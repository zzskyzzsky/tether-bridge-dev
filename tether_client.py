#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether CLI — Hermes 实例间通信客户端

用法:
  tether ping <host>                    # 测试连通性
  tether status <host>                  # 查对方状态
  tether msg <host> <text>              # 发消息（纯文本或结构化）
  tether msgs <host>                    # 拉取待处理消息（并清空）
  tether task <host> <command>          # 异步提交任务
  tether exec <host> <command>          # 同步执行命令并返回结果
"""
import json
import os
import socket
import subprocess
import sys
import urllib.request
import urllib.error

DEFAULT_PORT = 9001
AUTH_TOKEN = os.environ.get("TETHER_TOKEN", "")

# 主机短名映射（用于结构化消息的 from 字段）
_HOST_SHORT_MAP = {
    "zzskytpg3": "tp",
    "zzsky-mbp": "mac",
    "zzskyTPG3": "tp",
}
def _host_short() -> str:
    hn = socket.gethostname().lower()
    return _HOST_SHORT_MAP.get(hn, hn)

def _get_tailscale_ip() -> str:
    """获取本机 Tailscale IP（用于 sender_ip 字段）"""
    try:
        r = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            return r.stdout.strip()
    except:
        pass
    return ""

def _url(host, path):
    if ":" in host:
        # IP:port 格式
        return f"http://{host}{path}"
    # bare IP 或 hostname
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
    """发送消息，支持结构化格式

    用法:
      tether msg <host> <text>                                    # 纯文本（旧格式，向后兼容）
      tether msg <host> --type ack <text>                         # 结构化：指定类型
      tether msg <host> --template review_request <text>          # 结构化：指定模板
      tether msg <host> --type request --template code_review <text>
      tether msg <host> --llm false <text>                        # 不送 LLM 处理
      tether msg <host> --priority high <text>                    # 高优先（v2 协议）

    类型说明:
      ack     — 对之前消息的确认/回复
      info    — 信息性消息
      notify  — 普通通知（默认）
      request — 请求对方执行操作

    优先级:
      normal  — 普通（默认）
      high    — 高优先级（优先处理）
    """
    if len(args) < 2:
        print(cmd_msg.__doc__)
        return 1

    msg_type = "notify"
    msg_template = ""
    msg_priority = "normal"
    msg_llm = True
    text_parts = []
    i = 0
    while i < len(args):
        if args[i] == "--type" and i + 1 < len(args):
            msg_type = args[i + 1]
            i += 2
        elif args[i] == "--template" and i + 1 < len(args):
            msg_template = args[i + 1]
            i += 2
        elif args[i] == "--priority" and i + 1 < len(args):
            msg_priority = args[i + 1].lower()
            if msg_priority not in ("normal", "high"):
                print(f"⚠️ 无效优先级: {msg_priority}，使用默认值 normal")
                msg_priority = "normal"
            i += 2
        elif args[i] == "--llm" and i + 1 < len(args):
            raw = args[i + 1].lower()
            msg_llm = raw in ("true", "yes", "1")
            i += 2
        else:
            text_parts.append(args[i])
            i += 1

    host = text_parts[0]
    text = " ".join(text_parts[1:])

    if not text:
        print(cmd_msg.__doc__)
        return 1

    use_structured = (msg_type != "notify" or msg_template or not msg_llm or msg_priority != "normal")

    if use_structured:
        # v2 协议：含 sender_ip（回复路由）和 priority（队列优先级）
        payload = {
            "from": socket.gethostname(),
            "message": json.dumps({
                "version": 2,
                "from": _host_short(),
                "sender_ip": _get_tailscale_ip(),
                "type": msg_type,
                "priority": msg_priority,
                "llm": msg_llm,
                "template": msg_template,
                "message": text,
            })
        }
    else:
        payload = {"from": socket.gethostname(), "message": text}

    r = _post(host, "/message", payload)
    if r.get("status") == "ok":
        label = ""
        if use_structured:
            label = f" [type={msg_type}"
            if msg_template:
                label += f", template={msg_template}"
            if not msg_llm:
                label += f", llm=false"
            if msg_priority != "normal":
                label += f", priority={msg_priority}"
            label += "]"
        print(f"✅ 消息已发送 (id={r['message_id']}){label}")
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
    # 等待完成
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
            print(f"\n{'='*40}")
            print(f"状态: {s['status']}")
            res = s.get("result", {})
            if res.get("stdout"):
                print(res["stdout"])
            if res.get("stderr"):
                print(f"[stderr]\n{res['stderr']}", file=sys.stderr)
            print(f"耗时: {res.get('duration_seconds', 0)}s")
            print(f"退出码: {res.get('exit_code', -1)}")
            return 0 if res.get("exit_code") == 0 else 1
    print("⌛ 超时（120s）")
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
