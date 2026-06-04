#!/home/zzsky/.hermes/tether/venv/bin/python3
"""
Tether Bridge v2 — Hermes 实例间通信服务
SQLite 持久化 + ACK 确认机制，重启不丢消息

v2.2: 串行队列 + 自动回复转发 — worker 处理完自动将输出 POST 回发件方
v2.3: --gateway / --no-gateway 参数区分实例，mac 走 Gateway API Server 加速
"""
import json, os, socket, sqlite3, subprocess, threading, time, uuid, argparse
import urllib.request, urllib.error
from datetime import datetime, timezone
from queue import PriorityQueue, Empty
from flask import Flask, request, jsonify

app = Flask(__name__)

# 配置
HOSTNAME = socket.gethostname()
TAILSCALE_IP = None
LISTEN_PORT = 9001
AUTH_TOKEN = None  # 从环境变量或参数读取
DB_PATH = None
_use_gateway = True  # 默认启用 Gateway，tp 用 --no-gateway 关闭

# ===== 通知文件机制 (Phase 5) =====
# 收到消息时写一个 flag 文件到 /tmp/，便于本地客户端/inotify 感知新消息，
# 替代纯 API 轮询。客户端可用 inotifywait -m /tmp/tether_notify.json 监听。
NOTIFY_FILE = "/tmp/tether_notify.json"

def _write_notify_file(msg_id, sender, message_preview, pending_count):
    """写通知 flag 文件：包含最新消息摘要和待处理数量"""
    import json as _json
    try:
        data = {
            "new_messages": True,
            "timestamp": _now_iso(),
            "message_id": msg_id[:12],
            "sender": sender,
            "preview": message_preview[:80],
            "pending_count": pending_count,
        }
        with open(NOTIFY_FILE, "w") as f:
            _json.dump(data, f)
    except Exception as e:
        print(f"[tether] ⚠️ 通知文件写入失败: {e}")

# ===== 实时消息处理队列 =====
# 优先级队列：priority=(0=high, 1=normal)，串行处理
_message_queue = PriorityQueue()
_worker_thread = None
_own_tether_port = 9001
_own_auth_token = None
# 优先级队列 tiebreaker 计数器（防 dict 比较崩溃）
_queue_counter = 0

# ===== Phase 3: 协议模板 =====
# 消息类型：ack / info / discuss / task
# llm=false 的消息走模板快速回复，不进队列
_PROTOCOL_TEMPLATES = {
    # ACK 类（纯确认，无内容）
    "ack_received": "✅ 收到",
    "ack_understood": "✅ 收到，已了解",
    "ack_agree": "✅ 收到，同意",
    "ack_done": "✅ 收到，已完成",
    "ack_waiting": "⏳ 收到，等待中",
    "ack_negative": "❌ 不同意",
    # Info 类（状态通知）
    "info_status": None,       # 转发原始 message
    "info_complete": None,     # 转发原始 message
    "info_error": None,        # ⚠️ + 原始 message
    # 请求类
    "request/code_review": None,   # 需 LLM
    # 通知类
    "notify/status_change": None,  # 转发原始 message
}

def _generate_template_reply(template_name, original_message=""):
    """根据模板名生成回复文本。llm=false 时直接返回，零 LLM 开销。"""
    reply = _PROTOCOL_TEMPLATES.get(template_name)
    if reply is not None:
        return reply
    if template_name in ("info_status", "info_complete", "notify/status_change"):
        return original_message
    if template_name == "info_error":
        return f"⚠️ {original_message}" if original_message else "⚠️ 出错了"
    return None  # 未知模板，需要 LLM

def _is_ack_message(content):
    """检测消息是否为结构化 ack/info（llm=false），可直接模板回复。"""
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict) and parsed.get("type") in ("ack", "info"):
            if parsed.get("llm") is False or parsed.get("type") == "ack":
                return True, parsed.get("template", ""), parsed.get("message", "")
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    return False, "", ""

def _forward_reply(target_ip, reply_text, is_auto=True):
    """将 worker 处理后的回复自动转发回发件方的 Tether 服务器
    is_auto=True  → sender=HOSTNAME-auto（ACK/模板回复 → guard 丢弃）
    is_auto=False → sender=HOSTNAME（实际输出 → guard 放行）
    """
    if not target_ip or not reply_text:
        return False
    # 本地消息不转发（防止回环）
    if target_ip == TAILSCALE_IP:
        return False
    url = f"http://{target_ip}:{_own_tether_port}/message"
    sender = HOSTNAME + ("-auto" if is_auto else "")
    payload = json.dumps({
        "from": sender,
        "message": reply_text[:2000],
    }).encode()
    headers = {"Content-Type": "application/json"}
    if _own_auth_token:
        headers["X-Tether-Token"] = _own_auth_token
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        urllib.request.urlopen(req, timeout=10)
        print(f"[tether] ✅ 自动转发回复到 {target_ip}:{_own_tether_port} ({len(reply_text)} chars)")
        return True
    except Exception as e:
        print(f"[tether] ⚠️ 自动转发失败: {e}")
        return False

def _process_message_worker():
    """Worker 线程：从优先级队列取出消息，POST 到 /api/process 处理，串行执行"""
    print(f"[tether] 消息处理 worker 已启动（方案6：HTTP POST /api/process，优先级队列）")
    
    while True:
        try:
            priority_num, _, msg_data = _message_queue.get()
        except Empty:
            continue
        except Exception as e:
            print(f"[tether] worker 循环异常: {e}")
            continue
        
        try:
            if msg_data is None:  # 哨兵值，用于优雅关闭
                break
            
            msg_id = msg_data['id']
            sender = msg_data['sender']
            content = msg_data['message']
            reply_to_addr = msg_data.get('reply_to_addr')
            
            print(f"[tether] ▶ 处理消息 {msg_id[:8]} from={sender}: {content[:60]}")
            
            if _use_gateway:
                # v4: 走 Gateway API（复用 session，保持 prefix cache）
                prompt = (
                    f"[Agent-to-Agent] 来自 {sender}（Tether 通信桥 - Gateway）[v2]：\n"
                    f"{content}\n\n"
                    f"=== 协议版本说明 ===\n"
                    f"本消息协议版本: v2 | 我方支持: v2\n"
                    f"如需回复对方，请在消息内容开头加版本标记\n"
                    f"例如: v2|回复内容\n\n"
                    f"这是另一台 Hermes agent 通过 Tether 发来的消息，不是普通聊天。\n"
                    f"请根据内容判断需要做什么：\n"
                    f"1) 如果需要执行任务，使用 terminal 等工具完成\n"
                    f"2) 如果需要回复对方，直接输出回复内容，系统会自动转发\n"
                    f"3) 如果是通知需要主人知道，请在下次与主人交流时提及"
                )
                gateway_output, gateway_err = _gateway_chat(prompt, timeout=300)
                if gateway_err is None and gateway_output is not None:
                    has_output = bool(gateway_output.strip())
                    summary = f"{len(gateway_output)} chars" if has_output else "无输出"
                    print(f"[tether] ✅ 消息 {msg_id[:8]} 处理完成 (Gateway, {summary})")
                    _ack_incoming(msg_id)
                    if has_output and reply_to_addr:
                        _forward_reply(reply_to_addr, gateway_output.strip(), is_auto=False)
                else:
                    print(f"[tether] ⚠️ 消息 {msg_id[:8]} Gateway 失败 ({gateway_err}), 尝试子进程回退")
                    _process_fallback(msg_id, sender, content, reply_to_addr)
            else:
                # tp 模式：直接走子进程，不走 Gateway
                print(f"[tether] ▶ 消息 {msg_id[:8]} 直接走子进程 (Gateway 未启用)")
                _process_fallback(msg_id, sender, content, reply_to_addr)
                    
        except Exception as e:
            print(f"[tether] worker 循环异常: {e}")

def _process_fallback(msg_id, sender, content, reply_to_addr):
    """回退方案：直接调 hermes CLI 子进程（当 Gateway 不可用或未启用时）"""
    hermes_cmd = _find_hermes_cli()
    if not hermes_cmd:
        print(f"[tether] ❌ 回退失败：找不到 hermes CLI")
        return
    
    # === Phase 4 兼容层: 解析结构化消息（v1/v2），与 /api/process 保持一致 ===
    PROTOCOL_VERSION = 2
    msg_type = "discuss"
    msg_template = ""
    msg_priority = "normal"
    incoming_version = 1
    inner_message = content
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict) and "type" in parsed:
            incoming_version = parsed.get("version", 2)
            raw_type = parsed.get("type", "info")
            msg_type = "info" if raw_type == "notify" else raw_type
            msg_template = parsed.get("template", "")
            msg_priority = parsed.get("priority", "normal")
            inner_message = parsed.get("content") or parsed.get("message") or content
    except (json.JSONDecodeError, TypeError):
        pass

    msg_header = f"[Agent-to-Agent] 来自 {sender}（Tether 通信桥 - 回退）"
    incoming_ver_str = f"v{incoming_version}"
    msg_header += f" [{incoming_ver_str}]"
    if msg_type and msg_template:
        msg_header += f" [type={msg_type}, template={msg_template}]"
    if msg_priority == "high":
        msg_header += " [priority=high]"

    ack_note = ""
    if msg_type == "ack":
        ack_note = "\n\n=== 这是对之前消息的确认/回复 ===\n"

    version_note = (
        "\n\n"
        f"=== 协议版本说明 ===\n"
        f"本消息协议版本: v{incoming_version} | 我方支持: v{PROTOCOL_VERSION}\n"
        f"如需回复，请在消息内容开头加版本标记（如 v{PROTOCOL_VERSION}|回复）\n"
    ) if incoming_version >= 2 else ""

    prompt = (
        f"{msg_header}：\n"
        f"{inner_message}\n"
        f"{ack_note}"
        f"{version_note}"
        f"\n\n"
        f"这是另一台 Hermes agent 通过 Tether 发来的消息，不是普通聊天。\n"
        f"请根据内容判断需要做什么：\n"
        f"1) 如果需要执行任务，使用 terminal 等工具完成\n"
        f"2) 如果需要回复对方，直接输出回复内容，系统会自动转发\n"
        f"3) 如果是通知需要主人知道，请在下次与主人交流时提及"
    )

    if _use_gateway:
        # 先尝试 Gateway
        gateway_output, gateway_err = _gateway_chat(prompt, timeout=300)
        if gateway_err is None and gateway_output is not None:
            if gateway_output and reply_to_addr:
                _forward_reply(reply_to_addr, gateway_output, is_auto=False)
                print(f"[tether] ✅ 回退处理(Gateway)成功 ({len(gateway_output)} chars)")
            _ack_incoming(msg_id)
            return
        print(f"[tether] ⚡ 回退 Gateway 失败 ({gateway_err}), 尝试子进程")

    print(f"[tether] ⚡ 使用子进程 hermes -z 处理")
    try:
        r = subprocess.run(
            [hermes_cmd, "-z", prompt],
            capture_output=True, text=True, timeout=300
        )
        if r.returncode == 0 and r.stdout.strip() and reply_to_addr:
            _forward_reply(reply_to_addr, r.stdout.strip(), is_auto=False)
            print(f"[tether] ✅ 回退处理成功 ({len(r.stdout)} chars)")
        else:
            print(f"[tether] ⚠️ 回退处理完成但无输出或失败")
        _ack_incoming(msg_id)
    except Exception as e:
        print(f"[tether] ❌ 回退处理异常: {e}")
        _ack_incoming(msg_id)  # 即使异常，也标记为已处理，避免重放

def _replay_queue():
    """启动时扫描 SQLite 中未处理的持久化消息，重新入队。

    解决重启后 PriorityQueue（内存队列）丢失，但 SQLite 中有 orphan 消息的问题。
    """
    count = 0
    try:
        with _get_db() as conn:
            rows = conn.execute(
                "SELECT id, sender, message, received_at FROM messages WHERE acked=0 ORDER BY received_at ASC"
            ).fetchall()
        for row in rows:
            msg_id, sender, content, received_at = row
            # 尝试从 persisted message 还原 reply_to_addr（从结构化消息提取）
            reply_to_addr = None
            try:
                parsed = json.loads(content) if isinstance(content, str) and content.startswith("{") else {}
                if isinstance(parsed, dict):
                    reply_to_addr = parsed.get("sender_ip")
            except (json.JSONDecodeError, TypeError):
                pass
            global _queue_counter
            _queue_counter += 1
            _message_queue.put((1, _queue_counter, {
                "id": msg_id,
                "sender": sender,
                "message": content,
                "reply_to_addr": reply_to_addr,
            }))
            count += 1
        if count:
            print(f"[tether] ♻️ 重放 {count} 条持久化消息到处理队列")
    except Exception as e:
        print(f"[tether] ⚠️ 队列重放失败: {e}")


# Worker 健康监控
_WORKER_WATCHDOG = None
_WATCHDOG_STOP = threading.Event()


def _watchdog_loop():
    """监控 worker 健康：如果队列中有消息但长时间无变动，标记异常并尝试恢复。"""
    last_processed = 0
    stall_count = 0
    while not _WATCHDOG_STOP.is_set():
        qsize = _message_queue.qsize()
        global _queue_counter
        current = _queue_counter
        if qsize > 0 and current == last_processed:
            stall_count += 1
            if stall_count >= 12:  # ~60秒无进展
                print(f"[tether] 🚨 Worker 可能卡死（队列 {qsize} 项，{stall_count * 5}s 无进展），尝试重启")
                _WATCHDOG_STOP.set()
                _start_worker()
                return
        else:
            stall_count = 0
        last_processed = current
        _WATCHDOG_STOP.wait(timeout=5)


def _start_worker():
    """启动消息处理 worker 线程 + 队列重放 + 健康监控"""
    global _worker_thread, _WORKER_WATCHDOG, _WATCHDOG_STOP
    if _worker_thread is not None and _worker_thread.is_alive():
        print("[tether] worker 线程已在运行")
        return

    # 启动时重放持久化消息（修复重启丢队列问题）
    _replay_queue()

    _WATCHDOG_STOP = threading.Event()
    _worker_thread = threading.Thread(target=_process_message_worker, daemon=True)
    _worker_thread.start()

    # 启动健康监控
    _WORKER_WATCHDOG = threading.Thread(target=_watchdog_loop, daemon=True)
    _WORKER_WATCHDOG.start()
    print("[tether] worker + 健康监控已启动")

# ===== SQLite 持久化 =====
def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def _init_db():
    with _get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY, sender TEXT NOT NULL,
                message TEXT NOT NULL, received_at TEXT NOT NULL,
                acked INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS outgoing_messages (
                id TEXT PRIMARY KEY, target_host TEXT NOT NULL,
                sender TEXT NOT NULL, message TEXT NOT NULL,
                sent_at TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_messages_acked ON messages(acked);
            CREATE INDEX IF NOT EXISTS idx_outgoing_acked ON outgoing_messages(acked);
        """)
    print(f"[tether] 数据库已初始化: {DB_PATH}")

def _save_incoming(msg_id, sender, message, received_at):
    with _get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO messages VALUES (?,?,?,?,0)",
                     (msg_id, sender, message, received_at))

def _get_pending_messages(ack=False):
    with _get_db() as conn:
        rows = conn.execute("SELECT id,sender,message,received_at FROM messages WHERE acked=0").fetchall()
        if ack and rows:
            ids = [r["id"] for r in rows]
            conn.execute(f"UPDATE messages SET acked=1 WHERE id IN ({','.join('?' for _ in ids)})", ids)
        return [dict(r) for r in rows]

def _count_pending():
    with _get_db() as conn:
        return conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0").fetchone()[0]

def _save_outgoing(msg_id, target_host, sender, message, sent_at):
    with _get_db() as conn:
        conn.execute("INSERT OR IGNORE INTO outgoing_messages VALUES (?,?,?,?,?,0)",
                     (msg_id, target_host, sender, message, sent_at))

def _ack_outgoing(msg_id):
    with _get_db() as conn:
        conn.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (msg_id,))

def _ack_incoming(msg_id):
    """标记收到的消息已处理完毕，避免重启后 _replay_queue 重放"""
    with _get_db() as conn:
        conn.execute("UPDATE messages SET acked=1 WHERE id=?", (msg_id,))

def _count_pending_outgoing():
    with _get_db() as conn:
        return conn.execute("SELECT COUNT(*) FROM outgoing_messages WHERE acked=0").fetchone()[0]

# ===== 认证 =====
def require_auth(f):
    def wrapper(*args, **kwargs):
        if AUTH_TOKEN:
            token = request.headers.get("X-Tether-Token", "")
            if token != AUTH_TOKEN:
                return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

# ===== API — 消息（持久化 + ACK + 实时处理）=====
@app.route("/ping", methods=["GET"])
@require_auth
def ping():
    return jsonify({"pong": True, "hostname": HOSTNAME, "time": _now_iso()})

@app.route("/status", methods=["GET"])
@require_auth
def status():
    try: load_avg = list(os.getloadavg())
    except: load_avg = [0, 0, 0]
    return jsonify({
        "hostname": HOSTNAME, "tailscale_ip": TAILSCALE_IP,
        "uptime": _uptime(), "load_avg": load_avg,
        "messages_pending": _count_pending(),
        "messages_unacked_outgoing": _count_pending_outgoing(),
        "queue_size": _message_queue.qsize(),
        "time": _now_iso(),
    })

@app.route("/message", methods=["POST"])
@require_auth
def receive_message():
    """持久化到 SQLite + 实时推入处理队列，捕获发件方地址用于自动回复
    
    Phase 3 兼容层：从结构化消息提取 sender_ip（回复路由）和 priority（队列优先级）
    """
    data = request.get_json(silent=True) or {}
    msg_id = str(uuid.uuid4())
    sender = data.get("from", "unknown")
    content = data.get("message", "")
    remote_addr = request.remote_addr or data.get("reply_to_ip")

    # 静默丢弃空消息（如 Tether 回退通道的健康检查探针）
    if not content.strip():
        print(f"[tether] 🔇 静默丢弃空消息 from={sender}")
        return jsonify({"status": "ok", "message_id": msg_id, "silent_drop": True})

    # 防回声循环：*-auto 发来的消息只存不处理
    # auto 回复是 _forward_reply 产生的，处理它会触发下一轮 auto 回复 → 死循环
    if sender.endswith("-auto"):
        _save_incoming(msg_id, sender, content, _now_iso())
        print(f"[tether] 🔇 忽略 auto 回声 from={sender}: {content[:60]}")
        return jsonify({"status": "ok", "message_id": msg_id, "silent_echo": True})

    # Phase 3: 兼容层 —— 从结构化消息中提取 sender_ip 和 priority
    # v2 消息: {"version":2,"from":"tp","sender_ip":"100.102.54.90","priority":"high",...}
    # v1 消息: {"version":1,"from":"tp","type":"notify",...} — 无 sender_ip/priority
    # 旧格式: plain text — 完全无结构化
    sender_ip = None
    priority = "normal"
    payload_type = None
    payload_template = None
    payload_message = content
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            sender_ip = parsed.get("sender_ip")
            priority = parsed.get("priority", "normal")
            payload_type = parsed.get("type")
            payload_template = parsed.get("template")
            payload_message = parsed.get("message") or parsed.get("content") or content
    except (json.JSONDecodeError, TypeError):
        pass  # 旧格式纯文本，保持原样

    # 优先使用结构化消息中的 sender_ip（v2 显式指定），否则 fallback 到 TCP 连接地址
    effective_reply_to = sender_ip or remote_addr
    priority_num = 0 if priority == "high" else 1  # 0=高优先，1=普通

    # Phase 3 — Layer 1: 快速 ACK 通道
    # ack/info 类型 + llm=false → 直接模板回复，不进队列
    is_fast, ack_template, ack_content = _is_ack_message(content)
    if is_fast:
        reply = _generate_template_reply(ack_template, ack_content)
        if reply:
            # 存 SQLite + 直接转发回复（不进 worker 队列）
            _save_incoming(msg_id, sender, content, _now_iso())
            # 纯 ACK 心跳不进 notify.json，避免 inflight counter 虚高
            if payload_type != "ack":
                _write_notify_file(msg_id, sender, content, _count_pending())
            print(f"[tether] ⚡ 快速 ACK from={sender} template={ack_template}: {reply[:50]}")
            if effective_reply_to:
                _forward_reply(effective_reply_to, reply)
            return jsonify({"status": "ok", "message_id": msg_id, "fast_ack": True, "reply": reply})
        # 模板未知 → 降级到普通队列处理

    # 1. 存 SQLite（持久化保障）并标记 已确认
    #    先保存（acked=0），入队后立即标记 acked=1，防止此消息被 _replay_queue 重放
    _save_incoming(msg_id, sender, content, _now_iso())
    _write_notify_file(msg_id, sender, content, _count_pending())
    print(f"[tether] 收到消息 from={sender} addr={effective_reply_to} priority={priority}: {content[:80]}")

    # 2. 推入优先级队列（高优先在普通之前处理）
    global _queue_counter
    _queue_counter += 1
    _message_queue.put((priority_num, _queue_counter, {
        "id": msg_id,
        "sender": sender,
        "message": content,
        "reply_to_addr": effective_reply_to,
    }))

    # 3. 标记已确认：worker 已接手，重启不必重放
    with _get_db() as conn:
        conn.execute("UPDATE messages SET acked=1 WHERE id=?", (msg_id,))
    
    return jsonify({"status": "ok", "message_id": msg_id})

@app.route("/messages", methods=["GET"])
@require_auth
def get_messages():
    """获取未读消息。ack=1（默认）自动标记为已确认"""
    auto_ack = request.args.get("ack", "1") == "1"
    msgs = _get_pending_messages(ack=auto_ack)
    return jsonify({"messages": msgs, "count": len(msgs)})

@app.route("/ack", methods=["POST"])
@require_auth
def ack_messages():
    """对方确认收到我发出的消息"""
    data = request.get_json(silent=True) or {}
    msg_ids = data.get("message_ids", [])
    if not msg_ids:
        return jsonify({"error": "no message_ids"}), 400
    for mid in msg_ids:
        _ack_outgoing(mid)
    print(f"[tether] 收到确认: {len(msg_ids)} 条消息已送达")
    return jsonify({"status": "ok", "acked": len(msg_ids)})

@app.route("/pending_acks", methods=["GET"])
@require_auth
def pending_acks():
    """查看我发出但对方还没确认的消息"""
    return jsonify({"count": _count_pending_outgoing()})

# ===== API — 任务 =====
tasks = {}
tasks_lock = threading.Lock()

@app.route("/task", methods=["POST"])
@require_auth
def submit_task():
    data = request.get_json(silent=True) or {}
    tid = str(uuid.uuid4())
    cmd = data.get("command", "")
    if not cmd:
        return jsonify({"error": "command required"}), 400
    with tasks_lock:
        tasks[tid] = {
            "status": "pending", "command": cmd,
            "workdir": data.get("workdir"), "timeout": data.get("timeout", 300),
            "created_at": _now_iso(), "finished_at": None, "result": None,
        }
    threading.Thread(target=_run_task, args=(tid,), daemon=True).start()
    return jsonify({"status": "accepted", "task_id": tid})

@app.route("/task/<tid>", methods=["GET"])
@require_auth
def get_task(tid):
    with tasks_lock:
        t = tasks.get(tid)
    if not t:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "task_id": tid, "status": t["status"],
        "command": t["command"], "created_at": t["created_at"],
        "result": t["result"],
    })

# ===== API — 消息处理（方案6：HTTP 端点替代子进程）=====
_hermes_process_lock = threading.Lock()

@app.route("/api/process", methods=["POST"])
@require_auth
def process_message():
    """处理消息端点：接收消息 → 调 hermes CLI 处理 → 返回结果
    
    替代 worker 线程直接调 subprocess 的方式，通过 HTTP 统一管理处理生命周期。
    后续可升级为直接走 Gateway（不调子进程）。
    """
    PROTOCOL_VERSION = 2

    data = request.get_json(silent=True) or {}
    sender = data.get("sender", "unknown")
    content = data.get("message", "")
    timeout = data.get("timeout", 300)
    
    if not content:
        return jsonify({"error": "message required", "output": ""}), 400
    
    # === Phase 4 兼容层: 新/旧协议自动识别 ===
    # 新协议 (v2+): 内层 JSON 包含 "type" 字段 → 新协议路由
    # 旧格式 (v1/plain): JSON parse 失败或无 "type" → 降级为 type: discuss, llm: true
    msg_type = "discuss"  # 默认=旧格式降级
    msg_template = ""
    msg_priority = "normal"
    inner_message = content
    incoming_version = 1  # 默认旧协议
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict) and "type" in parsed:
            # 新协议路由（v2+）
            incoming_version = parsed.get("version", 2)
            raw_type = parsed.get("type", "info")
            msg_type = "info" if raw_type == "notify" else raw_type  # notify → info 映射
            msg_template = parsed.get("template", "")
            msg_priority = parsed.get("priority", "normal")
            inner_message = parsed.get("content") or parsed.get("message") or content
    except (json.JSONDecodeError, TypeError):
        pass  # 旧格式纯文本 → 降级为 type: discuss, llm: true
    
    # 查找 hermes CLI
    hermes_cmd = _find_hermes_cli()
    if not hermes_cmd:
        return jsonify({"error": "hermes CLI not found", "output": ""}), 500
    
    # 结构化消息头：通知 Hermes 这是结构化消息
    incoming_ver_str = f"v{incoming_version}"
    msg_header = f"[Agent-to-Agent] 来自 {sender}（Tether 通信桥 - /api/process）[{incoming_ver_str}]"
    if msg_type and msg_template:
        msg_header += f" [type={msg_type}, template={msg_template}]"
    if msg_priority == "high":
        msg_header += " [priority=high]"
    
    ack_note = ""
    if msg_type == "ack":
        ack_note = (
            "\n\n"
            f"=== 这是对之前消息的确认/回复 ===\n"
        )
    
    # Phase 4, Layer 2: 回复协议版本 — 告知 agent 回复时带协议版本号
    # 新协议消息回复时加 【v2|实际内容】前缀，方便对方识别协议版本
    version_note = (
        "\n\n"
        f"=== 协议版本说明 ===\n"
        f"本消息协议版本: v{incoming_version} | 我方支持: v{PROTOCOL_VERSION}\n"
        f"如果需回复对方（用 tether msg），请在消息内容开头加版本标记\n"
        f"例如: tether msg 目标IP 'v{PROTOCOL_VERSION}|回复内容'\n"
    ) if incoming_version >= 2 else ""
    
    prompt = (
        f"{msg_header}：\n"
        f"{inner_message}\n"
        f"{ack_note}"
        f"{version_note}"
        f"\n\n"
        f"这是另一台 Hermes agent 通过 Tether 发来的消息，不是普通聊天。\n"
        f"请根据内容判断需要做什么：\n"
        f"1) 如果需要执行任务，使用 terminal 等工具完成\n"
        f"2) 如果需要回复对方，直接输出回复内容，系统会自动转发\n"
        f"3) 如果是通知需要主人知道，请在下次与主人交流时提及"
    )
    
    try:
        # v3: 优先走 Gateway API（复用 session，保持 prefix cache）
        gateway_output, gateway_err = _gateway_chat(prompt, timeout=timeout)
        if gateway_err is None and gateway_output is not None:
            print(f"[tether] ✅ /api/process 通过 Gateway API 完成 ({len(gateway_output)} chars)")
            return jsonify({
                "status": "ok" if gateway_output else "error",
                "output": gateway_output,
                "gateway": True,
            })
        # Gateway 不可用，回退到子进程
        print(f"[tether] ⚡ /api/process 回退到子进程 (Gateway: {gateway_err})")
        r = subprocess.run(
            [hermes_cmd, "-z", prompt],
            capture_output=True, text=True, timeout=timeout
        )
        output = r.stdout.strip()
        stderr_short = r.stderr[:500] if r.stderr else ""
        print(f"[tether] ✅ /api/process 子进程完成 ({len(output)} chars, exit={r.returncode})")
        return jsonify({
            "status": "ok" if r.returncode == 0 else "error",
            "output": output,
            "stderr": stderr_short,
            "exit_code": r.returncode,
            "gateway": False,
        })
    except subprocess.TimeoutExpired:
        print(f"[tether] ⏰ /api/process 超时 ({timeout}s)")
        return jsonify({"status": "timeout", "output": "", "error": f"timeout ({timeout}s)"}), 504
    except Exception as e:
        print(f"[tether] 💥 /api/process 异常: {e}")
        return jsonify({"status": "error", "output": "", "error": str(e)}), 500

def _find_hermes_cli():
    """查找系统中可用的 hermes CLI"""
    for candidate in [
        os.path.expanduser("~/.hermes/hermes-agent/venv/bin/hermes"),
        "hermes"
    ]:
        if candidate == "hermes":
            try:
                subprocess.run(["which", "hermes"], capture_output=True, check=True)
                return "hermes"
            except:
                continue
        elif os.path.isfile(candidate):
            return candidate
    return None

# ===== Gateway API Server 客户端 (v3) =====
# 替代直接调 hermes CLI 子进程，复用 session 保持 prefix cache
GATEWAY_API_URL = "http://127.0.0.1:8642"
GATEWAY_SESSION_ID = "tether-bridge"
GATEWAY_API_KEY = ""  # 从环境变量读取

def _gateway_chat(message, timeout=300):
    """通过 Gateway API Server 处理消息，返回 (output, error) 二元组"""
    url = f"{GATEWAY_API_URL}/api/sessions/{GATEWAY_SESSION_ID}/chat"
    payload = json.dumps({"message": message}).encode()
    headers = {"Content-Type": "application/json"}
    if GATEWAY_API_KEY:
        headers["Authorization"] = f"Bearer {GATEWAY_API_KEY}"

    for attempt in range(2):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode())
            content = result.get("message", {}).get("content", "")
            return content.strip(), None
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            err_msg = str(e)
            if attempt == 0:
                print(f"[tether] ⚠️ Gateway API 请求失败 (attempt 1): {err_msg[:100]}")
                continue
            return None, err_msg
        except Exception as e:
            return None, str(e)
    return None, "Gateway API 请求失败（重试耗尽）"

def _ensure_gateway_session():
    """确保 tether-bridge session 存在，不存在则创建"""
    url = f"{GATEWAY_API_URL}/api/sessions"
    payload = json.dumps({"title": GATEWAY_SESSION_ID, "id": GATEWAY_SESSION_ID}).encode()
    headers = {"Content-Type": "application/json"}
    if GATEWAY_API_KEY:
        headers["Authorization"] = f"Bearer {GATEWAY_API_KEY}"

    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            print(f"[tether] ✅ Gateway session 已创建: {result.get('session', {}).get('id', GATEWAY_SESSION_ID)}")
            return True
    except urllib.error.HTTPError as e:
        if e.code == 409:  # session 已存在
            print(f"[tether] ✅ Gateway session 已存在: {GATEWAY_SESSION_ID}")
            return True
        print(f"[tether] ⚠️ Gateway session 创建失败 (HTTP {e.code}): {str(e)[:100]}")
        return False
    except Exception as e:
        print(f"[tether] ⚠️ Gateway 不可达，将使用子进程回退: {str(e)[:100]}")
        return False

# ===== 内部 =====
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _uptime():
    try:
        with open("/proc/uptime") as f:
            return float(f.read().split()[0])
    except:
        return 0

def _run_task(tid):
    with tasks_lock:
        t = tasks[tid]
        t["status"] = "running"
        cmd, wd, to = t["command"], t["workdir"], t["timeout"]
    r = _execute(cmd, wd, to)
    with tasks_lock:
        t["status"] = "completed" if r["exit_code"] == 0 else "failed"
        t["finished_at"] = _now_iso()
        t["result"] = r

def _execute(cmd, wd, to):
    import time as _t
    s = _t.time()
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=wd, timeout=to)
        ec, o, e = r.returncode, r.stdout[:50000], r.stderr[:10000]
    except subprocess.TimeoutExpired:
        ec, o, e = -1, "", f"[tether] 超时({to}s)"
    except Exception as ex:
        ec, o, e = -2, "", str(ex)
    return {"exit_code": ec, "stdout": o, "stderr": e, "duration_seconds": round(_t.time() - s, 2)}

def _resolve_tailscale_ip():
    try:
        r = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            return r.stdout.strip()
    except:
        pass
    try:
        return socket.gethostbyname(f"{HOSTNAME}.tailscale.network")
    except:
        return "0.0.0.0"

# ===== 启动 =====
def main():
    global TAILSCALE_IP, LISTEN_PORT, AUTH_TOKEN, DB_PATH, _own_tether_port, _own_auth_token

    parser = argparse.ArgumentParser(description="Tether Bridge v2")
    parser.add_argument("--port", type=int, default=LISTEN_PORT)
    parser.add_argument("--bind", default=None)
    parser.add_argument("--token", default=os.environ.get("TETHER_TOKEN"))
    parser.add_argument("--db", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db"))
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--gateway", action="store_true", default=None,
                        help="启用 Gateway API Server 模式（mac 用）")
    parser.add_argument("--no-gateway", action="store_true", default=None,
                        help="禁用 Gateway API Server 模式（tp 用）")
    args = parser.parse_args()

    LISTEN_PORT = args.port
    AUTH_TOKEN = args.token
    DB_PATH = args.db
    _own_tether_port = LISTEN_PORT
    _own_auth_token = AUTH_TOKEN

    # 解析 Gateway 模式
    global _use_gateway
    if args.no_gateway:
        _use_gateway = False
    elif args.gateway:
        _use_gateway = True
    # else: 保持默认值 (True)

    # 从 .env 文件读取 Gateway API Server key（如果环境变量中不存在）
    # 优先环境变量，再从 ~/.hermes/.env 加载
    global GATEWAY_API_KEY
    GATEWAY_API_KEY = os.environ.get("API_SERVER_KEY", "")
    if not GATEWAY_API_KEY:
        _env_path = os.path.expanduser("~/.hermes/.env")
        if os.path.isfile(_env_path):
            try:
                with open(_env_path) as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("API_SERVER_KEY="):
                            GATEWAY_API_KEY = line.split("=", 1)[1]
                            break
            except Exception as e:
                print(f"[tether] ⚠️ 读取 .env 失败: {e}")
    if GATEWAY_API_KEY:
        print(f"[tether] Gateway API key 已加载 ({len(GATEWAY_API_KEY)} chars)")

    _init_db()

    # 确保 Gateway session 存在（仅 mac 模式）
    if _use_gateway:
        _ensure_gateway_session()
    else:
        print(f"[tether] Gateway 模式已禁用，直接使用子进程")

    # 统一绑定 0.0.0.0（跨实例兼容）
    bind_addr = "0.0.0.0"

    print(f"🌉 Tether Bridge v2.3 — {HOSTNAME}")
    print(f"   监听: {bind_addr}:{LISTEN_PORT}")
    auth_status = "启用" if AUTH_TOKEN else "未设置"
    print(f"   认证: {auth_status}")
    gw_mode = "启用 (Gateway API)" if _use_gateway else "禁用 (子进程)"
    print(f"   消息处理模式: {gw_mode}")
    print(f"   数据库: {DB_PATH}")
    print(f"   实时消息处理: 已启用（串行队列 + 自动回复转发）")

    # 启动消息处理 worker（必须在 app.run 之前，因为 app.run 阻塞）
    _start_worker()

    app.run(host=bind_addr, port=LISTEN_PORT, debug=args.debug, use_reloader=False)

if __name__ == "__main__":
    main()
