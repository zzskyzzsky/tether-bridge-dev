#!/home/zzsky/.hermes/tether/venv/bin/python3
""""
Tether Watcher — 事件驱动消息处理器
2 秒轮询 /messages?ack=1 获取新消息 + 监控 handoff 文件。
处理完消息后自动将 agent 的回复 POST 回对方 Tether，无需 agent 手动执行命令。

独立于 tether_server.py 运行，没有耦合。
"""
import json, os, subprocess, threading, time, urllib.request

# 设置本机请求绕过 HTTP 代理（MacBook 上可能配了 Clash 环境变量）
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

NOTIFY_FILE = "/tmp/tether_notify.json"
HANDOFF_FILE = "/tmp/tether_handoff.json"
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8642")
GATEWAY_SESSION = "tether-watcher"
GATEWAY_API_KEY = ""
HANDOFF_RESULT_FILE = "/tmp/tether_handoff_result.json"
POLL_INTERVAL = 2

# 同行 Tether 地址（对方 Hermes 实例，用于自动回复）
PEER_HOST = os.environ.get("TETHER_PEER_HOST", "")
PEER_PORT = int(os.environ.get("TETHER_PEER_PORT", "9001"))
TETHER_URL = f"http://127.0.0.1:{PEER_PORT}"

# 从环境变量或 ~/.hermes/.env 读取 Gateway API Key + DingTalk Webhook URL
API_KEY_ENV = os.environ.get("API_SERVER_KEY", "")
GATEWAY_API_KEY = API_KEY_ENV.strip() if API_KEY_ENV else ""

DINGTALK_WEBHOOK_URL = os.environ.get("DINGTALK_WEBHOOK_URL", "")

_env_path = os.path.expanduser("~/.hermes/.env")
if os.path.isfile(_env_path):
    try:
        with open(_env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("API_SERVER_KEY=") and not GATEWAY_API_KEY:
                    GATEWAY_API_KEY = line.split("=", 1)[1].strip()
                elif line.startswith("DINGTALK_WEBHOOK_URL=") and not DINGTALK_WEBHOOK_URL:
                    DINGTALK_WEBHOOK_URL = line.split("=", 1)[1].strip()
    except Exception:
        pass

# 自愈相关
_last_restart_time = 0.0
_SELF_HEAL_INTERVAL = 15  # 每15秒检查一次 tether 健康

# DingTalk 通知桥
_DINGTALK_DEDUP_CACHE = {}  # content_hash -> timestamp
_DINGTALK_DEDUP_SECS = 30
DINGTALK_LOG_ONLY = False  # 正式通知，真实 POST 到钉钉群

# process_messages 重入锁，防止递归
_processing = False


def log(msg):
    print(f"[watcher] {msg}", flush=True)


def _tether_healthy():
    """检查本地 Tether 服务是否健康（HTTP GET /health → 200）"""
    try:
        req = urllib.request.Request(f"{TETHER_URL}/health")
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status == 200
    except Exception:
        return False


GATEWAY_PORT = 8642
_last_restart_time = 0  # 重启防抖计时（同时用于 Gateway 和 Tether 的自愈）


def _is_gateway_alive():
    """快速探测 Gateway 是否存活（HTTP /health 返回 200 才算活）"""
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{GATEWAY_PORT}/health")
        with urllib.request.urlopen(req, timeout=3) as resp:
            return resp.status == 200
    except Exception:
        return False


def _ensure_gateway_alive():
    """如果 Gateway 挂了，尝试自动重启 hermes-gateway.service

    包含重启防抖：连续两次重启间隔至少 30 秒，防止无限重启循环。
    """
    global _last_restart_time
    now = time.time()
    if now - _last_restart_time < 30:
        return  # 防抖：30秒内不重复重启
    _last_restart_time = now

    if _is_gateway_alive():
        return

    log("⚠️ Gateway 不在线，尝试重启...")
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", "hermes-gateway.service"],
            capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        log(f"❌ 重启命令失败: {e}")
        return

    for attempt in range(6):
        if _is_gateway_alive():
            log(f"✅ Gateway 重启成功（第{attempt+1}次检查）")
            return
        time.sleep(5)
    log("❌ Gateway 重启确认失败")


def _tether_restart():
    """自动重启 tether.service"""
    global _last_restart_time
    now = time.time()
    if now - _last_restart_time < 30:
        return
    _last_restart_time = now

    log("🔄 尝试重启 tether.service...")
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", "tether.service"],
            capture_output=True, text=True, timeout=10
        )
    except Exception as e:
        log(f"❌ 重启命令失败: {e}")
        return False

    for attempt in range(3):
        if attempt > 0:
            time.sleep(5)
        if _tether_healthy():
            log(f"✅ 重启成功（第{attempt+1}次检查后）")
            return True
        log(f"⏳ 等待 tether 恢复（第{attempt+1}次检查，未就绪）")
    log("❌ 重启确认失败，tether 仍不可用")
    return False


def _self_heal():
    """自愈巡检：Gateway 健康检查 + 自动重启"""
    _ensure_gateway_alive()
    if not _tether_healthy():
        log("⚠️ Tether 不可达（由 systemd Restart=always 负责恢复）")


def _tether_get(path):
    try:
        req = urllib.request.Request(f"{TETHER_URL}{path}")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read()), None
    except Exception as e:
        return None, str(e)


def _gateway_headers():
    h = {"Content-Type": "application/json"}
    if GATEWAY_API_KEY:
        h["Authorization"] = f"Bearer {GATEWAY_API_KEY}"
    return h


def _gateway_chat(message, timeout=300):
    """通过 Gateway API 处理消息，返回 (output, error)"""
    url = f"{GATEWAY_URL}/api/sessions/{GATEWAY_SESSION}/chat"
    headers = _gateway_headers()
    payload = json.dumps({"message": message}).encode()

    for attempt in range(2):
        try:
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode())
            content = result.get("message", {}).get("content", "")
            return content.strip(), None
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            err = str(e)
            if attempt == 0:
                log(f"Gateway 请求失败 (重试): {err[:80]}")
                time.sleep(2)
                continue
            return None, err
        except Exception as e:
            return None, str(e)
    return None, "Gateway 失败（重试耗尽）"


def _ensure_gateway_session():
    """确保 Gateway session 存在"""
    url = f"{GATEWAY_URL}/api/sessions"
    headers = _gateway_headers()
    payload = json.dumps({"title": GATEWAY_SESSION, "id": GATEWAY_SESSION}).encode()
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        urllib.request.urlopen(req, timeout=10)
        log("Gateway session 已创建")
    except urllib.error.HTTPError as e:
        if e.code != 409:
            log(f"Gateway session 创建失败: HTTP {e.code}")
    except Exception as e:
        log(f"Gateway 不可达: {str(e)[:60]}")


def _find_hermes():
    for c in [os.path.expanduser("~/.hermes/hermes-agent/venv/bin/hermes"), "hermes"]:
        if c == "hermes":
            try:
                subprocess.run(["which", "hermes"], capture_output=True, check=True)
                return "hermes"
            except Exception:
                continue
        elif os.path.isfile(c):
            return c
    return None


def _get_hermes_args(prompt):
    """构建 hermes -z 命令行参数，支持通过 HERMES_EXTRA_FLAGS 环境变量传递额外参数"""
    hermes = _find_hermes()
    if not hermes:
        return None
    cmd = [hermes, "-z"]
    extra = os.environ.get("HERMES_EXTRA_FLAGS", "").strip()
    if extra:
        cmd.extend(extra.split())
    cmd.append(prompt)
    return cmd


_REPORT_KEYWORDS = ["汇报给主人", "最终报告", "测试通过", "请汇报", "测试全部通过"]


def _should_report(output):
    """检查输出是否应该通过 DingTalk 汇报给主人，而不是自动回复给发送方"""
    if not output:
        return False
    # [REPORT] 精确前缀
    if output.startswith("[REPORT]"):
        return True
    # 关键词 fallback（AI 可能忘记加 [REPORT] 前缀）
    output_lower = output.lower()
    for kw in _REPORT_KEYWORDS:
        if kw in output_lower:
            return True
    return False


def _send_dingtalk(content):
    """通过 DingTalk 群机器人 webhook 发送消息

    读取 DINGTALK_WEBHOOK_URL，POST markdown 消息到钉钉群。
    支持 dedup（30秒内同内容不重复发）和 log-only 测试模式。
    """
    if not content:
        return
    if not DINGTALK_WEBHOOK_URL:
        log("⚠️ DINGTALK_WEBHOOK_URL 未配置，跳过 DingTalk 通知")
        _write_report_to_file(content)
        return

    # Dedup：30秒内相同内容 hash 不重复发
    content_hash = __import__("hashlib").md5(content.encode()).hexdigest()
    now = time.time()
    if content_hash in _DINGTALK_DEDUP_CACHE:
        if now - _DINGTALK_DEDUP_CACHE[content_hash] < _DINGTALK_DEDUP_SECS:
            log(f"⏭️ 跳过重复 DingTalk 通知（30秒内相同内容 {content_hash[:8]}）")
            _write_report_to_file(content)
            return
    _DINGTALK_DEDUP_CACHE[content_hash] = now
    # 定期清理过期缓存（最多保留 100 条）
    if len(_DINGTALK_DEDUP_CACHE) > 100:
        cutoff = now - _DINGTALK_DEDUP_SECS
        for k, v in list(_DINGTALK_DEDUP_CACHE.items()):
            if v < cutoff:
                del _DINGTALK_DEDUP_CACHE[k]

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": "🤖 Tether 报告",
            "text": f"🤖 **Tether 报告**\n\n{content[:4000]}",
        },
    }

    if DINGTALK_LOG_ONLY:
        log(f"📋 [LOG-ONLY] DingTalk 通知（{len(content)} chars）: {content[:200]}")
        _write_report_to_file(content)
        return

    try:
        req = urllib.request.Request(
            DINGTALK_WEBHOOK_URL,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        if result.get("errcode") == 0:
            log("✅ DingTalk 通知发送成功")
        else:
            log(f"⚠️ DingTalk 返回错误: {result.get('errmsg', 'unknown')}")
    except Exception as e:
        log(f"❌ DingTalk POST 失败: {str(e)[:60]}")
    _write_report_to_file(content)


def _write_report_to_file(content):
    """将汇报内容写入临时文件，供主 session 下次启动时读取"""
    try:
        now = __import__("datetime").datetime.now().isoformat()
        with open(HANDOFF_RESULT_FILE, "w") as f:
            json.dump({
                "type": "report",
                "content": content,
                "time": now,
            }, f)
    except Exception:
        pass


def _auto_reply(output, sender_info):
    """自动回复：将 hermes -z / Gateway 的输出 POST 回发送方 Tether

    sender_info 是消息中的 sender 字段值，格式为 'hostname (nickname)'。
    从 sender_info 中提取 hostname，解析出对方的 Tether 地址。
    """
    if not output or not sender_info:
        return

    # 从 sender_info 中提取主机名（格式: "hostname (nickname)"）
    target_host = sender_info.split()[0] if sender_info else ""
    if not target_host or target_host in ("unknown",):
        # 无法从 sender 提取主机名时，回退到 PEER_HOST 环境变量
        target_host = os.environ.get("TETHER_PEER_HOST", "")
        if not target_host:
            log("⚠️ TETHER_PEER_HOST 未设置且 sender 无主机名 → auto-reply 跳过，请设置 TETHER_PEER_HOST=对方主机名")
            return

    # 跳过自己发给自己的消息（防止回环）
    local_hostname = __import__("socket").gethostname()
    if target_host == local_hostname:
        return

    peer_url = f"http://{target_host}:{PEER_PORT}/message"
    hostname = __import__("socket").gethostname()
    sender_nick = os.environ.get("TETHER_SENDER_NICK", hostname)

    # 只取输出前 4000 字符，防止过长
    reply_text = output[:4000]

    try:
        payload = json.dumps({
            "from": f"{hostname} ({sender_nick})",
            "sender": f"{hostname} ({sender_nick})",
            "message": reply_text,
            "content": reply_text,
            "type": "info",
        }).encode()
        req = urllib.request.Request(peer_url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10):
            log(f"📤 自动回复 {len(reply_text)} chars 到 {target_host}")
    except Exception as e:
        log(f"⏰ 自动回复失败: {str(e)[:60]}")


# DingTalk 通知相关
_last_dingtalk_time = 0.0  # 上次发 DingTalk 通知的时间（30s 去重）


def _send_dingtalk_notification(output, sender_info):
    """发送 DingTalk 群通知：将 Tether 消息处理结果推送到钉钉群。

    遵守 4 条约定：
    - 先 log 再 POST（信息不会丢）
    - 30s 去重（不刷屏）
    - Fallback：POST 失败时 log error
    - Webhook URL 由主人在 .env 中配置，不自动生成
    """
    if not DINGTALK_WEBHOOK_URL:
        return  # 未配置 webhook，不通知

    if not output or not sender_info:
        return

    # 30s 去重
    global _last_dingtalk_time
    now = time.time()
    if now - _last_dingtalk_time < 30:
        return
    _last_dingtalk_time = now

    # 先 log 再 POST
    log(f"📣 DingTalk 通知: {len(output)} chars from={sender_info[:40]}")

    # 截断输出，钉钉消息不宜过长
    text = output[:2000]

    # 提取 sender 简短描述
    sender_desc = sender_info.strip()

    payload = json.dumps({
        "msgtype": "markdown",
        "markdown": {
            "title": "Hermes Tether 通知",
            "text": f"## Hermes Tether 通知\n\n"
                    f"**来自**: {sender_desc}\n\n"
                    f"{text}\n\n"
                    f"---\n*Tether watcher 自动推送*"
        }
    }).encode()

    try:
        req = urllib.request.Request(
            DINGTALK_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("errcode") == 0:
                log("✅ DingTalk 通知发送成功")
            else:
                log(f"⚠️ DingTalk 通知返回异常: {result}")
    except Exception as e:
        log(f"⏰ DingTalk 通知失败: {str(e)[:80]}（信息已在日志中，无丢失）")


def process_messages():
    """从 Tether 拉取未处理消息并逐一处理。返回本次处理的消息数。

    带 _processing 重入锁，防止递归调用（例如：gateway 处理过程中，
    外部信号或回调又触发了 process_messages）。
    """
    global _processing
    if _processing:
        return 0
    _processing = True
    try:
        # 不在此处做自愈——自愈由 _self_heal() 每15s处理
        # 如果 Gateway 挂了，_gateway_chat() 会回退到 hermes -z 子进程

        data, err = _tether_get("/messages?ack=1")
        if err or not data:
            log(f"取消息失败: {err}")
            return 0

        msgs = data.get("messages", [])
        if not msgs:
            return 0

        log(f"\U0001f4ec {len(msgs)} 条新消息")
        for msg in msgs:
            mid = msg.get("id", "?")[:8]
            sender = msg.get("sender", "unknown")
            content = msg.get("message", "")
            log(f"\u25b6 处理 {mid} from={sender}: {content[:80]}")

            # 去重过滤：连续确认循环消息直接跳过（同 sender、含确认关键词、N 分钟内重复）
            skip_keywords = ["已清理", "无积压", "无需操作", "不回复以阻断", "等主人回来",
                            "不回复以阻断循环", "双方一致", "已对齐", "已确认"]
            if any(kw in content for kw in skip_keywords):
                log(f"\u23ed {mid} 跳过（确认循环消息）")
                continue

            prompt = (
                f"[Tether] 来自 {sender}：\n{content}\n\n"
                f"这是另一台 Hermes agent 通过 Tether 发来的消息。\n"
                f"请理解内容并直接处理或回复。处理完成后给出总结。"
            )

            processed = False
            output = None

            # 优先走 Gateway
            output, err = _gateway_chat(prompt, timeout=300)
            if err is None and output is not None:
                has_out = bool(output)
                log(f"\u2705 {mid} 处理完成 (Gateway, {len(output) if has_out else 0} chars)")
                processed = True
            else:
                log(f"Gateway 失败 ({err or 'no output'}), 回退子进程")

            # Gateway 失败则走子进程
            if not processed:
                cmd = _get_hermes_args(prompt)
                if cmd:
                    try:
                        r = subprocess.run(
                            cmd,
                            capture_output=True, text=True, timeout=300
                        )
                        if r.returncode == 0 and r.stdout.strip():
                            output = r.stdout.strip()
                            log(f"\u2705 {mid} 处理完成 (子进程, {len(output)} chars)")
                        else:
                            log(f"\u26a0\ufe0f {mid} 处理完成但无输出")
                    except subprocess.TimeoutExpired:
                        log(f"\u23f0 {mid} 超时 (300s)")
                else:
                    log("\u274c 找不到 hermes CLI")

            # 汇报或自动回复：Report 发 DingTalk，其他回发送方
            if output and sender:
                if _should_report(output):
                    log(f"🔔 {mid} 标记为 Report → 走 DingTalk 通知")
                    _send_dingtalk(output)

                # 所有消息都 auto-reply 回发送方
                _auto_reply(output, sender)

        log(f"✅ 本轮处理完成 ({len(msgs)} 条)")
        return len(msgs)
    finally:
        _processing = False


def _write_handoff_result(sender, summary, output):
    """将 handoff 处理结果写入临时文件，供主 session 检测"""
    try:
        now = __import__("datetime").datetime.now().isoformat()
        with open(HANDOFF_RESULT_FILE, "w") as f:
            json.dump({"sender": sender, "summary": summary,
                       "output": output, "time": now}, f)
    except Exception:
        pass


def process_handoffs():
    """检查 handoff 文件，有内容就通过 hermes -z 处理（需要工具执行能力）

    通过子线程运行 hermes -z，不阻塞主循环（info 消息处理不受影响）。
    处理完成后自动回复到发送方。
    """
    if not os.path.isfile(HANDOFF_FILE):
        return

    try:
        with open(HANDOFF_FILE) as f:
            handoff = json.load(f)
    except (json.JSONDecodeError, OSError):
        return

    sender = handoff.get("sender", "")
    summary = handoff.get("summary", "")
    if not sender or not summary:
        # 空 handoff 文件，删掉避免重复 stat
        try:
            os.remove(HANDOFF_FILE)
        except OSError:
            pass
        # 检查 SQLite 中是否还有未处理的 handoff，有就恢复
        _recover_next_handoff()
        return

    log(f"\udccb 发现 handoff from={sender}: {summary[:60]}...")

    # 先删除 handoff 文件，防止重复处理
    try:
        os.remove(HANDOFF_FILE)
    except OSError:
        pass

    prompt = (
        f"[Tether Handoff] 来自 {sender} 的接力消息：\n{summary}\n\n"
        f"这是另一台 Hermes agent 发来的 handoff 消息，需要你主动处理。\n"
        f"1) 分析消息内容并执行必要的操作（修改文件、重启服务等）\n"
        f"2) 处理完成后输出总结\n"
        f"3) 如果这是需要汇报给主人的最终报告，请在第一行写上 [REPORT]\n"
    )

    msg_id = handoff.get("msg_id", "")

    # 子线程运行 hermes -z，不阻塞主循环
    def _run_handoff(sender=sender, msg_id=msg_id):
        output = None
        try:
            cmd = _get_hermes_args(prompt)
            if not cmd:
                log("❌ 找不到 hermes CLI，handoff 跳过")
                return
            r = subprocess.run(
                cmd,
                capture_output=True, text=True, timeout=300
            )
            output = r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else ""
            if output:
                log(f"✅ Handoff 处理完成 ({len(output)} chars)")
                _write_handoff_result(sender, summary[:40], output[:500])
            else:
                log(f"⚠️ Handoff 完成但无输出 (rc={r.returncode})")
        except subprocess.TimeoutExpired:
            log(f"⏰ Handoff 超时 (300s)")
        except Exception as e:
            log(f"❌ Handoff 异常: {str(e)[:80]}")

        # 汇报或自动回复：Report 发 DingTalk，其他回发送方
        if output and sender:
            if _should_report(output):
                log(f"🔔 Handoff [{msg_id[:8]}] 标记为 Report → 走 DingTalk 通知")
                _send_dingtalk(output)
            else:
                _auto_reply(output, sender)

        # 标记本消息为已确认，防止 _recover_next_handoff 再次恢复同一消息
        _ack_handoff(msg_id)

        # 处理完一条后尝试恢复下一条积压 handoff
        _recover_next_handoff()

    t = threading.Thread(target=_run_handoff, daemon=True)
    t.start()
    log("🔀 Handoff 已交给子线程处理，继续轮询")


def _ack_handoff(msg_id):
    """标记 handoff 消息为已确认（acked=1），避免重复恢复"""
    if not msg_id:
        return
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        if not os.path.isfile(db_path):
            return
        conn = sqlite3.connect(db_path, timeout=3)
        conn.execute("UPDATE messages SET acked=1 WHERE id=?", (msg_id,))
        conn.commit()
        conn.close()
        log(f"✅ Handoff {msg_id[:8]} 已标记为 acked")
    except Exception as e:
        log(f"⚠️ 标记 handoff acked 失败: {str(e)[:60]}")


def _recover_next_handoff():
    """从 SQLite 中取下一个未处理的 handoff，写入 handoff 文件"""
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        if not os.path.isfile(db_path):
            return
        conn = sqlite3.connect(db_path, timeout=3)
        row = conn.execute(
            "SELECT id, sender, message FROM messages WHERE acked=0 AND type='handoff' LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return
        msg_id, sender, message = row
        log(f"♻️ 恢复下一条 handoff #{msg_id[:8]} from={sender}")
        with open(HANDOFF_FILE, "w") as f:
            json.dump({
                "msg_id": msg_id,
                "sender": sender,
                "summary": message[:200],
                "timestamp": __import__("datetime").datetime.now().isoformat(),
            }, f)
    except Exception as e:
        log(f"handoff 恢复异常: {str(e)[:60]}")


def _recover_stale_handoffs():
    """启动时检查：SQLite 中是否有未处理的 handoff 消息，重新生成 handoff 文件"""
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        if not os.path.isfile(db_path):
            return
        conn = sqlite3.connect(db_path, timeout=3)
        rows = conn.execute(
            "SELECT id, sender, message FROM messages WHERE acked=0 AND type='handoff'"
        ).fetchall()
        conn.close()
        if not rows:
            return
        for row in rows:
            msg_id, sender, message = row
            log(f"♻️ 发现未处理的 handoff #{msg_id[:8]} from={sender}，重新生成 handoff 文件")
            with open(HANDOFF_FILE, "w") as f:
                json.dump({
                    "msg_id": msg_id,
                    "sender": sender,
                    "summary": message[:200],
                    "timestamp": __import__("datetime").datetime.now().isoformat(),
                }, f)
            # 只恢复第一条，后续靠子线程链式推进
            break
    except Exception as e:
        log(f"handoff 恢复检查异常: {str(e)[:60]}")


def _validate_env():
    """启动时校验关键环境变量，缺失时输出警告（不阻塞启动）"""
    if not PEER_HOST:
        log("⚠️ TETHER_PEER_HOST 未设置 → auto-reply 不会自动回复，需手动发送")
    else:
        log(f"✅ TETHER_PEER_HOST={PEER_HOST}")

    sender_nick = os.environ.get("TETHER_SENDER_NICK", "")
    if not sender_nick:
        log("⚠️ TETHER_SENDER_NICK 未设置 → 将使用主机名作为发送者昵称")
    else:
        log(f"✅ TETHER_SENDER_NICK={sender_nick}")


def main():
    # 启动时先确保 Gateway 存活
    _ensure_gateway_alive()

    _ensure_gateway_session()

    # 环境变量校验
    _validate_env()

    # 启动时恢复因 watcher 重启而残留的未处理 handoff
    _recover_stale_handoffs()

    log(f"Watcher 已启动 (间隔={POLL_INTERVAL}s, 自愈={_SELF_HEAL_INTERVAL}s, 轮询模式)")
    last_heal_time = 0.0

    while True:
        now = time.time()
        try:
            # 主动轮询所有未处理消息（不再依赖 notify.json mtime 门控）
            # 每个轮询周期都拉取，移除竞态条件风险
            count = process_messages()
            # 如果本轮处理了消息，立即再 catch 一轮（最多追加1次），
            # 捕获处理期间到达的漏网消息，不加 sleep
            if count > 0:
                process_messages()

            # handoff 文件检查（handoff 走独立渠道，由 server 写入 handoff 文件）
            process_handoffs()

        except Exception as e:
            log(f"轮询异常: {e}")

        # 自愈巡检（每 _SELF_HEAL_INTERVAL 秒执行一次）
        if now - last_heal_time >= _SELF_HEAL_INTERVAL:
            last_heal_time = now
            _self_heal()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
