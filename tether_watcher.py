#!/home/zzsky/.hermes/tether/venv/bin/python3
""""
Tether Watcher — 事件驱动消息处理器
2 秒轮询 /messages?ack=1 获取新消息 + 监控 handoff 文件。
处理完消息后自动将 agent 的回复 POST 回对方 Tether，无需 agent 手动执行命令。

独立于 tether_server.py 运行，没有耦合。
"""
import json, os, resource, subprocess, threading, time, urllib.request, uuid
from datetime import datetime, timezone

# 设置本机请求绕过 HTTP 代理（MacBook 上可能配了 Clash 环境变量）
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

NOTIFY_FILE = "/tmp/tether_notify.json"
HANDOFF_FILE = "/tmp/tether_handoff.json"
HEARTBEAT_FILE = "/tmp/tether_watcher_heartbeat.json"
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://127.0.0.1:8642")
GATEWAY_SESSION = "tether-watcher"
GATEWAY_API_KEY = ""
HANDOFF_RESULT_FILE = "/tmp/tether_handoff_result.json"
POLL_INTERVAL = 2

# 同行 Tether 地址（对方 Hermes 实例，用于自动回复）
PEER_HOST = os.environ.get("TETHER_PEER_HOST", "")
PEER_PORT = int(os.environ.get("TETHER_PEER_PORT", "9001"))
PEER_FALLBACK_HOST = os.environ.get("TETHER_PEER_FALLBACK_HOST", "")
TETHER_URL = f"http://127.0.0.1:{PEER_PORT}"

# host -> 短名映射（target_nick：mac/tp）
_HOST_TO_NICK_SHORT = {
    "zzsky-mbp": "mac",
    "zzskytpg3": "tp",
    "154.8.143.218": "tp",   # VPS relay -> tp
}

# host -> 全名映射（local_nick：tp-哥哥/mac-弟弟）
_HOST_TO_NICK_FULL = {
    "zzsky-mbp": "mac-弟弟",
    "zzskytpg3": "tp-哥哥",
    "154.8.143.218": "tp-哥哥",  # VPS relay -> tp-哥哥
}


def _get_nick(host):
    """从映射获取指定主机的短名（mac/tp），用于 target_nick"""
    if not host:
        return host
    host_lower = host.lower()
    for h, n in _HOST_TO_NICK_SHORT.items():
        if h == host_lower:
            return n
    peer_nick = os.environ.get("TETHER_PEER_NICK", "")
    if PEER_HOST and host == PEER_HOST and peer_nick:
        return peer_nick
    return host


def _get_full_nick(host):
    """从映射获取指定主机的全名（tp-哥哥/mac-弟弟），用于 local_nick"""
    if not host:
        return host
    host_lower = host.lower()
    for h, n in _HOST_TO_NICK_FULL.items():
        if h == host_lower:
            return n
    return host


def _get_peer_nick():
    """获取对端短名（mac/tp），用于唤醒消息"""
    local = __import__("socket").gethostname().lower()
    for h, n in _HOST_TO_NICK_SHORT.items():
        if h != local:
            return n
    # fallback：从环境变量获取
    return os.environ.get("TETHER_PEER_NICK", "对方")

# 从环境变量或 ~/.hermes/.env 读取 Gateway API Key + DingTalk Webhook URL
API_KEY_ENV = os.environ.get("API_SERVER_KEY", "")
GATEWAY_API_KEY = API_KEY_ENV.strip() if API_KEY_ENV else ""

DINGTALK_WEBHOOK_URL = os.environ.get("DINGTALK_WEBHOOK_URL", "")
NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "")

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
                elif line.startswith("NOTIFY_WEBHOOK_URL=") and not NOTIFY_WEBHOOK_URL:
                    NOTIFY_WEBHOOK_URL = line.split("=", 1)[1].strip()
    except Exception:
        pass

def _limit_memory():
    """限制子进程内存上限为 2GB，防止 OOM 拖垮整个机器"""
    try:
        resource.setrlimit(resource.RLIMIT_AS, (2*1024**3, 2*1024**3))
    except (resource.error, ValueError, AttributeError):
        pass  # 某些系统不支持，静默跳过


# 自愈巡检计数器：连续失败次数超过阈值只打 WARN，不主动重启 watcher
_last_restart_time = 0.0
_SELF_HEAL_INTERVAL = 15  # 每15秒检查一次 tether 健康

# DingTalk 通知桥
_DINGTALK_DEDUP_CACHE = {}  # content_hash -> timestamp
_DINGTALK_DEDUP_SECS = 30
DINGTALK_LOG_ONLY = False  # 正式通知，真实 POST 到钉钉群

# 飞书/通用 Webhook 通知：NOTIFY_WEBHOOK_URL 优先，兼容 DINGTALK
if not NOTIFY_WEBHOOK_URL and DINGTALK_WEBHOOK_URL:
    NOTIFY_WEBHOOK_URL = DINGTALK_WEBHOOK_URL

# 超时唤醒去重缓存
_HANDOFF_TIMEOUT_CACHE = {}  # outgoing_msg_id -> timestamp
_HANDOFF_TIMEOUT_MINUTES = 15  # 默认15分钟（之前5分钟，通知太频繁）
_HANDOFF_TIMEOUT_COOLDOWN = 600  # 10分钟内不重复唤醒同一消息

# OOM 自愈计数器：按 msg_id 跟踪连续 OOM
_OOM_COUNTER = {}  # {msg_id: [count, timestamp]}
_OOM_WINDOW = 300  # 5分钟窗口，超时重置
_OOM_THRESHOLD = 3  # 连续 3 次同一 handoff OOM 触发链路重启


def _detect_oom(returncode, msg_id=""):
    """检测子进程 OOM（returncode=-9=SIGKILL）

    RLIMIT_AS 触发 OOM killer 时内核发送 SIGKILL，returncode 为 -9。
    带 per-msg_id 计数器，同一 handoff 连续 OOM N 次触发链路重启。
    """
    if returncode != -9:
        return False

    log(f"💥 OOM 事件: 子进程被 SIGKILL (returncode=-9)")

    if not msg_id:
        return True

    # 清理 5 分钟窗口外的过期记录
    now = time.time()
    for mid in list(_OOM_COUNTER.keys()):
        if now - _OOM_COUNTER[mid][1] > _OOM_WINDOW:
            del _OOM_COUNTER[mid]

    # 更新当前 msg_id 计数
    if msg_id not in _OOM_COUNTER:
        _OOM_COUNTER[msg_id] = [0, now]
    _OOM_COUNTER[msg_id][0] += 1
    _OOM_COUNTER[msg_id][1] = now
    count = _OOM_COUNTER[msg_id][0]

    log(f"📊 连续 OOM #{count}（msg_id={msg_id[:8]}）")

    if count >= _OOM_THRESHOLD:
        log(f"🚨 连续 {_OOM_THRESHOLD} 次 OOM（msg_id={msg_id[:8]}），触发链路重启")
        # 先 ack 断开循环，再重启
        _ack_handoff(msg_id)
        _restart_watcher_chain(msg_id)

    return True


def _restart_watcher_chain(msg_id=""):
    """重启 watcher 链路：先 ack handoff 断开循环，再重启 watcher 服务"""
    try:
        log(f"🔄 重启 tether-watcher.service...")
        subprocess.run(
            ["systemctl", "--user", "restart", "tether-watcher.service"],
            capture_output=True, text=True, timeout=10
        )
        log(f"✅ tether-watcher.service 重启命令已发送")
    except Exception as e:
        log(f"❌ 重启 tether-watcher.service 失败: {e}")


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
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status == 200
    except Exception:
        return False


def _ensure_gateway_alive():
    """如果 Gateway 挂了，先反复确认，再尝试重启

    重启防抖 30 秒。但在判定死亡前先做多次探测，
    给 Gateway 自己恢复的机会（如飞书 WebSocket 重连期间的短暂无响应）。
    """
    global _last_restart_time
    now = time.time()
    if now - _last_restart_time < 30:
        return  # 防抖：30秒内不重复重启

    # 第一次快速检查
    if _is_gateway_alive():
        return

    # 慢速重试：给 Gateway 最多 ~25 秒自行恢复
    log("⚠️ Gateway 暂未响应，持续监测中...")
    for attempt in range(5):
        time.sleep(5)
        if _is_gateway_alive():
            log(f"✅ Gateway 已自行恢复（第{attempt+1}次检查）")
            return
    log("⚠️ Gateway 连续 25 秒无响应，准备重启")

    _last_restart_time = now
    try:
        subprocess.run(
            ["systemctl", "--user", "restart", "hermes-gateway.service"],
            capture_output=True, text=True, timeout=30
        )
    except Exception as e:
        log(f"❌ 重启命令失败: {e}")
        return

    for attempt in range(12):
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
    """自愈巡检：Gateway 健康检查 + outgoing 重试 + 超时唤醒 + 自动重启"""
    _ensure_gateway_alive()
    _check_outgoing_retry()
    _check_handoff_timeout()  # 检查超时无回复的消息，生成唤醒
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


def _send_notification(content):
    """通过 Webhook 发送通知（支持飞书 + 钉钉）

    根据 NOTIFY_WEBHOOK_URL 自动判断格式：
    - 飞书群机器人: msg_type=text
    - 钉钉群机器人: msgtype=text
    - 都不匹配则仅写入日志
    """
    if not content:
        return
    if not NOTIFY_WEBHOOK_URL:
        log("⚠️ NOTIFY_WEBHOOK_URL 未配置，跳过通知")
        _write_report_to_file(content)
        return

    # 判断 webhook 类型
    is_feishu = "feishu.cn" in NOTIFY_WEBHOOK_URL.lower() or "larksuite" in NOTIFY_WEBHOOK_URL.lower()
    is_dingtalk = "dingtalk" in NOTIFY_WEBHOOK_URL.lower()

    if is_feishu:
        payload = {
            "msg_type": "text",
            "content": {"text": f"🤖 Tether 报告\n\n{content[:3000]}"},
        }
    elif is_dingtalk:
        payload = {
            "msgtype": "text",
            "text": {"content": f"🤖 Tether 报告\n\n{content[:3000]}"},
        }
    else:
        # 无法识别类型，尝试飞书格式
        payload = {
            "msg_type": "text",
            "content": {"text": f"🤖 Tether 报告\n\n{content[:3000]}"},
        }

    try:
        req = urllib.request.Request(
            NOTIFY_WEBHOOK_URL,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        if result.get("StatusCode") == 0 or result.get("errcode") == 0:
            log("✅ 通知发送成功")
        else:
            log(f"⚠️ 通知返回错误: {str(result)[:80]}")
    except Exception as e:
        log(f"❌ 通知 POST 失败: {str(e)[:60]}")
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


def _try_post(peer_url, payload, timeout=10):
    """尝试 POST 到指定 URL，返回 (success, error_msg)"""
    try:
        req = urllib.request.Request(peer_url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout):
            return True, None
    except Exception as e:
        return False, str(e)[:100]


def _auto_reply(output, sender_info, original_msg_id=None):
    """自动回复：将 hermes -z / Gateway 的输出 POST 回发送方 Tether

    sender_info 是消息中的 sender 字段值，格式为 'hostname (nickname)'。
    从 sender_info 中提取 hostname，解析出对方的 Tether 地址。

    如果提供了 original_msg_id，会在 payload 中带上 in_reply_to，
    对方 server 收到后自动 ack 对应的 outgoing_messages。

    多候选连接策略：
    - 如果 TETHER_PEER_FALLBACK_HOST 设置了，构建 [PEER_HOST, FALLBACK_HOST] 候选列表
    - 先试 PEER_HOST（VPS WG IP），失败后试 FALLBACK_HOST（Tailscale 直连）
    - 只在首次失败时尝试 fallback，成功即停止
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

    # 检查 target_host 是否含非 ASCII 字符（如纯中文昵称），有则回退到 PEER_HOST
    if any(ord(c) > 127 for c in target_host):
        fallback = os.environ.get("TETHER_PEER_HOST", "")
        if fallback and fallback != target_host:
            log(f"⏭ target_host 含非 ASCII 字符 ({target_host})，回退到 TETHER_PEER_HOST={fallback}")
            target_host = fallback
        else:
            log(f"⚠️ target_host 含非 ASCII 字符 ({target_host})，且无 TETHER_PEER_HOST 回退，跳过 auto-reply")
            return

    # 跳过自己发给自己的消息（防止回环）
    local_hostname = __import__("socket").gethostname()
    if target_host == local_hostname:
        return

    # 构建候选连接列表：[主路径, 回退路径]
    candidates = [target_host]
    if PEER_FALLBACK_HOST and PEER_FALLBACK_HOST != target_host and PEER_FALLBACK_HOST != local_hostname:
        candidates.append(PEER_FALLBACK_HOST)
    hostname = __import__("socket").gethostname()
    sender_nick = os.environ.get("TETHER_SENDER_NICK", hostname)

    # 只取输出前 4000 字符；清理非法 surrogate 字符（json.dumps 对 surrogate 报错）
    reply_text = output[:4000]
    reply_text = reply_text.encode("utf-8", errors="replace").decode("utf-8")

    # 防自动回复循环：检查最近 N 秒内是否给同一目标发过相同内容
    DEDUP_SECONDS = 30
    try:
        import sqlite3
        _db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        _conn = sqlite3.connect(_db_path, timeout=3)
        _cur = _conn.execute(
            "SELECT COUNT(*) FROM outgoing_messages "
            "WHERE target_host = ? AND message = ? "
            "AND sent_at > datetime('now', ? || ' seconds', 'utc')",
            (target_host, reply_text, f"-{DEDUP_SECONDS}")
        )
        if _cur.fetchone()[0] > 0:
            _conn.close()
            log(f"⏭️ 跳过重复 auto-reply 到 {target_host}（{DEDUP_SECONDS}s 内已有相同内容）")
            return
        _conn.close()
    except Exception:
        pass

    payload = json.dumps({
        "from": f"{hostname} ({sender_nick})",
        "sender": f"{hostname} ({sender_nick})",
        "message": reply_text,
        "content": reply_text,
        "type": "info",       # 改 info 让对端 watcher 可见
        "is_reply": True,     # 标记为回复消息，对端跳过 auto-reply-back 防回环
        "ttl": 1,
    }).encode()

    # 如果有 original_msg_id，payload 中带 in_reply_to 字段
    # 对方 server 收到后会自动 ack 对应的 outgoing_messages
    if original_msg_id:
        payload_obj = json.loads(payload.decode())
        payload_obj["in_reply_to"] = original_msg_id
        payload = json.dumps(payload_obj).encode()

    # 遍历候选列表，逐个尝试
    last_err_msg = ""
    for i, candidate in enumerate(candidates):
        peer_url = f"http://{candidate}:{PEER_PORT}/message"
        ok, err = _try_post(peer_url, payload)
        if ok:
            used = "主路径" if i == 0 else f"回退路径（前次失败: {last_err_msg}）"
            log(f"📤 自动回复 {len(reply_text)} chars 到 {candidate}（{used}）")
            # 记录到 outgoing_messages
            try:
                import sqlite3
                db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
                conn = sqlite3.connect(db_path, timeout=3)
                conn.execute(
                    "INSERT OR IGNORE INTO outgoing_messages (id, target_host, sender, message, sent_at, acked) VALUES (?,?,?,?,?,1)",
                    (str(uuid.uuid4()), candidate, f"{hostname} ({sender_nick})", reply_text,
                     datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
            return
        last_err_msg = err or "unknown"
        log(f"⚠️ 尝试 {candidate} 失败: {last_err_msg[:80]}")

    log(f"❌ 所有路径都失败: {last_err_msg}")
    # 写入 outgoing_messages（acked=0），供 _check_outgoing_retry 后续重试
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        conn = sqlite3.connect(db_path, timeout=3)
        conn.execute(
            "INSERT OR IGNORE INTO outgoing_messages (id, target_host, sender, message, sent_at, acked) VALUES (?,?,?,?,?,0)",
            (str(uuid.uuid4()), target_host, f"{hostname} ({sender_nick})", reply_text,
             datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
        )
        conn.commit()
        conn.close()
        log(f"📝 已记录失败 auto-reply 到 outgoing 队列（等待重试）")
    except Exception:
        pass



# 文件传输去重缓存
_FILE_XFER_CACHE = {}  # filename -> {retries, last_time}


def _handle_file_transfer(content, sender, mid):
    """处理 {(文件传输)} 控制消息，返回 True 表示已处理（跳过 Gateway）"""
    if FILE_MARKER_START not in content:
        return False

    try:
        meta = _parse_file_xfer(content)
    except Exception:
        return False

    action = meta.get("action", "")
    if not action:
        return False  # 没有 action 字段，不是有效的文件传输控制消息

    name = meta.get("name", "?")
    log(f"📁 文件传输 [{mid}] action={action} name={name}")

    if action == "confirm" and meta.get("auto_receive") == "true":
        target = meta.get("target_path", "")
        expected_size = int(meta.get("source_size", 0))
        target_abs = os.path.expanduser(target)

        if not os.path.exists(target_abs):
            log(f"⚠️ 文件未到达: {target_abs}")
            _file_xfer_respond(sender, "nack", name, reason="not_found")
            return True

        actual_size = 0
        if os.path.isfile(target_abs):
            actual_size = os.path.getsize(target_abs)
        elif os.path.isdir(target_abs):
            for dirpath, dirnames, filenames in os.walk(target_abs):
                for f in filenames:
                    try:
                        actual_size += os.path.getsize(os.path.join(dirpath, f))
                    except OSError:
                        pass

        if actual_size != expected_size:
            log(f"⚠️ 大小不匹配: expected={expected_size} actual={actual_size}")
            _file_xfer_respond(sender, "nack", name, reason="size_mismatch",
                               detail=f"expected={expected_size} actual={actual_size}")
            return True

        # sha256 校验
        sha256_expected = meta.get("sha256", "")
        if sha256_expected and os.path.isfile(target_abs):
            computed = _file_sha256(target_abs)
            if computed != sha256_expected:
                log(f"⚠️ sha256 不匹配")
                _file_xfer_respond(sender, "nack", name, reason="sha256_mismatch",
                                   computed_sha256=computed)
                return True

        log(f"✅ 文件验收通过: {name} ({actual_size} bytes)")
        _file_xfer_respond(sender, "ack", name)
        return True

    if action == "ack":
        log(f"✅ 对方已确认收到: {name}")
        return True

    if action == "nack":
        reason = meta.get("reason", "unknown")
        log(f"⚠️ 对方拒收: {name} reason={reason}")
        # 自动重试逻辑在 tether_send_file 中处理
        return True

    return False  # 非文件传输消息或未知 action，交回正常流程


def _parse_file_xfer(content):
    """从 {(文件传输)}...{(完)} 中解析键值对"""
    meta = {}
    start = content.find(FILE_MARKER_START)
    end = content.find(FILE_MARKER_END)
    if start < 0 or end < 0:
        return meta
    body = content[start + len(FILE_MARKER_START):end].strip()
    for line in body.split("\n"):
        line = line.strip()
        if "=" in line:
            k, v = line.split("=", 1)
            meta[k.strip()] = v.strip()
    return meta


def _file_sha256(path):
    """计算文件 sha256"""
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _file_xfer_respond(sender, action, name, reason="", detail="", computed_sha256=""):
    """发送文件传输确认消息回发送方"""
    hostname = __import__("socket").gethostname()
    nick = os.environ.get("TETHER_SENDER_NICK", hostname)

    lines = [FILE_MARKER_START, f"action={action}", f"name={name}"]
    if reason:
        lines.append(f"reason={reason}")
    if detail:
        lines.append(f"detail={detail}")
    if computed_sha256:
        lines.append(f"computed_sha256={computed_sha256}")
    lines.append(FILE_MARKER_END)
    msg = "\n".join(lines)

    # 从 sender 提取目标主机
    target_host = sender.split()[0] if sender else ""
    if not target_host or target_host in ("unknown",):
        target_host = os.environ.get("TETHER_PEER_HOST", "")
    if not target_host:
        log("⚠️ 无法确定回复目标")
        return

    payload = json.dumps({
        "from": f"{hostname} ({nick})",
        "sender": f"{hostname} ({nick})",
        "message": msg,
        "content": msg,
        "type": "info",
    }).encode()

    peer_url = f"http://{target_host}:{PEER_PORT}/message"
    try:
        req = urllib.request.Request(peer_url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        log(f"📤 文件传输确认 已发送 → {target_host}  msg_id={result.get('message_id','?')[:8]}")
    except Exception as e:
        log(f"⚠️ 文件传输确认发送失败: {str(e)[:60]}")


FILE_MARKER_START = "{(文件传输)}"
FILE_MARKER_END = "{(完)}"


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

            # 跳过 auto_reply 类型消息（旧格式安全兜底，新格式走 is_reply 逻辑）
            if msg.get("type") == "auto_reply":
                log(f"\u23ed {mid} 跳过（旧格式 auto_reply）")
                continue

            # 检查 is_reply 标记：回复消息处理但不 auto-reply 回去（防回环）
            is_reply = msg.get("is_reply", False)

            # TTL 防死循环：检查 relay 消息的 TTL
            ttl = msg.get("ttl")
            if ttl is not None:
                if ttl <= 0:
                    log(f"\u23ed {mid} 跳过（TTL={ttl}，已达零，防止消息循环）")
                    continue
                else:
                    # TTL 减 1 后转发
                    log(f"\u26a1 {mid} TTL={ttl} → TTL={ttl-1}")
                    msg["ttl"] = ttl - 1
            else:
                # 没有 ttl 字段的旧消息，首次经过此 watcher 时设置 ttl=1
                msg["ttl"] = 1
                log(f"\u26a1 {mid} 首次经过，设置 TTL=1")

            # 去重过滤：连续确认循环消息直接跳过（同 sender、含确认关键词、N 分钟内重复）
            skip_keywords = ["已清理", "无积压", "无需操作", "不回复以阻断", "等主人回来",
                            "不回复以阻断循环", "双方一致", "已对齐", "已确认",
                            "5分钟没收到对端新消息"]
            if any(kw in content for kw in skip_keywords):
                log(f"\u23ed {mid} 跳过（确认循环消息）")
                continue

            # 文件传输控制消息：直接处理，不走 Gateway
            if _handle_file_transfer(content, sender, mid):
                continue

            prompt = (
                f"[Tether] 来自 {sender}：\n{content}\n\n"
                f"这是另一台 Hermes agent 通过 Tether 发来的消息。\n"
                f"请理解内容并直接处理或回复。处理完成后给出总结。"
            )

            output = None

            if _is_gateway_alive():
                output, err = _gateway_chat(prompt, timeout=300)
                if err is None and output is not None:
                    has_out = bool(output)
                    log(f"✅ {mid} 处理完成 (Gateway, {len(output) if has_out else 0} chars)")
                else:
                    log(f"❌ Gateway 处理失败 ({err or 'no output'}), 消息跳过")
            else:
                log(f"❌ Gateway 不在线，消息跳过")

            # 汇报或自动回复：Report 发 notification，其他回发送方
            if output and sender:
                if _should_report(output):
                    log(f"🔔 {mid} 标记为 Report → 走通知")
                    _send_dingtalk(output)

                # is_reply 消息跳过 auto-reply，防止回环
                # 对端 watcher 已处理过，我们 Gateway 收到上下文即可继续推进
                if not is_reply:
                    _auto_reply(output, sender, msg.get("id", ""))
                else:
                    if output:
                        log(f"⏭ {mid} 跳过 auto-reply（is_reply 消息）")

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
    """检查 handoff 文件，有内容就通过 Gateway API 处理

    通过子线程调用 _gateway_chat()，不阻塞主循环（info 消息处理不受影响）。
    替代了原来的 hermes -z 子进程方案，不再有 OOM 风险。
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

    log(f"📋 发现 handoff from={sender}: {summary[:60]}...")

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

    # 子线程调用 Gateway API，不阻塞主循环
    def _run_handoff(sender=sender, msg_id=msg_id):
        output = None
        try:
            output, error = _gateway_chat(prompt)
            if output:
                log(f"✅ Handoff 处理完成 ({len(output)} chars)")
                _write_handoff_result(sender, summary[:40], output[:500])
            else:
                log(f"⚠️ Handoff Gateway 无输出: {error[:80] if error else 'unknown'}")
        except Exception as e:
            log(f"❌ Handoff 异常: {str(e)[:80]}")

        # Handoff 处理结果：Report 走通知，所有消息都 auto-reply 回发送方
        if output and sender:
            if _should_report(output):
                log(f"🔔 Handoff [{msg_id[:8]}] 标记为 Report → 走通知")
                _send_dingtalk(output)

            # 所有消息都 auto-reply 回发送方
            _auto_reply(output, sender, msg_id)

        # 标记本消息为已确认，防止 _recover_next_handoff 再次恢复同一消息
        _ack_handoff(msg_id)

        # 处理完一条后尝试恢复下一条积压 handoff
        _recover_next_handoff()

    t = threading.Thread(target=_run_handoff, daemon=True)
    t.start()
    log("🔀 Handoff 已交给子线程处理，继续轮询")


def _check_outgoing_retry():
    """检查 outgoing_messages 中 acked=0 且超过 30 秒的消息，自动重试"""
    try:
        import sqlite3
        import uuid as _uuid
        from datetime import datetime, timezone
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        conn = sqlite3.connect(db_path, timeout=3)
        rows = conn.execute(
            "SELECT id, target_host, message, sender FROM outgoing_messages "
            "WHERE acked=0 AND REPLACE(SUBSTR(sent_at, 1, 19), 'T', ' ') < datetime('now', '-30 seconds') "
            "ORDER BY sent_at ASC LIMIT 5"
        ).fetchall()
        conn.close()
    except Exception:
        return

    for msg_id, target_host, msg_text, sender in rows:
        # 跳过含非 ASCII 字符的目标（如中文昵称），直接标记 acked 防止循环重试
        if any(ord(c) > 127 for c in target_host):
            try:
                conn2 = sqlite3.connect(db_path, timeout=3)
                conn2.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (msg_id,))
                conn2.commit()
                conn2.close()
                log(f"⏭ 跳过不可送达目标: {target_host}（标记 acked）")
            except Exception:
                pass
            continue
        hostname = __import__("socket").gethostname()
        sender_nick = os.environ.get("TETHER_SENDER_NICK", hostname)
        payload = json.dumps({
            "from": f"{hostname} ({sender_nick})",
            "sender": f"{hostname} ({sender_nick})",
            "message": msg_text,
            "content": msg_text,
            "type": "info",
            "is_reply": True,
            "ttl": 1,
        }).encode()
        peer_url = f"http://{target_host}:{PEER_PORT}/message"
        ok, err = _try_post(peer_url, payload)
        if ok:
            try:
                conn = sqlite3.connect(db_path, timeout=3)
                conn.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (msg_id,))
                conn.commit()
                conn.close()
                log(f"♻️ 重试成功: {msg_id[:8]} → {target_host}")
            except Exception:
                pass
        else:
            log(f"⏳ 重试仍失败: {msg_id[:8]} → {target_host} ({err[:60]})")


def _check_handoff_timeout():
    """检查 outgoing 消息超时无回复，生成唤醒 handoff 并通知主人

    在 _self_heal() 中每15秒调用一次。
    - 查 outgoing 中 acked=0 且超过 TIMEOUT_MINUTES 的消息
    - 对每条超时消息，生成 [呼叫] 唤醒 handoff 重新发到对端
    - 同时发送通知到飞书/钉钉
    - 同一条消息 COOLDOWN 内不重复唤醒（防刷屏）
    """
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        if not os.path.isfile(db_path):
            return

        conn = sqlite3.connect(db_path, timeout=3)
        # 检查1：出站消息未送达（acked=0）超时
        rows = conn.execute(
            "SELECT id, target_host, message FROM outgoing_messages "
            "WHERE acked=0 "
            "AND REPLACE(SUBSTR(sent_at, 1, 19), 'T', ' ') < datetime('now', '-' || ? || ' minutes') "
            "ORDER BY sent_at ASC LIMIT 3",
            (str(_HANDOFF_TIMEOUT_MINUTES),)
        ).fetchall()

        conn.close()
    except Exception:
        return

    if not rows:
        return

    now = time.time()
    hostname = __import__("socket").gethostname()
    sender_nick = os.environ.get("TETHER_SENDER_NICK", hostname)
    for msg_id, target_host, msg_text in rows:
        # 去重：同一消息在 COOLDOWN 内不重复唤醒
        if msg_id in _HANDOFF_TIMEOUT_CACHE:
            if now - _HANDOFF_TIMEOUT_CACHE[msg_id] < _HANDOFF_TIMEOUT_COOLDOWN:
                continue
        _HANDOFF_TIMEOUT_CACHE[msg_id] = now

        # 清理过期缓存
        if len(_HANDOFF_TIMEOUT_CACHE) > 100:
            cutoff = now - _HANDOFF_TIMEOUT_COOLDOWN
            for k, v in list(_HANDOFF_TIMEOUT_CACHE.items()):
                if v < cutoff:
                    del _HANDOFF_TIMEOUT_CACHE[k]

        summary = (msg_text or "")[:100]
        # 跳过无法送达的目标（含非ASCII字符的主机名，如中文）
        if any(ord(c) > 127 for c in target_host):
            log(f"⏭ 跳过不可送达目标: {target_host}")
            # 标记为 acked=1 防止重复扫描
            try:
                _conn = sqlite3.connect(db_path, timeout=3)
                _conn.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (msg_id,))
                _conn.commit()
                _conn.close()
            except Exception:
                pass
            continue
        timeout_msg = (
            f"[呼叫] 等了{_HANDOFF_TIMEOUT_MINUTES}分钟没人回：{summary}"
        )
        log(f"📞 超时唤醒 #{msg_id[:8]} → {target_host}: {summary[:60]}")

        # 1. 发送唤醒 handoff 到对端 Tether
        payload = json.dumps({
            "from": f"{hostname} ({sender_nick})",
            "sender": f"{hostname} ({sender_nick})",
            "message": timeout_msg,
            "content": timeout_msg,
            "type": "info",
            "ttl": 2,
        }).encode()

        peer_url = f"http://{target_host}:{PEER_PORT}/message"
        # 如果有 fallback，也加到候选地址中
        candidates = [target_host]
        if PEER_FALLBACK_HOST and PEER_FALLBACK_HOST != target_host:
            candidates.append(PEER_FALLBACK_HOST)

        sent = False
        for host in candidates:
            url = f"http://{host}:{PEER_PORT}/message"
            ok, err = _try_post(url, payload)
            if ok:
                log(f"📞 唤醒 handoff 已发送到 {host}")
                sent = True
                break
            else:
                log(f"⏳ 唤醒发送到 {host} 失败: {err}")

        # 2. 通知主人
        notify_text = (
            f"⏰ [{sender_nick}] Tether 超时唤醒\n\n"
            f"消息 #{msg_id[:8]} 已发出 {_HANDOFF_TIMEOUT_MINUTES} 分钟未送达对端。\n"
            f"目标: {target_host}\n"
            f"摘要: {summary}\n"
            f"已发送唤醒 handoff: {'是' if sent else '否（发送失败）'}"
        )
        _send_notification(notify_text)

        # 通知后标记 acked=1，防止重复通知（通知一次就够）
        try:
            _conn = sqlite3.connect(db_path, timeout=3)
            _conn.execute("UPDATE outgoing_messages SET acked=1 WHERE id=?", (msg_id,))
            _conn.commit()
            _conn.close()
        except Exception:
            pass



def _cleanup_old_messages():
    """删除 7 天前的已确认消息（incoming + outgoing），控制 DB 无上限增长"""
    try:
        import sqlite3
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
        if not os.path.isfile(db_path):
            return
        conn = sqlite3.connect(db_path, timeout=3)
        keep_days = 7
        # 删除 7 天前的 acked 入站消息
        cur = conn.execute(
            "DELETE FROM messages WHERE acked=1 AND received_at < datetime('now', ? || ' days')",
            (f"-{keep_days}",)
        )
        del_in = cur.rowcount
        # 删除 7 天前的 acked 出站消息
        cur = conn.execute(
            "DELETE FROM outgoing_messages WHERE acked=1 AND sent_at < datetime('now', ? || ' days')",
            (f"-{keep_days}",)
        )
        del_out = cur.rowcount
        conn.commit()
        conn.close()
        if del_in > 0 or del_out > 0:
            log(f"🧹 DB 清理: 删除了 {del_in} 条入站 + {del_out} 条出站旧消息（> {keep_days} 天）")
    except Exception as e:
        log(f"DB 清理异常: {str(e)[:60]}")


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


def _write_heartbeat():
    """每轮主循环写入心跳文件：pid + 当前时间戳"""
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            json.dump({
                "pid": os.getpid(),
                "timestamp": time.time(),
                "time_iso": datetime.now(timezone.utc).isoformat(),
            }, f)
    except Exception:
        pass


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

        # 每轮写心跳文件，标记 watcher 存活
        _write_heartbeat()

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

        # 自愈巡检 + DB 清理（每 _SELF_HEAL_INTERVAL 秒执行一次）
        if now - last_heal_time >= _SELF_HEAL_INTERVAL:
            last_heal_time = now
            _self_heal()
            _cleanup_old_messages()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
