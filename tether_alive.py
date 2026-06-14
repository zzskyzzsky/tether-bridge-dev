#!/usr/bin/env python3
"""
Tether Alive — 对话活性监控 + 保活唤醒（v2）

独立于 tether_watcher.py 运行的外部守护进程。
检测 Tether 协作对话是否卡住（消息成功送达但双方停止推进），
通过发送唤醒消息打破僵局。

v2 改进：
  - 自动扫描 DB 中 {(新任务)}...{(完)} 格式，保存任务上下文
  - 唤醒时注入任务原文，让 agent 真正继续未完成的工作
  - 缩短检测间隔（5 分钟超时），尽早发现卡住

核心差异 vs tether watcher 内置的超时检测:
  内置 _check_handoff_timeout() 只查 outgoing WHERE acked=0（投递失败）
  本脚本查 messages 表中"来自对端的最后消息时间"（对话活性）

设计原则:
  - 不修改任何已有代码
  - 仅读取本地 SQLite + 通过 HTTP POST 发送唤醒消息
  - 状态持久化到 JSON 文件，崩溃后恢复

用法:
  python3 tether_alive.py                          # 单次检查
  python3 tether_alive.py --watch                  # 持续监控(默认间隔120s)
  python3 tether_alive.py --watch --interval 60    # 每60秒检查一次
"""

import json
import os
import re as _re
import sqlite3
import socket
import sys
import time
import urllib.request
from datetime import datetime, timezone

# ── 常量 ────────────────────────────────────────────────

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")
STATE_FILE = "/tmp/tether_alive_state.json"

DEFAULT_POLL_INTERVAL = 120        # 检查间隔（秒）
STALL_TIMEOUT_MINUTES = 5          # 对端多久无消息视为"可能卡住"
COOLDOWN_MINUTES = 10              # 同一方向不重复唤醒
ACTIVITY_WINDOW_MINUTES = 20       # 在此窗口内有对话记录才算"活跃过"

PEER_PORT = int(os.environ.get("TETHER_PEER_PORT", "9001"))
LOCAL_HOSTNAME = socket.gethostname()

# 从 .env 读取飞书通知 webhook
_NOTIFY_WEBHOOK_URL = ""
_env_path = os.path.expanduser("~/.hermes/.env")
if os.path.isfile(_env_path):
    try:
        with open(_env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("NOTIFY_WEBHOOK_URL="):
                    _NOTIFY_WEBHOOK_URL = line.split("=", 1)[1].strip()
                    break
    except Exception:
        pass

# 唤醒时包含的最近消息条数
CONTEXT_MESSAGE_COUNT = 3

# {(新任务)}...{(完)} 格式的正则
TASK_MARKER_PATTERN = _re.compile(
    r'\{\('
    r'新任务'
    r'\)\}'
    r'(.*?)'
    r'\{\('
    r'完'
    r'\)\}',
    _re.DOTALL
)

# {(已完成)} 标记正则
DONE_MARKER_PATTERN = _re.compile(
    r'\{\('
    r'已完成'
    r'\)\}'
)


def log(msg):
    print(f"[alive] {msg}", flush=True)


def _get_peer_host():
    """获取对端主机地址：环境变量优先"""
    return os.environ.get("TETHER_PEER_HOST", "").strip()


def _get_sender_nick():
    """获取本机昵称"""
    return os.environ.get("TETHER_SENDER_NICK", LOCAL_HOSTNAME)


MAX_WAKEUPS_PER_TASK = 3             # 每次任务最多发送带上下文的唤醒次数


def _load_state():
    """加载持久化状态"""
    default = {
        "last_peer_msg_time": None,
        "last_wakeup_time": 0.0,
        "last_wakeup_target": "",
        "conversation_was_active": False,
        "consecutive_stalls": 0,
        "current_task": "",          # v2: 上次检测到的 {(新任务)}...{(完)} 内容
        "wakeup_count": 0,            # 当前任务已发送的唤醒次数（达到上限后清空 task）
    }
    if not os.path.isfile(STATE_FILE):
        return default
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        for k in default:
            data.setdefault(k, default[k])
        return data
    except (json.JSONDecodeError, OSError):
        return default


def _save_state(state):
    """持久化状态"""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except OSError as e:
        log(f"⚠️ 状态文件写入失败: {e}")


def _get_db():
    """连接 SQLite，如果 DB 不存在返回 None"""
    if not os.path.isfile(DB_PATH):
        return None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=3)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _scan_new_task(conn):
    """扫描 messages 表，查找 {(新任务)}...{(完)} 格式，返回任务文本或空字符串"""
    try:
        rows = conn.execute(
            "SELECT message FROM messages "
            "WHERE message LIKE '%{(新任务)}%' "
            "ORDER BY received_at DESC LIMIT 5"
        ).fetchall()
        for row in rows:
            m = TASK_MARKER_PATTERN.search(row["message"])
            if m:
                task_text = m.group(1).strip()
                if task_text:
                    return task_text
        return ""
    except sqlite3.Error:
        return ""


def _check_task_done(conn):
    """扫描 messages + outgoing_messages 表，查找 {(已完成)} 标记

    任务完成时 agent 或主人可以发送包含 {(已完成)} 的消息，
    tether_alive 检测到后清空 current_task，停止带任务上下文的唤醒。
    """
    try:
        for table in ("messages", "outgoing_messages"):
            time_col = "received_at" if table == "messages" else "sent_at"
            rows = conn.execute(
                f"SELECT message FROM {table} "
                f"WHERE message LIKE '%{{(已完成)}}%' "
                f"ORDER BY {time_col} DESC LIMIT 3"
            ).fetchall()
            for row in rows:
                msg = row[0] if isinstance(row, tuple) else row["message"]
                if DONE_MARKER_PATTERN.search(msg):
                    return True
        return False
    except sqlite3.Error:
        return False


def _get_last_peer_message(conn):
    """查询 messages 表中来自对端的最新一条消息

    用排除法：去掉自己发的（含本机 hostname）、去掉 127.0.0.1
    剩下的就是对端发来的。

    返回 (received_at_iso, sender, message_preview, type) 或 None
    """
    try:
        row = conn.execute(
            "SELECT sender, message, received_at, type FROM messages "
            "WHERE sender NOT LIKE ? "
            "AND sender NOT LIKE '127.0.0.1%' "
            "AND sender != 'unknown' "
            "ORDER BY received_at DESC LIMIT 1",
            (f"%{LOCAL_HOSTNAME}%",)
        ).fetchone()
        if row:
            return (row["received_at"], row["sender"], row["message"], row["type"])
        return None
    except sqlite3.Error:
        return None


def _get_recent_message_count(conn, since_iso):
    """统计在 since_iso 之后有多少条消息（含收发双方）"""
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE received_at >= ?",
            (since_iso,)
        ).fetchone()
        if row:
            return row["cnt"]
        return 0
    except sqlite3.Error:
        return 0


def _get_recent_context_messages(conn, since_iso, limit=CONTEXT_MESSAGE_COUNT):
    """获取最近 N 条消息摘要（用于唤醒消息附加上下文）"""
    try:
        rows = conn.execute(
            "SELECT sender, substr(message, 1, 200) as msg, received_at, type "
            "FROM messages WHERE received_at >= ? "
            "ORDER BY received_at ASC LIMIT ?",
            (since_iso, limit)
        ).fetchall()
        return [(r["sender"], r["msg"], r["received_at"], r["type"]) for r in rows]
    except sqlite3.Error:
        return []

def _send_feishu_notification(task_preview, sender_host):
    """通过飞书 webhook 发送通知到群"""
    if not _NOTIFY_WEBHOOK_URL:
        return
    is_feishu = "feishu.cn" in _NOTIFY_WEBHOOK_URL.lower() or "larksuite" in _NOTIFY_WEBHOOK_URL.lower()
    text = (
        f"⚠️ Tether 任务已停止自动唤醒\n\n"
        f"来源：{sender_host}\n"
        f"任务摘要：{task_preview[:100]}\n"
        f"已尝试唤醒 3 次均无进展，已清空任务上下文。\n"
        f"请手动检查两端状态。"
    )
    if is_feishu:
        payload = {"msg_type": "text", "content": {"text": text}}
    else:
        payload = {"msgtype": "text", "text": {"content": text}}
    try:
        req = urllib.request.Request(
            _NOTIFY_WEBHOOK_URL,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read()
        log("✅ 飞书通知发送成功")
    except Exception as e:
        log(f"⚠️ 飞书通知发送失败: {str(e)[:60]}")



def _format_elapsed(seconds):
    """将秒数格式化为可读的 elapsed 字符串"""
    minutes = int(seconds / 60)
    if minutes < 60:
        return f"{minutes} 分钟"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours} 小时 {mins} 分钟"


def _send_wakeup(peer_host, state, stall_timeout=STALL_TIMEOUT_MINUTES):
    """发送带任务上下文的唤醒消息到对端 Tether

    消息格式：
      如果 state 中保存了 current_task：
        [任务重启] 检测到对话卡住了，检查你目前的进度，并继续以下任务直到完成。
        ---
        {任务原文}
        ---
        最后消息来自 {sender}，已于 {elapsed} 前发送。
      否则回退到通用 [呼叫-保活] 消息。
    """
    if not peer_host:
        log("⚠️ TETHER_PEER_HOST 未设置，无法发送唤醒")
        return False

    hostname = LOCAL_HOSTNAME
    nick = _get_sender_nick()
    task_text = state.get("current_task", "")

    if task_text:
        # 带任务上下文的唤醒
        wakeup_text = (
            "[任务重启] 检测到对话卡住了，检查你目前的进度，并继续以下任务直到完成。\n"
            "---\n"
            f"{task_text}\n"
            "---\n"
            f"最后一条消息来自 {state.get('last_wakeup_target', '对方')}，"
            f"已等待 {_format_elapsed(stall_timeout * 60)} 无回复。"
            "\n请检查实际完成情况，未完成的部分继续推进。完成后向主人汇报。"
        )
    else:
        # 没有任务上下文时的通用唤醒
        wakeup_text = (
            "[任务重启] 检测到对话卡住了，请检查目前的进度并继续推进。"
            "如果任务已完成请忽略此消息。"
        )

    payload = json.dumps({
        "from": f"{hostname} ({nick})",
        "sender": f"{hostname} ({nick})",
        "message": wakeup_text,
        "content": wakeup_text,
        "type": "info",
        "ttl": 2,
    }).encode()

    # 尝试主路径和回退路径
    candidates = [peer_host]
    fallback = os.environ.get("TETHER_PEER_FALLBACK_HOST", "")
    if fallback and fallback != peer_host:
        candidates.append(fallback)

    last_err = ""
    for host in candidates:
        url = f"http://{host}:{PEER_PORT}/message"
        try:
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
            msg_id = result.get("message_id", "?")
            log(f"📤 唤醒已发送 → {host}:{PEER_PORT}  msg_id={msg_id[:8]}"
                f"{'（带任务上下文）' if task_text else ''}")
            return True
        except Exception as e:
            last_err = str(e)[:80]
            log(f"⏳ 发送到 {host} 失败: {last_err}")
            continue

    log(f"❌ 所有候选路径都无法发送唤醒: {last_err}")
    return False


def check_and_alert(state, stall_timeout=STALL_TIMEOUT_MINUTES,
                    cooldown=COOLDOWN_MINUTES, activity_window=ACTIVITY_WINDOW_MINUTES):
    """核心检测逻辑。返回更新后的 state 字典。"""
    now = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    peer_host = _get_peer_host()

    conn = _get_db()
    if conn is None:
        log("⏳ DB 不存在（首次部署？）")
        if conn:
            conn.close()
        return state

    # v2: 每次检查时扫描新任务标记，提取任务上下文
    new_task = _scan_new_task(conn)
    if new_task:
        log(f"📋 检测到新任务定义（{len(new_task)} chars）")
        state["current_task"] = new_task
        state["wakeup_count"] = 0

    # v3: 检测 {(已完成)} 标记，清空 current_task
    if state.get("current_task") and _check_task_done(conn):
        log(f"📋 检测到 {(完成)} 标记，清空 current_task")
        state["current_task"] = ""
        state["wakeup_count"] = 0

    # 1. 查询来自对端的最后一条消息
    last_msg = _get_last_peer_message(conn)
    if last_msg is None:
        log("📭 尚无来自对端的消息，跳过检查")
        conn.close()
        state["conversation_was_active"] = False
        _save_state(state)
        return state

    last_time_iso, last_sender, last_content, last_type = last_msg
    log(f"📋 对端最后消息: [{last_time_iso[:19]}] {last_type} 来自 {last_sender[:20]}")

    # 2. 计算对端最后消息距今多少分钟
    try:
        if last_time_iso.endswith("Z"):
            last_dt = datetime.fromisoformat(last_time_iso.replace("Z", "+00:00"))
        else:
            last_dt = datetime.fromisoformat(last_time_iso)
        elapsed_minutes = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0
    except (ValueError, TypeError):
        log(f"⚠️ 无法解析时间戳: {last_time_iso}，跳过")
        conn.close()
        _save_state(state)
        return state

    # 3. 判断是否在 activity_window 内有活跃对话
    activity_ago_iso = (datetime.now(timezone.utc).timestamp() - activity_window * 60)
    activity_ago_iso_str = datetime.fromtimestamp(activity_ago_iso, tz=timezone.utc).isoformat()
    recent_count = _get_recent_message_count(conn, activity_ago_iso_str)
    had_recent_activity = recent_count >= 3

    # 更新状态中的 last_peer_msg_time
    state["last_peer_msg_time"] = last_time_iso

    # 4. 多层判断：是否卡住？
    is_stalled = False
    stall_reason = ""

    if elapsed_minutes < stall_timeout:
        # 对端最近还有消息，正常
        state["conversation_was_active"] = True
        state["consecutive_stalls"] = 0
        log(f"✅ 正常（对端 {elapsed_minutes:.0f} 分钟前有消息）")
    else:
        # 对端超过 TIMEOUT 无消息
        if had_recent_activity or state.get("conversation_was_active", False):
            is_stalled = True
            stall_reason = (
                f"对端 {elapsed_minutes:.0f} 分钟无消息"
                f"（阈值 {stall_timeout} 分钟）"
                f"{'，最近有过活跃对话' if had_recent_activity else '，之前对话活跃过'}"
            )
            state["consecutive_stalls"] = state.get("consecutive_stalls", 0) + 1
            state["conversation_was_active"] = False
            log(f"⚠️ 可能卡住: {stall_reason}")
        else:
            state["conversation_was_active"] = False
            log(f"💤 对端 {elapsed_minutes:.0f} 分钟无消息（无近期活跃对话，跳过）")

    conn.close()

    # 5. 如果检测到卡住，发送唤醒
    if is_stalled and peer_host:
        cooldown_remaining = cooldown * 60 - (now - state.get("last_wakeup_time", 0))
        same_target = state.get("last_wakeup_target", "") == peer_host

        if cooldown_remaining > 0 and same_target:
            log(f"⏭ 跳过唤醒（冷却中，剩余 {cooldown_remaining:.0f} 秒）")
        else:
            # 检查当前任务是否超过最大唤醒次数
            if state.get("current_task") and state.get("wakeup_count", 0) >= MAX_WAKEUPS_PER_TASK:
                log(f"🔔 当前任务已唤醒 {state['wakeup_count']} 次（上限 {MAX_WAKEUPS_PER_TASK}），清空 current_task")
                # 通知主人
                task_preview = state.get("current_task", "")
                _send_feishu_notification(task_preview, LOCAL_HOSTNAME)
                state["current_task"] = ""
                state["wakeup_count"] = 0

            ok = _send_wakeup(peer_host, state, stall_timeout)
            if ok:
                state["last_wakeup_time"] = now
                if state.get("current_task"):
                    state["wakeup_count"] = state.get("wakeup_count", 0) + 1
                    log(f"📊 当前任务已唤醒 {state['wakeup_count']}/{MAX_WAKEUPS_PER_TASK} 次")
                state["last_wakeup_target"] = peer_host

        if state["consecutive_stalls"] >= 3:
            log(f"🔔 连续 {state['consecutive_stalls']} 次检测到卡住（可配置主人通知）")
    elif is_stalled and not peer_host:
        log("⚠️ 检测到卡住但 TETHER_PEER_HOST 未设置，无法发送唤醒")

    _save_state(state)
    return state


def watch_loop(interval=DEFAULT_POLL_INTERVAL,
               stall_timeout=STALL_TIMEOUT_MINUTES,
               cooldown=COOLDOWN_MINUTES,
               activity_window=ACTIVITY_WINDOW_MINUTES):
    """持续监控循环"""
    log(f"🐾 Tether Alive v2 启动 (间隔={interval}s, 超时={stall_timeout}min)")
    log(f"   冷却={cooldown}min, 活跃窗口={activity_window}min")
    log(f"   任务上下文提取: {'{(新任务)}...{(完)}'}")
    log(f"   DB={DB_PATH}")
    log(f"   唤醒目标={_get_peer_host() or '未设置(仅检查模式)'}")

    state = _load_state()
    if state.get("current_task"):
        log(f"   当前保存的任务: {state['current_task'][:60]}...")

    while True:
        try:
            state = check_and_alert(state, stall_timeout, cooldown, activity_window)
        except Exception as e:
            log(f"❌ 检查异常: {e}")
        time.sleep(interval)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Tether Alive v2 — 对话活性监控 + 带任务上下文的保活唤醒"
    )
    parser.add_argument("--watch", action="store_true",
                        help="持续监控模式（默认只跑一次）")
    parser.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL,
                        help=f"监控间隔秒数（默认 {DEFAULT_POLL_INTERVAL}s）")
    parser.add_argument("--timeout", type=int, default=STALL_TIMEOUT_MINUTES,
                        help=f"对端无消息超时分钟数（默认 {STALL_TIMEOUT_MINUTES}min）")
    parser.add_argument("--cooldown", type=int, default=COOLDOWN_MINUTES,
                        help=f"唤醒冷却分钟数（默认 {COOLDOWN_MINUTES}min）")

    args = parser.parse_args()

    if args.watch:
        watch_loop(args.interval, args.timeout, args.cooldown)
    else:
        state = _load_state()
        check_and_alert(state, args.timeout, args.cooldown)


if __name__ == "__main__":
    main()
