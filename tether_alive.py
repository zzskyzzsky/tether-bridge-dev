#!/usr/bin/env python3
"""
Tether Alive — 对话活性监控 + 保活唤醒

独立于 tether_watcher.py 运行的外部守护进程。
检测 Tether 协作对话是否卡住（消息成功送达但双方停止推进），
通过发送唤醒消息打破僵局。

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

安装为 systemd (可选):
  cp tether-alive.service ~/.config/systemd/user/
  systemctl --user daemon-reload
  systemctl --user enable --now tether-alive.service
"""

import json
import os
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
STALL_TIMEOUT_MINUTES = 25         # 对端多久无消息视为"可能卡住"
COOLDOWN_MINUTES = 20              # 同一方向不重复唤醒
ACTIVITY_WINDOW_MINUTES = 40       # 在此窗口内有对话记录才算"活跃过"

PEER_PORT = int(os.environ.get("TETHER_PEER_PORT", "9001"))
LOCAL_HOSTNAME = socket.gethostname()

# 唤醒时包含的最近消息条数
CONTEXT_MESSAGE_COUNT = 3


def log(msg):
    print(f"[alive] {msg}", flush=True)


def _get_peer_host():
    """获取对端主机地址：环境变量优先"""
    return os.environ.get("TETHER_PEER_HOST", "").strip()


def _get_sender_nick():
    """获取本机昵称"""
    return os.environ.get("TETHER_SENDER_NICK", LOCAL_HOSTNAME)


def _load_state():
    """加载持久化状态"""
    default = {
        "last_peer_msg_time": None,      # 上次看到对端消息的时间
        "last_wakeup_time": 0.0,         # 上次发送唤醒的时间戳(epoch)
        "last_wakeup_target": "",        # 上次唤醒的目标
        "conversation_was_active": False, # 之前是否活跃（用于检测"从活跃变静默"）
        "consecutive_stalls": 0,         # 连续检测到卡住的次数（含去重）
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


def _get_last_peer_message(conn):
    """查询 messages 表中来自对端的最新一条消息（排除 auto_reply）

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


def _send_wakeup(peer_host, context_messages=None, stall_timeout=STALL_TIMEOUT_MINUTES):
    """发送唤醒消息到对端 Tether

    用 type=info 直接 POST 到对端（不走 auto_reply 通道），
    这样对端 watcher 会取出并处理。
    """
    if not peer_host:
        log("⚠️ TETHER_PEER_HOST 未设置，无法发送唤醒")
        return False

    hostname = LOCAL_HOSTNAME
    nick = _get_sender_nick()

    # 构建唤醒消息
    wakeup_parts = [
        "[呼叫-保活] 检测到对话超过 {} 分钟无进展。".format(stall_timeout),
        "请检查待处理消息并继续推进工作。如果任务已完成请忽略此消息。",
    ]

    if context_messages:
        wakeup_parts.append("\n最近消息记录：")
        for sender, msg, ts, mtype in context_messages:
            short_sender = sender.split("(")[0].strip() if "(" in sender else sender
            preview = (msg[:100] + "...") if len(msg) > 100 else msg
            wakeup_parts.append(f"  [{ts[:19]}] {short_sender}: {preview}")

    wakeup_text = "\n".join(wakeup_parts)

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
            log(f"📤 唤醒已发送 → {host}:{PEER_PORT}  msg_id={msg_id[:8]}")
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

    # 1. 查询来自对端的最后一条消息
    last_msg = _get_last_peer_message(conn)
    if last_msg is None:
        log("📭 尚无来自对端的消息，跳过检查")
        conn.close()
        state["conversation_was_active"] = False
        return state

    last_time_iso, last_sender, last_content, last_type = last_msg
    log(f"📋 对端最后消息: [{last_time_iso[:19]}] {last_type} 来自 {last_sender[:20]}")

    # 2. 计算对端最后消息距今多少分钟
    try:
        # 支持多种 ISO 格式
        if last_time_iso.endswith("Z"):
            last_dt = datetime.fromisoformat(last_time_iso.replace("Z", "+00:00"))
        else:
            last_dt = datetime.fromisoformat(last_time_iso)
        elapsed_minutes = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0
    except (ValueError, TypeError):
        log(f"⚠️ 无法解析时间戳: {last_time_iso}，跳过")
        conn.close()
        return state

    # 3. 判断是否在 activity_window 内有活跃对话
    activity_ago_iso = (datetime.now(timezone.utc).timestamp() - activity_window * 60)
    activity_ago_iso_str = datetime.fromtimestamp(activity_ago_iso, tz=timezone.utc).isoformat()
    recent_count = _get_recent_message_count(conn, activity_ago_iso_str)
    had_recent_activity = recent_count >= 3  # 至少 3 条消息才算"有过对话"

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
            # 情况 A: 有过对话，现在停了 → 可能是卡住
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
            # 情况 B: 本来就没活跃对话，双方都休息 → 正常
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
            # 获取上下文消息
            since_iso = (datetime.fromtimestamp(
                time.time() - activity_window * 60, tz=timezone.utc
            ).isoformat())
            ctx = _get_recent_context_messages(
                None, since_iso, CONTEXT_MESSAGE_COUNT
            ) if had_recent_activity else []

            ok = _send_wakeup(peer_host, ctx, stall_timeout)
            if ok:
                state["last_wakeup_time"] = now
                state["last_wakeup_target"] = peer_host

        # 连续多次卡住 → 可以在这里扩展：通知主人
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
    log(f"🐾 Tether Alive 启动 (间隔={interval}s, 超时={stall_timeout}min)")
    log(f"   冷却={cooldown}min, 活跃窗口={activity_window}min")
    log(f"   DB={DB_PATH}")
    log(f"   唤醒目标={_get_peer_host() or '未设置(仅检查模式)'}")

    state = _load_state()

    while True:
        try:
            state = check_and_alert(state, stall_timeout, cooldown, activity_window)
        except Exception as e:
            log(f"❌ 检查异常: {e}")
        time.sleep(interval)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Tether Alive — 对话活性监控 + 保活唤醒"
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
