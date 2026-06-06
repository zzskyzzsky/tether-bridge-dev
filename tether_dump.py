#!/usr/bin/env python3
"""Tether 消息 Dump — 按时间顺序展示本地缓存的 Tether 消息。

用法:
  python3 ~/.hermes/tether/tether_dump.py
  python3 ~/.hermes/tether/tether_dump.py -n 20           # 只看最近 20 条
  python3 ~/.hermes/tether/tether_dump.py --since '2026-06-06'
  python3 ~/.hermes/tether/tether_dump.py --full           # 显示完整内容
  python3 ~/.hermes/tether/tether_dump.py --watch          # 实时监控（每 5 秒刷新）
"""

import sqlite3
import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone, timedelta

CST = timezone(timedelta(hours=8), "CST")
DB_PATH = os.path.expanduser("~/.hermes/tether/tether.db")

def parse_ts(ts_str):
    """解析各种格式的时间戳为 datetime"""
    if not ts_str:
        return None
    try:
        # ISO 格式: 2026-06-06T13:41:51.693660+00:00
        if 'T' in ts_str or ' ' in ts_str:
            sep = 'T' if 'T' in ts_str else ' '
            parts = ts_str.split(sep)
            if len(parts) >= 2:
                # 处理时区
                rest = parts[1]
                if rest.endswith('Z'):
                    rest = rest[:-1] + '+00:00'
                elif '+' in rest[6:] or '-' in rest[6:]:
                    pass  # 已有偏移
                else:
                    rest += '+08:00'  # 默认东八区
                ts_str = sep.join(parts[:1] + [rest])
            return datetime.fromisoformat(ts_str)
        return datetime.fromisoformat(ts_str)
    except Exception:
        return None

def fmt_ts(dt):
    """格式化为 CST 可读时间"""
    if dt is None:
        return "?"
    try:
        dt_cst = dt.astimezone(CST)
        return dt_cst.strftime("%m-%d %H:%M:%S.%f")[:15]
    except Exception:
        return str(dt)

def fmt_ago(dt):
    """显示相对时间"""
    if dt is None:
        return ""
    try:
        now = datetime.now(timezone.utc) if dt.tzinfo else datetime.now()
        delta = now - dt
        if delta.days > 0:
            return f" ({delta.days}d ago)"
        if delta.seconds > 3600:
            return f" ({delta.seconds//3600}h ago)"
        if delta.seconds > 60:
            return f" ({delta.seconds//60}m ago)"
        return f" ({delta.seconds}s ago)"
    except Exception:
        return ""

def truncate(text, max_len=120):
    """截断长文本，保留开头"""
    if not text:
        return ""
    text = str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."

def dump_messages(full=False, limit=None, since=None, direction='all'):
    """dump 消息"""
    if not os.path.exists(DB_PATH):
        print(f"❌ Tether DB 不存在: {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # ── 收到的消息 (incoming) ──
        if direction in ('all', 'in'):
            q = 'SELECT * FROM messages'
            params = []
            conds = []
            if since:
                conds.append('received_at >= ?')
                params.append(since)
            if conds:
                q += ' WHERE ' + ' AND '.join(conds)
            q += ' ORDER BY received_at ASC'
            if limit:
                # 用子查询取最后 N 条（按时间升序但只取最近）
                q = f'SELECT * FROM messages ORDER BY received_at DESC'
                if since:
                    q += f' WHERE received_at >= ?'
                    params = [since]
                q += f' LIMIT {limit}'
                # 再按时间升序排回来
                q = f'SELECT * FROM ({q}) sub ORDER BY received_at ASC'

            rows = conn.execute(q, params).fetchall()
            if rows:
                print(f"┌─ 收到的消息（均来自对方 Tether） ({len(rows)} 条) {'─'*30}")
                for r in rows:
                    ts = parse_ts(r['received_at'])
                    prefix = "IN " if r['type'] == 'info' else "HD "
                    raw_sender = r['sender'] or "?"
                    # 提取机器名和昵称
                    hostname_part = raw_sender.split('(')[0].strip() if '(' in raw_sender else raw_sender
                    nick_part = raw_sender.split('(')[1].rstrip(')').strip() if '(' in raw_sender else ""
                    sender_display = f"{nick_part:12s} @{hostname_part}"
                    print(f"│ {prefix} {fmt_ts(ts):15s}{fmt_ago(ts):8s} {r['type']:7s} {sender_display}")
                    msg = r['message'] or ''
                    if full:
                        print(f"│ {'─'*60}")
                        for line in msg.split('\n'):
                            print(f"│ {line}")
                        print(f"│ {'─'*60}")
                    else:
                        # 首行缩略
                        first_line = msg.split('\n')[0] if '\n' in msg else msg
                        if len(msg) > 120 or '\n' in msg:
                            print(f"│   {truncate(first_line, 100)}")
                            print(f"│   ... ({len(msg)} chars, {msg.count(chr(10))+1} lines)")
                        else:
                            print(f"│   {msg}")
                    print()
            else:
                if direction == 'in':
                    print("(暂无收到的消息)")

        # ── 发送的消息 (outgoing) ──
        if direction in ('all', 'out'):
            q = 'SELECT * FROM outgoing_messages'
            params = []
            conds = []
            if since:
                conds.append('sent_at >= ?')
                params.append(since)
            if conds:
                q += ' WHERE ' + ' AND '.join(conds)
            q += ' ORDER BY sent_at ASC'
            if limit:
                q = f'SELECT * FROM outgoing_messages ORDER BY sent_at DESC'
                if since:
                    q += f' WHERE sent_at >= ?'
                    params = [since]
                q += f' LIMIT {limit}'
                q = f'SELECT * FROM ({q}) sub ORDER BY sent_at ASC'

            rows = conn.execute(q, params).fetchall()
            if rows:
                print(f"├─ 发送的消息 ({len(rows)} 条) {'─'*40}")
                for r in rows:
                    ts = parse_ts(r['sent_at'])
                    target = r['target_host'] or "?"
                    nick = r['sender'] or "?"
                    ack_mark = "✓" if r['acked'] else "○"
                    print(f"│ {ack_mark} {fmt_ts(ts):15s}{fmt_ago(ts):8s} → {target:15s}")
                    msg = r['message'] or ''
                    if full:
                        print(f"│ {'─'*60}")
                        for line in msg.split('\n'):
                            print(f"│ {line}")
                        print(f"│ {'─'*60}")
                    else:
                        first_line = msg.split('\n')[0] if '\n' in msg else msg
                        if len(msg) > 120 or '\n' in msg:
                            print(f"│   {truncate(first_line, 100)}")
                        else:
                            print(f"│   {msg}")
                    print()
            else:
                if direction == 'out':
                    print("(暂无发送的消息)")

        print(f"└{'─'*56}")

        # 统计
        cnt_in = conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        cnt_out = conn.execute('SELECT COUNT(*) FROM outgoing_messages').fetchone()[0]
        cnt_unacked_in = conn.execute("SELECT COUNT(*) FROM messages WHERE acked=0").fetchone()[0]
        cnt_unacked_out = conn.execute("SELECT COUNT(*) FROM outgoing_messages WHERE acked=0").fetchone()[0]
        print(f"  📥 {cnt_in} (未处理 {cnt_unacked_in})  📤 {cnt_out} (未确认 {cnt_unacked_out})")

        conn.close()

    except Exception as e:
        print(f"❌ 读取失败: {e}")
        sys.exit(1)


def watch_mode(limit=5, full=False):
    """实时监控模式"""
    last_count = -1
    last_content = ""
    try:
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            print(f"⏳ Tether 监控 (每 5 秒刷新)  —  Ctrl+C 退出\n")
            dump_messages(full=full, limit=limit, direction='in')
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n监控已退出。")


def main():
    parser = argparse.ArgumentParser(
        description="Tether 消息 Dump — 查看本地缓存的 Tether 消息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例:
  %(prog)s                   显示所有消息
  %(prog)s -n 20            只看最近 20 条
  %(prog)s --since 2026-06-06  只看某天之后
  %(prog)s --full           显示完整消息内容
  %(prog)s --watch          实时监控模式（每 5 秒刷新）
  %(prog)s --out            只看发送的消息
""")
    parser.add_argument('-n', '--limit', type=int, default=None,
                        help='只显示最近 N 条消息')
    parser.add_argument('--since', type=str, default=None,
                        help='只显示此时间之后的消息 (ISO格式: 2026-06-06)')
    parser.add_argument('--full', action='store_true',
                        help='显示完整消息内容（默认只显示首行摘要）')
    parser.add_argument('--watch', action='store_true',
                        help='实时监控模式（每 5 秒刷新）')
    parser.add_argument('--out', action='store_true',
                        help='只看发送的消息')
    parser.add_argument('--in', dest='show_in', action='store_true',
                        help='只看收到的消息')
    args = parser.parse_args()

    # 确定方向
    if args.out:
        direction = 'out'
    elif not args.show_in:
        direction = 'all'
    else:
        direction = 'in'

    # 规范化 since 参数
    since = args.since
    if since and 'T' not in since and ' ' not in since:
        since = since + 'T00:00:00'

    if args.watch:
        watch_mode(limit=args.limit or 5, full=args.full)
    else:
        dump_messages(full=args.full, limit=args.limit, since=since, direction=direction)


if __name__ == '__main__':
    main()
