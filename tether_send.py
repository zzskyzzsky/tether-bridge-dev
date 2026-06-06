#!/usr/bin/env python3
"""
Tether Send — 极简跨机消息发送工具

发送消息到远端 Tether 服务的 /message 端点。

用法:
  tether_send.py "你好，请执行任务X"                        # 默认 info 类型
  tether_send.py --host 100.81.192.38 "Hello"               # 指定目标
  tether_send.py --type handoff "需要你执行的任务"           # handoff 类型
  tether_send.py --host mac --type handoff "任务详情"        # host 支持 Tailscale MagicDNS 名称
  echo "多行内容" | tether_send.py                           # 从 stdin 读取
  tether_send.py --host 100.81.192.38 --type handoff < file  # 从文件重定向

目标主机优先级：
  1. --host 参数
  2. TETHER_PEER_HOST 环境变量
  3. 默认 127.0.0.1（本地测试）
"""

import json
import os
import socket
import sqlite3
import sys
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.db")

SENDER_NAME = socket.gethostname()
SENDER_NICK = os.environ.get("TETHER_SENDER_NICK", "tp-小钉hermes")
ENV_HOST_KEY = "TETHER_PEER_HOST"
DEFAULT_PORT = int(os.environ.get("TETHER_PEER_PORT", "9001"))
DEFAULT_TYPE = "info"
VALID_TYPES = {"info", "handoff"}


def print_usage():
    """打印完整帮助信息"""
    print("用法: tether_send.py [选项] <消息内容>", file=sys.stderr)
    print("       echo '消息' | tether_send.py [选项]", file=sys.stderr)
    print("       tether_send.py --host <host> < file", file=sys.stderr)
    print("", file=sys.stderr)
    print("选项:", file=sys.stderr)
    print("  --help              显示此帮助", file=sys.stderr)
    print("  --host, -h <主机>   目标主机（Tailscale 主机名或 IP）", file=sys.stderr)
    print("  --port, -p <端口>   目标端口（默认 9001）", file=sys.stderr)
    print("  --type, -t <类型>   消息类型: info（默认）| handoff", file=sys.stderr)
    print("  --nick <昵称>       发送方昵称（覆盖环境变量 TETHER_SENDER_NICK）", file=sys.stderr)
    print("", file=sys.stderr)
    print("环境变量:", file=sys.stderr)
    print(f"  {ENV_HOST_KEY}     默认目标主机", file=sys.stderr)
    print(f"  TETHER_SENDER_NICK   发送方昵称", file=sys.stderr)
    print(f"  TETHER_PEER_PORT     目标端口", file=sys.stderr)


def parse_args():
    args = sys.argv[1:]
    host = None
    msg_type = DEFAULT_TYPE
    message = None
    nick = None
    port = None

    i = 0
    while i < len(args):
        if args[i] == "--help":
            print_usage()
            sys.exit(0)
        elif args[i] in ("--host", "-h"):
            i += 1
            if i >= len(args):
                print("❌ --host 缺少参数值", file=sys.stderr)
                sys.exit(1)
            host = args[i]
        elif args[i] in ("--type", "-t"):
            i += 1
            if i >= len(args):
                print("❌ --type 缺少参数值（info | handoff）", file=sys.stderr)
                sys.exit(1)
            t = args[i].lower()
            if t not in VALID_TYPES:
                print(f"❌ 无效 type: {t}，可选: info, handoff", file=sys.stderr)
                sys.exit(1)
            msg_type = t
        elif args[i] in ("--port", "-p"):
            i += 1
            if i >= len(args):
                print("❌ --port 缺少参数值", file=sys.stderr)
                sys.exit(1)
            try:
                port = int(args[i])
            except ValueError:
                print(f"❌ 无效端口: {args[i]}", file=sys.stderr)
                sys.exit(1)
        elif args[i] == "--nick":
            i += 1
            if i >= len(args):
                print("❌ --nick 缺少参数值", file=sys.stderr)
                sys.exit(1)
            nick = args[i]
        else:
            # 白名单：只认 --host/--type/--port/--nick，其余 --xxx 跳过自身和下一个 token
            if args[i].startswith("-"):
                # 跳过 --xxx 及其值（AI 经常误传 --sender mac-弟弟 等）
                i += 1  # skip flag name
                if i < len(args) and not args[i].startswith("-"):
                    i += 1  # skip flag value (unless next is also a flag)
                continue
            # 第一个非 flag 参数视为消息内容
            if message is None:
                message = args[i]
            # 忽略多余参数（防误输入不报错）
        i += 1

    # 从环境变量读取目标主机
    if host is None:
        host = os.environ.get(ENV_HOST_KEY, "127.0.0.1")

    # 如果没有参数消息，尝试读 stdin（管道/重定向）
    if message is None:
        if not sys.stdin.isatty():
            message = sys.stdin.read().strip()

    return host, msg_type, message, nick, port


def send(host: str, msg_type: str, message: str, port: int | None = None, nick: str | None = None):
    if not message:
        print("⚠️ 空消息，跳过发送", file=sys.stderr)
        sys.exit(0)

    # 解析 host（支持 hostname/Tailscale MagicDNS，不包含 port）
    if port is None:
        if ":" in host:
            target_host, port_str = host.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                print(f"❌ 无效端口: {port_str}", file=sys.stderr)
                sys.exit(1)
        else:
            target_host = host
            port = DEFAULT_PORT
    else:
        target_host = host

    # --nick 覆盖默认昵称
    sender_nick = nick or SENDER_NICK

    url = f"http://{target_host}:{port}/message"

    # JSON body 同时携带 message 和 content 字段（兼容 v3 两种 receiver 实现）
    payload = json.dumps({
        "from": f"{SENDER_NAME} ({sender_nick})",
        "sender": f"{SENDER_NAME} ({sender_nick})",
        "message": message,
        "content": message,
        "type": msg_type,
    }).encode()

    req = urllib.request.Request(url, data=payload, headers={
        "Content-Type": "application/json",
    })

    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
            msg_id = result.get("message_id", "?")
            print(f"✅ [{msg_type}] → {target_host}:{port}  message_id={msg_id[:8]}")
            # 记录到 outgoing_messages
            try:
                conn = sqlite3.connect(DB_PATH, timeout=3)
                conn.execute(
                    "INSERT OR IGNORE INTO outgoing_messages (id, target_host, sender, message, sent_at, acked) VALUES (?,?,?,?,?,1)",
                    (msg_id, target_host, f"{SENDER_NAME} ({SENDER_NICK})", message, datetime.now(timezone.utc).isoformat())
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
            return True
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            err = str(e)
            if attempt == 0:
                print(f"⏳ 发送失败，重试一次... ({err[:60]})", file=sys.stderr)
                continue
            print(f"❌ 发送失败: {err}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"❌ 异常: {e}", file=sys.stderr)
            return False

    return False


def main():
    host, msg_type, message, nick, port = parse_args()

    if not message:
        print_usage()
        sys.exit(1)

    ok = send(host, msg_type, message, port, nick)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
