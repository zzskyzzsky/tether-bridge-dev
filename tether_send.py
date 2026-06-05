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
import sys
import urllib.request
import urllib.error

SENDER_NAME = socket.gethostname()
ENV_HOST_KEY = "TETHER_PEER_HOST"
DEFAULT_PORT = 9001
DEFAULT_TYPE = "info"
VALID_TYPES = {"info", "handoff"}


def parse_args():
    args = sys.argv[1:]
    host = None
    msg_type = DEFAULT_TYPE
    message = None

    i = 0
    while i < len(args):
        if args[i] in ("--host", "-h"):
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
        else:
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

    return host, msg_type, message


def send(host: str, msg_type: str, message: str):
    if not message:
        print("⚠️ 空消息，跳过发送", file=sys.stderr)
        sys.exit(0)

    # 解析 host（支持 hostname/Tailscale MagicDNS，不包含 port）
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

    url = f"http://{target_host}:{port}/message"

    # JSON body 同时携带 message 和 content 字段（兼容 v3 两种 receiver 实现）
    payload = json.dumps({
        "from": f"{SENDER_NAME} (tp-小钉hermes)",
        "sender": f"{SENDER_NAME} (tp-小钉hermes)",
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
    host, msg_type, message = parse_args()

    if not message:
        print("用法: tether_send.py [--host HOST] [--type info|handoff] <消息内容>", file=sys.stderr)
        print("       echo '消息' | tether_send.py [--host HOST]", file=sys.stderr)
        print("")
        print(f"环境变量 {ENV_HOST_KEY} 可设默认目标主机", file=sys.stderr)
        sys.exit(1)

    ok = send(host, msg_type, message)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
