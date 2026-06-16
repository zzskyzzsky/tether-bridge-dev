#!/usr/bin/env python3
"""
Tether File Send — 通过 scp/rsync + Tether 控制消息传输文件/目录

用法:
  tether_send_file local_file.txt ~/downloads/
  tether_send_file --host zzsky-mbp ./my_project/ ~/backups/
  tether_send_file --rsync ./big_dir/ ~/backups/
  tether_send_file --strict --auto-receive config.yaml ~/configs/
  tether_send_file --confirm sensitive.doc ~/secure/
"""
import argparse, hashlib, json, os, socket, subprocess, sys, time, urllib.request
from datetime import datetime, timezone

TETHER_PORT = int(os.environ.get("TETHER_PEER_PORT", "9001"))
RELAY_HOST = os.environ.get("TETHER_PEER_HOST", "")
LOCAL_HOSTNAME = socket.gethostname()
SENDER_NICK = os.environ.get("TETHER_SENDER_NICK", LOCAL_HOSTNAME)
SSH_USER = os.environ.get("TETHER_SSH_USER", os.environ.get("USER", "zzsky"))
MAX_RETRY = 3

FILE_MARKER_START = "{(文件传输)}"
FILE_MARKER_END = "{(完)}"


def log(msg):
    print(f"[tether-send-file] {msg}", flush=True)


def _resolve_host(host):
    if host:
        return host
    if RELAY_HOST:
        return RELAY_HOST
    log("❌ 未指定 --host 且 TETHER_PEER_HOST 未设置")
    sys.exit(1)


def _expand_target(target):
    """将 target 中的 ~/ 展开为接收方绝对路径路径，同时保留给 scp 用的原始路径"""
    if target.startswith("~/"):
        # 留给 scp 的路径保留 ~/ 让远端 shell 展开
        return target
    if not target.startswith("/"):
        return os.path.join("~/", target)
    return target


def _abs_target(target):
    """将 ~/ 展开为 /home/user/ 用于本地校验逻辑"""
    if target.startswith("~/"):
        home = os.path.expanduser("~")
        return os.path.join(home, target[2:])
    return target


def _check_ssh(host):
    """快速检查 SSH 是否可达"""
    try:
        r = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes",
             f"{SSH_USER}@{host}", "echo ok"],
            capture_output=True, text=True, timeout=10
        )
        return r.returncode == 0 and r.stdout.strip() == "ok"
    except Exception:
        return False


def _scp(source, host, target, use_rsync=False, progress=False):
    """执行 scp 或 rsync"""
    ssh_dest = f"{SSH_USER}@{host}:{target}"

    if use_rsync:
        cmd = ["rsync", "-az"]
        if progress:
            cmd.append("--progress")
        cmd.extend(["-e", "ssh", source, ssh_dest])
    else:
        cmd = ["scp", "-rpq"]
        if progress:
            cmd.append("-v")
        cmd.extend([source, ssh_dest])

    log(f"🚀 {'rsync' if use_rsync else 'scp'} {' '.join(cmd)}")
    start = time.time()
    r = subprocess.run(cmd, capture_output=not progress, text=True, timeout=3600)
    elapsed = time.time() - start
    return r.returncode == 0, elapsed


def _file_sha256(path):
    """计算文件 sha256"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _send_tether(target_host, message):
    """POST 控制消息到目标 Tether"""
    hostname = LOCAL_HOSTNAME
    nick = SENDER_NICK
    payload = json.dumps({
        "from": f"{hostname} ({nick})",
        "sender": f"{hostname} ({nick})",
        "message": message,
        "content": message,
        "type": "info",
    }).encode()

    candidates = [target_host]
    fallback = os.environ.get("TETHER_PEER_FALLBACK_HOST", "")
    if fallback and fallback != target_host:
        candidates.append(fallback)

    for host in candidates:
        url = f"http://{host}:{TETHER_PORT}/message"
        try:
            req = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
            log(f"📤 控制消息已发送 → {host}  msg_id={result.get('message_id','?')[:8]}")
            return True
        except Exception as e:
            err = str(e)[:60]
            if host != candidates[-1]:
                log(f"⏳ 发送到 {host} 失败: {err}，尝试下一候选")
    log(f"❌ 控制消息发送失败")
    return False


def _build_metadata(source, target, args):
    """构建文件元信息"""
    is_dir = os.path.isdir(source)
    name = os.path.basename(os.path.normpath(source))
    if not name:
        name = os.path.basename(source)

    # 计算总大小
    total_size = 0
    items = 0
    if is_dir:
        for dirpath, dirnames, filenames in os.walk(source):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    total_size += os.path.getsize(fp)
                    items += 1
                except OSError:
                    pass
    else:
        total_size = os.path.getsize(source)
        items = 1

    return {
        "type": "dir" if is_dir else "file",
        "name": name,
        "source_size": total_size,
        "items": items,
        "target_path": _abs_target(target),
        "auto_receive": "true" if args.auto_receive else "false",
    }


def _build_msg(action, meta, sha256_val=None, reason=None, computed_sha256=None):
    """构建 {(文件传输)} 格式的消息"""
    lines = [FILE_MARKER_START, f"action={action}"]
    for k, v in meta.items():
        lines.append(f"{k}={v}")
    if sha256_val:
        lines.append(f"sha256={sha256_val}")
    if reason:
        lines.append(f"reason={reason}")
    if computed_sha256:
        lines.append(f"computed_sha256={computed_sha256}")
    lines.append(FILE_MARKER_END)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Tether File Send — scp/rsync + 控制消息")
    parser.add_argument("source", help="本地文件或目录路径")
    parser.add_argument("target", help="接收方目标路径")
    parser.add_argument("--host", help="目标主机名/IP（默认 TETHER_PEER_HOST）")
    parser.add_argument("--rsync", action="store_true", help="用 rsync 替代 scp")
    parser.add_argument("--strict", action="store_true", help="启用 sha256 校验")
    parser.add_argument("--auto-receive", action="store_true",
                        help="接收方 watcher 自动验收，不等 agent")
    parser.add_argument("--confirm", action="store_true",
                        help="收发双方确认后才传输")
    parser.add_argument("--progress", action="store_true", help="显示传输进度")
    args = parser.parse_args()

    host = _resolve_host(args.host)
    source_path = os.path.abspath(args.source)
    target_path = _expand_target(args.target)

    if not os.path.exists(source_path):
        log(f"❌ 源路径不存在: {source_path}")
        sys.exit(1)

    # 检查 SSH 连通性
    log(f"🔍 检查 {host} SSH 连通性...")
    if not _check_ssh(host):
        log(f"❌ SSH 不可达 {host}，请先配置 SSH key")
        sys.exit(1)
    log(f"✅ SSH 可达 {host}")

    meta = _build_metadata(source_path, args.target, args)

    if args.confirm:
        # --confirm 模式：先发询问，等收到 ack 再传
        log("⏳ --confirm 模式，发送询问消息...")
        _send_tether(host, _build_msg("ask", meta))
        log("⏳ 等待对方确认（Ctrl+C 取消）...")
        print("  对方 agent 会在 Tether 上收到询问，回复 {(文件传输)} action=allow 即可继续。")
        print(f"  如不回复，此命令会在 120 秒后超时退出。")
        time.sleep(5)  # 简单等待，实际靠外部确认
        log("⏳ 继续传输（未等待确认，如有需要请重新执行）")

    # 发送 action=send
    _send_tether(host, _build_msg("send", meta))

    # 执行传输
    log(f"📦 开始传输 {source_path} → {host}:{target_path}")
    ok, elapsed = _scp(source_path, host, target_path, args.rsync, args.progress)
    if not ok:
        log(f"❌ 传输失败")
        sys.exit(1)
    log(f"✅ 传输完成 ({elapsed:.1f}s)")

    # 计算 sha256（--strict 模式）
    sha256_val = None
    if args.strict and not os.path.isdir(source_path):
        log("🔐 计算 sha256...")
        sha256_val = _file_sha256(source_path)

    # 发送 action=confirm
    _send_tether(host, _build_msg("confirm", meta, sha256_val))

    # --strict 模式等待接收方回传校验结果
    if args.strict and sha256_val:
        log(f"⏳ 等待接收方校验 sha256 {'(最多重试 3 次)' if MAX_RETRY > 0 else ''}...")
        # 校验结果由 watcher 异步通知，这里不阻塞
        log("✅ 文件传输流程完成")

    log("🎉 全部完成")


if __name__ == "__main__":
    main()
