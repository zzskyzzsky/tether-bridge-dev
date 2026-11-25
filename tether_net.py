#!/usr/bin/env python3
"""Peer 地址解析：Tailscale 主机名 → Tailscale IP，不依赖 DNS。

背景（2026-11-25，主人指出）：tp/mac 的 `tailscale accept-dns=false`，改 DNS 那次
之后 MagicDNS 就失效了 —— `getent hosts zzsky-mbp` 和全名 `*.tailc5fd51.ts.net`
都解析失败。而 Tailscale 分配 100.x 地址仅在节点重注册（管理台 Remove 重加、
tailscale reset/重装、换盘丢 node key、管理员改 IP pool）时才会变。

所以配置里写**主机名**（意图清晰），运行时用 `tailscale ip -4 <host>` 从本地 netmap
取地址（不走 DNS，离线也查得到），失败才回落到 LAST_KNOWN（运维记录，非事实源）。
这样节点重注册换了 IP 也能自愈，不需要改配置。
"""
import ipaddress
import re
import subprocess
import time

# 运维记录：最近一次已知地址。只作 tailscale CLI 不可用时的兜底，
# 不当事实源 —— 真相始终是 netmap。
LAST_KNOWN = {
    "zzsky-mbp": "100.81.192.38",
    "zzskytpg3": "100.102.54.90",
    "vps-tencent-1": "100.109.129.111",   # tailscale status 里的真实 HostName
    "vps-tencent": "100.109.129.111",     # 别名（旧写法）
    "154.8.143.218": "154.8.143.218",
}

CACHE_TTL = 60.0        # 秒；节点换 IP 的话最迟一分钟后生效
CLI_TIMEOUT = 3.0       # tailscale CLI 单次上限，绝不阻塞主循环
_cache = {}             # host(lower) -> (ip, monotonic_ts)

URL_RE = re.compile(r'^(?P<scheme>[a-z]+://)?(?P<host>[^/:]+)(?P<rest>:\d+)?(?P<tail>/.*)?$',
                    re.IGNORECASE)


def is_ip(s):
    """IPv4 / IPv6 字面量判定（IPv6 直接放行，不做解析）。"""
    s = (s or "").strip()
    if not s:
        return False
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False


def _run_tailscale(host):
    """`tailscale ip -4 <host>` → 首个 IP；失败返回 None（不抛异常）。"""
    try:
        p = subprocess.run(["tailscale", "ip", "-4", host],
                           capture_output=True, text=True, timeout=CLI_TIMEOUT)
    except (OSError, subprocess.SubprocessError):
        return None
    if p.returncode != 0:
        return None
    for line in (p.stdout or "").splitlines():
        cand = line.strip()
        if cand and is_ip(cand) and ":" not in cand:
            return cand
    return None


def tailscale_ip(host, runner=None):
    """带 TTL 缓存的 netmap 查询；runner 仅测试用。"""
    key = (host or "").strip().lower()
    if not key:
        return None
    now = time.monotonic()
    hit = _cache.get(key)
    if hit and now - hit[1] < CACHE_TTL:
        return hit[0]
    ip = (runner or _run_tailscale)(host.strip())
    if ip:
        _cache[key] = (ip, now)
    return ip


def resolve_peer(host, fallback="", runner=None):
    """主机名 → 可直连地址。已是 IP 或无法解析时原样返回。"""
    raw = (host or "").strip()
    if not raw:
        return (fallback or "").strip()
    if is_ip(raw):
        return raw
    ip = tailscale_ip(raw, runner=runner) or LAST_KNOWN.get(raw.lower()) or (fallback or "").strip()
    return ip or raw


def resolve_hostport(value):
    """解析 `host` / `host:port` / `http://host:port/path`，只替换其中的 host。"""
    raw = (value or "").strip()
    if not raw:
        return raw
    m = URL_RE.match(raw)
    if not m:
        return raw
    host = m.group("host")
    if is_ip(host) or host.lower() in ("localhost", "127.0.0.1"):
        return raw
    ip = resolve_peer(host)
    if not ip or ip == host:
        return raw
    return f"{m.group('scheme') or ''}{ip}{m.group('rest') or ''}{m.group('tail') or ''}"


def resolve_url(url):
    """同 resolve_hostport，语义别名（仅供可读性）。"""
    return resolve_hostport(url)


def for_log(host):
    """给日志用的一行说明：原始值 → 解析结果 + 来源。"""
    raw = (host or "").strip()
    if not raw:
        return "（未设置）"
    if is_ip(raw):
        return f"{raw}（已是 IP）"
    ip = tailscale_ip(raw)
    if ip:
        return f"{raw} → {ip}（tailscale netmap）"
    fb = LAST_KNOWN.get(raw.lower())
    if fb:
        return f"{raw} → {fb}（LAST_KNOWN 兜底，tailscale CLI 不可用）"
    return f"{raw}（无法解析，按原值使用）"
