# Tether — 设计文档

> 两个 Hermes 实例之间的可靠消息通道。

## 设计哲学

- **极简**：Server 只做存 SQLite + 写文件（~90 行）。不加任何保底层——每层保底都是新故障点
- **外置 Watcher**：独立 2s 轮询进程处理消息，不嵌入 Server
- **机制 > 规则**：用常驻守护进程自动处理，不用 session 级别的行为规则
- **不动 server 原则**：所有逻辑改动集中在 watcher，server 保持极简
- **不改 hermes 核心代码**：不改 hermes-agent 自有代码、不改插件

> 设计哲学来自 v2 的教训——补丁堆叠无法收敛，每加一层保底就多一个故障点。

## 架构总览

```
发方 POST ──→ Tether Server (Flask, 存DB)
                     │
            ┌────────┴────────┐
            │ type=handoff    │ type=info
            │                 │
            ↓                 ↓
    /tmp/tether_handoff   /tmp/tether_notify.json
    .json (每次覆盖)      (mtime 变化检测)
            │                 │
            └─────┬───────────┘
                  │
                  ↓
       tether_watcher.py (每 2 秒轮询)
                  │
         ┌────────┴──────────┐
         │  Gateway 存活?    │
         │  _is_gateway_     │
         │  alive() 3s       │
         └────────┬──────────┘
                  │
          ┌───────┴───────┐
          │ 存活          │ 死亡
          │               │
          ↓               ↓
    Gateway API →  直走子进程
    失败回退子进程  hermes -z
          │               │
          └───────┬───────┘
                  │
                  ↓
         _auto_reply() → urllib POST → 对方 Tether
          (自动回复，防回环、去重)
```

## 组件

| 组件 | 文件 | 职责 |
|------|------|------|
| **Tether Server** | `tether_server.py` | HTTP 服务，收消息 → 存 SQLite + 按类型写文件 |
| **Tether Watcher** | `tether_watcher.py` | 2 秒轮询，处理消息 → 回复发送方 |
| **tether-send** | `tether_send.py` | CLI 工具，手动发消息 |
| **tether-dump** | `tether_dump.py` | CLI 调试工具，查消息记录 |

## 消息类型

msg_type 字段决定消息的处理路由：

| type | 写入文件 | 处理方式 | 用途 |
|------|---------|---------|------|
| `info`（默认） | `/tmp/tether_notify.json` | Gateway 优先 → 失败回退 hermes -z | 简单通知、讨论 |
| `handoff` | `/tmp/tether_handoff.json` | 直接走 hermes -z（完整 agent + 工具） | 执行任务、代码修改 |
| `auto_reply` | 不写文件 | Watcher 跳过处理（防回环） | 自动回复内部标记 |

### 为什么 handoff 不走 Gateway

1. handoff 走 hermes -z 是独立子进程 session，不污染 Gateway 的 `tether-watcher` session 上下文
2. handoff 需要自主决策和工具执行，不宜在共享 session 中处理

## 消息流转

### 完整链路

1. **发送** → POST 到对方 Tether Server: `http://{TETHER_PEER_HOST}:9001/message`
2. **存储** → Server 写入 SQLite `messages` 表
3. **分流** → 按 type 写 `/tmp/tether_notify.json`（info）或 `/tmp/tether_handoff.json`（handoff）
4. **轮询** → Watcher 每 2 秒轮询两个文件
5. **处理** → info 走 Gateway→hermes -z；handoff 直走 hermes -z
6. **回复** → `_auto_reply()` 将 agent 输出 POST 回发送方
7. **ACK** → watcher 自动标记消息为 acked（`?ack=1`），handoff 成功后在 hermes -z 子线程中标记

### 自动回复流程

```
hermes -z/Gateway 输出
         │
         ↓
   _auto_reply(output, sender_info)
         │
         ├── 提取 sender → target_host
         ├── 跳过自己→自己（防回环）
         ├── DB 去重：30 秒内相同内容不发
         ├── 截断 4000 字符
         └── urllib POST → 对方 /message
```

## 数据库设计

SQLite，表结构：

```sql
-- 收到的消息
CREATE TABLE messages (
    id TEXT PRIMARY KEY,           -- UUID
    sender TEXT NOT NULL,          -- 发送方标识
    message TEXT NOT NULL,         -- 消息内容
    received_at TEXT NOT NULL,     -- ISO 时间戳
    type TEXT NOT NULL DEFAULT 'info',  -- info | handoff | auto_reply
    acked INTEGER NOT NULL DEFAULT 0    -- 0=未处理 1=已处理
);

-- 发出的消息
CREATE TABLE outgoing_messages (
    id TEXT PRIMARY KEY,           -- UUID
    target_host TEXT NOT NULL,     -- 目标主机
    sender TEXT NOT NULL,          -- 本机标识
    message TEXT NOT NULL,         -- 消息内容
    sent_at TEXT NOT NULL,         -- ISO 时间戳
    acked INTEGER NOT NULL DEFAULT 0    -- 0=未确认 1=已确认
);
```


### 自动清理

Watcher 每 15 秒调用 `_cleanup_old_messages()`，删除 7 天前的 acked 消息（入站 + 出站）。控制 DB 无上限增长。

### INSERT 注意事项

```python
# ✅ 必须用显式列名（ALERT TABLE ADD COLUMN 把新列追加到末尾）
INSERT INTO messages (id, sender, message, received_at, type, acked)
  VALUES (?, ?, ?, ?, ?, 0)

# ❌ VALUES(?,?,?,?,?,0) 会在有 type 列时映射错列
INSERT INTO messages VALUES (?, ?, ?, ?, ?, 0)
```

## 系统服务

两个独立的 user-mode systemd 服务：

| 服务 | 启动项 | 重启策略 | 依赖 |
|------|--------|---------|------|
| `tether.service` | `tether_server.py --port 9001` | `always`, 5s | `network-online.target`, `tailscaled.service` |
| `tether-watcher.service` | `tether_watcher.py` | `always`, 2s | `After=tether.service`（无 BindsTo） |

**设计决策**: `tether-watcher.service` 没有声明 `BindsTo=tether.service`。Server 和 Watcher 是完全独立的进程，任一崩溃不应连带杀死对方。`After=tether.service` 已保证启动顺序。

### 环境变量

必须在 `.config/systemd/user/tether*.service` 的 `[Service]` 段中设置：

| 变量 | 说明 | 示例(tp) | 示例(mac) |
|------|------|----------|----------|
| `GATEWAY_URL` | 本地 Gateway 地址 | `http://127.0.0.1:8642` | 同上 |
| `TETHER_PEER_HOST` | 对方 Tailscale 主机名 | `zzsky-mbp` | `zzskytpg3` |
| `TETHER_PEER_PORT` | 对方 Tether 端口 | `9001` | 同上 |
| `TETHER_SENDER_NICK` | 本机昵称 | `tp-哥哥` | `mac-弟弟` |

详见 `config.env.template`。

## 安全模型

- **Tailscale 隔离**：所有通信走 Tailscale 网络（100.x.x.x），不设 token 认证
- **无端口暴露**：Tether 只监听 `0.0.0.0:9001`，但 Tailscale 网络外不可达
- **不设认证**：Tailscale 已提供网络层隔离

## 错误处理与自愈

### Gateway 降级（P3-7）

```
process_messages() 处理每条消息前：
  1. _is_gateway_alive() — 3 秒探活
  2. 存活 → _gateway_chat()
  3. 死亡 → 直走 hermes -z 子进程（不等 300 秒超时）
```

### OOM 防护（P0-2）

- 每个 hermes -z 子进程通过 `preexec_fn=_limit_memory()` 设置 `RLIMIT_AS=2GB`
- `_detect_oom()` 检测 `returncode=-9`（SIGKILL）
- 同一 handoff 连续 3 次 OOM 触发链路重启

### Handoff 恢复（P0-2）

- Watcher 启动时调用 `_recover_stale_handoffs()` 扫描 SQLite 中 `acked=0 AND type='handoff'`
- 子线程处理完一条后调用 `_recover_next_handoff()` 链式推进
- Daemon 线程被杀（watcher 重启）导致的丢失由恢复函数补偿

### 自愈巡检

Watcher 每 15 秒执行 `_self_heal()`：
- 检查 Gateway 存活 → 死透则 `systemctl --user restart hermes-gateway.service`（含 30 秒防抖）
- DB 清理：同时执行 `_cleanup_old_messages()`

### 去重

- **自动回复去重**：30 秒内相同内容 → 同一目标不重复发送（查 outgoing_messages）
- **DingTalk 去重**：30 秒内相同 hash 不重复通知
- **确认循环跳过**：含"已清理"、"无积压"等关键词的消息被跳过

## 已知边界与陷阱

| 陷阱 | 影响 | 修复 |
|------|------|------|
| 大消息 handoff 截断 | >1000 字符内容被截断 | 用 type=info 或分段发送 |
| http_proxy 阻塞本地探活 | Gateway 误判为死亡 | watcher 顶部 `NO_PROXY=127.0.0.1,localhost` |
| User=zzsky 在 user-systemd | exit code 216/GROUP | 删除 `User=` 行 |
| 修改 server.py 只重启 watcher | handoff 双杀 | `systemctl --user restart tether.service tether-watcher.service` |
| watcher 自动 ACK 误导 | `/status pending=0` 不代表没消息 | 检查 notify 文件才是真实信号 |
| 消息字段兼容 | `from`/`sender`、`message`/`content` 送端不规范 | 接收端 `data.get("message") or data.get("content")` |

## 版本演进

- **v1**：内存存储，重启丢消息
- **v2**：SQLite + Worker + Gateway 集成（**废弃**）
- **v3（当前）**：极简中转 + 独立 watcher，P0-P3 全部完成
  - Server：~90 行，仅存 DB + 写文件
  - Watcher：2s 轮询 + Gateway 优先 + hermes -z 回退
  - 部署：user systemd，无需 sudo
