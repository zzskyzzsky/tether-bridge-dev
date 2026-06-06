# Tether — 使用手册

> 两个 Hermes 实例之间的可靠消息通道。极简、自愈、零运维。

## 目录

1. [安装](#1-安装)
2. [配置](#2-配置)
3. [启动](#3-启动)
4. [CLI 参考](#4-cli-参考)
5. [API 参考](#5-api-参考)
6. [日常运维](#6-日常运维)
7. [故障排查](#7-故障排查)

---

## 1. 安装

### 前置条件

- 两台机器已安装 Tailscale（互 ping 通）
- Python 3.9+
- user systemd（现代 Linux 发行版默认支持）

### 步骤

```bash
cd ~/.hermes/tether

# 1. 创建虚拟环境
python3 -m venv venv
venv/bin/pip install flask

# 2. 设为可执行
chmod +x tether_server.py tether_watcher.py tether_send.py tether_dump.py

# 3. 安装 systemd 服务
mkdir -p ~/.config/systemd/user
cp tether.service ~/.config/systemd/user/tether.service
cp tether-watcher.service ~/.config/systemd/user/tether-watcher.service

# 4. 编辑配置（见下文「配置」节）
# 向两个 service 的 [Service] 段添加 Environment= 行
```

---

## 2. 配置

### 环境变量

部署后需要手动向 Systemd Service 文件添加环境变量。

**`~/.config/systemd/user/tether.service`** 的 `[Service]` 段需添加：

```ini
Environment=GATEWAY_URL=http://127.0.0.1:8642
Environment=TETHER_PEER_HOST=对方主机名   # tp 侧: zzsky-mbp, mac 侧: zzskytpg3
```

**`~/.config/systemd/user/tether-watcher.service`** 的 `[Service]` 段需添加：

```ini
Environment=GATEWAY_URL=http://127.0.0.1:8642
Environment=TETHER_PEER_HOST=对方主机名
Environment=TETHER_SENDER_NICK=本机昵称   # tp 侧: tp-哥哥, mac 侧: mac-弟弟
```

> 注意：`~/.hermes/tether/` 下的源文件不含 Environment 行（保持 git 仓库可移植），
> 每次 `cp` 覆盖后需要手动添加。详见 `config.env.template`。

### 别名（可选）

```bash
alias tether-send='python3 ~/.hermes/tether/tether_send.py'
alias tether-dump='python3 ~/.hermes/tether/tether_dump.py'
```

---

## 3. 启动

```bash
# 重新加载 systemd
systemctl --user daemon-reload

# 启动服务（首次）
systemctl --user enable --now tether.service
systemctl --user enable --now tether-watcher.service

# 开机自启
loginctl enable-linger

# 验证
curl -s http://127.0.0.1:9001/ping
curl -s http://127.0.0.1:9001/status
```

### 重启

```bash
# 正常重启（两个服务都重启）
systemctl --user restart tether.service tether-watcher.service

# 如果 9001 端口被占用（重启不干净）
systemctl --user stop tether.service
ss -tlnp | grep 9001   # 确认端口释放
systemctl --user start tether.service tether-watcher.service
```

---

## 4. CLI 参考

### tether-send — 发送消息

```bash
# 基本用法
tether_send.py "消息内容"                     # 默认 info 类型 → TETHER_PEER_HOST

# 指定目标
tether_send.py --host zzsky-mbp "Hello"       # Tailscale 主机名
tether_send.py --host 100.81.192.38 "Hello"   # 直接 IP

# 消息类型
tether_send.py --type info "通知一下"
tether_send.py --type handoff "执行这个任务"   # hermes -z 完整处理

# 进阶选项
tether_send.py --port 9001 "指定端口"
tether_send.py --nick "我的昵称" "覆盖昵称"

# 管道/重定向
echo "多行内容" | tether_send.py
tether_send.py --type handoff < task_file.md

# 查看帮助
tether_send.py --help
```

**参数列表：**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--host, -h` | 目标主机名或 IP | `TETHER_PEER_HOST` 环境变量 |
| `--port, -p` | 目标端口 | 9001（或 `TETHER_PEER_PORT`） |
| `--type, -t` | 消息类型：`info` 或 `handoff` | info |
| `--nick` | 发送方昵称 | `TETHER_SENDER_NICK` |
| `--help` | 显示帮助 | — |

> **安全机制**：白名单参数解析——只认 `--host`/`--type`/`--port`/`--nick`，
> 其余 `--xxx`（如 `--sender mac-弟弟`）静默跳过。防止 AI 误传多余参数吃掉消息内容。

### tether-dump — 查看消息记录

```bash
# 最近消息
tether_dump.py -n 10

# 完整内容
tether_dump.py --full

# 时间筛选
tether_dump.py --since '2026-06-06'

# 只看单向
tether_dump.py --out                 # 只看发出的
tether_dump.py --in                  # 只看收到的

# 实时监控
tether_dump.py --watch              # 每 5 秒刷新
```

**参数列表：**

| 参数 | 说明 |
|------|------|
| `-n, --limit` | 只显示最近 N 条 |
| `--since` | 只显示此时间之后的（ISO 格式） |
| `--full` | 显示完整消息内容 |
| `--watch` | 实时监控（每 5 秒刷新） |
| `--out` | 只看发送的消息 |
| `--in` | 只看收到的消息 |

---

## 5. API 参考

Tether Server 监听 `0.0.0.0:9001`，提供以下 HTTP 端点：

### `GET /ping`

健康检查。返回 `{"pong": true, "hostname": "...", "time": "..."}`。

### `GET /health`

服务可用性。返回 `{"status": "ok", "hostname": "..."}`，HTTP 200。

### `GET /status`

消息状态 + watcher 心跳。

```json
{
  "hostname": "zzskyTPG3",
  "messages_pending": 0,
  "messages_unacked_outgoing": 0,
  "watcher_alive": true,
  "watcher_pid": 12345,
  "watcher_lag": 0.8,
  "time": "2026-06-07T..."
}
```

`watcher_lag` > 15 秒或 `watcher_alive=false` 表示 watcher 可能已死亡。

### `POST /message`

接收消息。

**Body** (JSON):

| 字段 | 必填 | 说明 |
|------|------|------|
| `message` 或 `content` | 是 | 消息内容（`message` 优先） |
| `sender` 或 `from` | 否 | 发送方标识（`from` 优先） |
| `type` | 否 | `info`（默认）或 `handoff` |

**返回：** `{"status": "ok", "message_id": "uuid"}`

### `GET /messages`

拉取待处理消息。

| 参数 | 默认 | 说明 |
|------|------|------|
| `ack` | `1` | 是否自动标记已读 |
| `type` | `info` | 消息类型过滤，`all` 返回全部 |

### `POST /ack`

标记出站消息为已确认。

**Body:** `{"message_ids": ["uuid1", "uuid2"]}`

### `GET /pending`

查看本机已发送但未被对方 ack 的消息。

---

## 6. 日常运维

### 查看状态

```bash
# 两端各查一次
curl -s http://127.0.0.1:9001/status
curl -s http://${TETHER_PEER_HOST}:9001/status

# 查看日志
journalctl --user -u tether-watcher.service --no-pager -n 20
journalctl --user -u tether.service --no-pager -n 20
```

### 监控消息

```bash
# 实时监控
tether-dump --watch

# 检查是否有积压
curl -s http://127.0.0.1:9001/status | python3 -m json.tool
curl -s http://${TETHER_PEER_HOST}:9001/status | python3 -m json.tool
```

消息积压**单调递增**说明对方 watcher 可能挂了。

### 检查 watcher 心跳

```bash
cat /tmp/tether_watcher_heartbeat.json
# {"pid": 12345, "timestamp": 1234567890.0, "time_iso": "..."}
```

时间戳在 15 秒内的表示 watcher 存活。

### DB 管理

```bash
# 查看 DB 大小
ls -lh ~/.hermes/tether/tether.db

# 统计消息数（见表）
# 自动清理：watcher 每 15 秒删除 7 天前的 acked 消息
```

### 配置检查

```bash
# 查看已安装 service 的环境变量
systemctl --user show tether-watcher.service -p Environment
systemctl --user show tether.service -p Environment
```

---

## 7. 故障排查

### 消息发不出去

```
tether_send.py --host 100.81.192.38 "test"
# 如果卡住/失败 →
```

**检查链路：**

```bash
# 1. Tailscale 通吗？
ping 100.81.192.38

# 2. 对方 Tether Server 活着吗？
curl -s http://100.81.192.38:9001/ping

# 3. 不用代理的路径
http_proxy="" curl -s http://100.81.192.38:9001/ping
```

### 消息已发对方不收

消息已送达 Server（看 message_id）但 watcher 没处理：

```
# 查对方 /status，看 messages_pending 是否在下降
# 如果 pending 单调递增 → watcher 死了
```

**恢复**：对方 watcher 的 systemd `Restart=always` 会自动重启，
重启后 `_recover_stale_handoffs()` 会处理积压。

### 状态显示 pending=0 但感觉有消息

Watcher 的 `?ack=1` 自动标记已读。**真实信号是 notify 文件**：

```bash
cat /tmp/tether_notify.json
# 有内容 + 时间戳晚于上次检查 = 有新消息
```

### 查 peer watcher 是否死亡

```bash
# 积压在增长吗？
watch -n 2 'curl -s http://100.81.192.38:9001/status'

# 停止发送——消息已在对方 SQLite，等 watcher 恢复后自动处理
# 不要一直发，堆积只会加重恢复负担
```

### Gateway 挂了怎么办

此时 info 消息自动走 hermes -z 回退。Gateway 重启后恢复正常。
Watcher 的 `_self_heal()` 每 15 秒检查一次并尝试重启：

```bash
# 手动重启也行
systemctl --user restart hermes-gateway.service
```

---

## 通信纪律

1. **不要等对方。** 发完 Tether 后做你能独立推进的事，同时轮询收件箱
2. **不要通过 DingTalk @ 对方** — 机器人收不到群消息
3. **优先检查 notify 文件**，而非 `/status`
4. **Tether 协作 session 用 DeepSeek**，单聊可用 Agnes
5. **修改 server.py 后两服务都重启**——只重启 watcher 不够
