# Tether v3 — 使用手册

## 快速开始

### 检查两端是否活着

```bash
# 检查本机 Tether 状态
curl http://127.0.0.1:9001/status

# 期望输出：
# {
#   "hostname": "zzsky-mbp",
#   "messages_pending": 0,
#   "messages_unacked_outgoing": 0,
#   "watcher_alive": true,
#   "watcher_lag": 0.8,
#   "watcher_pid": 12345
# }

# 检查对方 Tether 状态
curl http://zzskytpg3:9001/status
```

### 发送消息

```bash
# 发送 info 消息（通知、确认等）
tether_send --host zzskytpg3 --type info "消息内容"

# 发送 handoff 消息（任务接力、代码 review）
tether_send --host zzskytpg3 --type handoff "需要对方处理的任务"

# 覆盖昵称
tether_send --host zzskytpg3 --nick mac-弟弟 --type info "消息"

# 指定端口
tether_send --host zzskytpg3 --port 9001 --type info "消息"

# 通过管道发送
echo "消息" | tether_send --host zzskytpg3

# 查看帮助
tether_send --help
```

### 查看消息历史

```bash
# 显示所有消息（收发双向）
tether-dump

# 只看最近 10 条
tether-dump -n 10

# 显示完整消息内容
tether-dump --full

# 实时监控（每 5 秒刷新）
tether-dump --watch

# 只看发送的消息
tether-dump --out

# 只看收到的消息
tether-dump --in

# 查看某天之后的消息
tether-dump --since 2026-06-06
```

---

## 日常巡检

```bash
# 1. 检查本机所有服务状态
systemctl --user status tether.service
systemctl --user status tether-watcher.service
systemctl --user status hermes-gateway.service

# 2. 检查 Tether 运行状态
curl -s http://127.0.0.1:9001/status | python3 -m json.tool

# 3. 检查消息队列
tether-dump -n 5

# 4. 检查对方是否在线
curl -s http://zzskytpg3:9001/ping
```

---

## 服务管理

```bash
# 重启 watcher（改代码后）
systemctl --user restart tether-watcher.service

# 重启 tether server
systemctl --user restart tether.service

# 重启 gateway
systemctl --user restart hermes-gateway.service

# 查看 watcher 实时日志
journalctl --user -u tether-watcher.service -f

# 查看 tether server 日志
journalctl --user -u tether.service -f

# daemon-reload（改了 .service 文件后）
systemctl --user daemon-reload
```

---

## 服务文件位置

```
~/.config/systemd/user/tether.service
~/.config/systemd/user/tether-watcher.service
~/.config/systemd/user/hermes-gateway.service
```

## 代码位置

```
~/.hermes/tether/
├── tether_server.py      # HTTP 服务
├── tether_watcher.py     # 消息处理守护进程
├── tether_send.py        # CLI 发送工具
├── tether_dump.py        # CLI 消息查看工具
├── tether.db             # SQLite 消息数据库
├── DESIGN.md             # 设计文档
└── .env                  # 环境变量配置
```

---

## 消息类型说明

| 场景 | 用什么类型 | 注意事项 |
|------|-----------|---------|
| "帮我 review 这段代码" | `handoff` | 对方 watcher 会用 hermes-z 完整处理 |
| "收到，确认通过" | `info` | 简单通知，对方 watcher 快速处理 |
| "测试通过，可以汇报了" | `info` | 可加 `[REPORT]` 前缀触发钉钉通知 |
| "任务完成，最终总结" | `handoff` + 内容含 `[REPORT]` | 结果同时走 auto-reply + 钉钉通知 |

---

## 故障排查

### Watcher 日志报 surrogate 错误

```
轮询异常: 'utf-8' codec can't encode character '\udccb' in position ...
```

修复：在 `tether_watcher.py` 中将 `\udccb` 替换为 `📋` 等安全字符，重启 watcher。

### 自动回复报 latin-1 错误

```
⏰ 自动回复失败: 'latin-1' codec can't encode characters in position 3-4
```

原因：发送方 sender 为纯中文昵称（如 `tp-小钉hermes`），`split()[0]` 拿到中文拼到 URL 中导致 urllib 报错。
修复：检查 target_host 是否含非 ASCII 字符，有则回退到 `TETHER_PEER_HOST` 环境变量。

### 消息积压不处理

```bash
# 1. 检查 watcher 是否运行
systemctl --user status tether-watcher.service

# 2. 检查 Tether server 是否运行
systemctl --user status tether.service

# 3. 查看积压数量
curl -s http://127.0.0.1:9001/status

# 4. 重启 watcher
systemctl --user restart tether-watcher.service
```

### Watcher 无限回环

症状：两端 watcher 不断互相发确认消息，DB 中消息数快速增长。

修复：P0-1 已修复此问题（type=auto_reply 跳过处理）。如果再次出现：
1. 手动停掉一端 watcher: `systemctl --user stop tether-watcher.service`
2. 清理积压: `sqlite3 ~/.hermes/tether/tether.db "DELETE FROM messages"`
3. 重启: `systemctl --user start tether-watcher.service`
