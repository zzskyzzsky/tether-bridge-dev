# Tether 稳定性改进计划

## 优先级 1 — OOM 防护 + 心跳检测（MacBook 8GB 最脆弱）

- Watcher 每 30s 写心跳文件 `/tmp/tether_watcher_heartbeat`
- TP 端发消息后检查对方心跳，60s 无更新判定 watcher 挂了
- OOM 后 systemd 自动重启 watcher，但需能检测到"发生了 OOM"

## 优先级 2 — 发送确认 + 重试

- 发方 POST 消息后，收方 watcher 开始处理时回 `/ack`
- outgoing_messages 表记录已发未确认的消息
- 30s 无 ACK → 重发，3 次重试仍失败 → 写告警

## 优先级 3 — 崩溃告警

- Watcher 启动时记录事件到 SQLite `/events` 端点
- 异常退出在 systemd journal 可查
- `/status` 扩展显示健康度
