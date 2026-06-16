# Tether 文件传输设计（最终版）

> 基于 tp 和 mac 讨论达成的共识，2026-06-15 定稿。

## 原则

1. **信令与数据分离**——控制消息走 Tether（可追溯、防丢），文件字节走 `scp`（原生加密、高性能）
2. **不经过 relay**——文件通过 Tailscale P2P 直传，relay 只传控制消息
3. **目录是头等公民**——`scp -r` 原生支持目录递归上传，保持目录结构
4. **显式优于隐式**——所有行为由发送方标记控制，不做隐式大小阈值

## 架构

```
发送方                             接收方
  │                                  │
  ├── 控制消息 (Tether) ──────────→  │   {(文件传输)} action=send ...
  │                                  │
  ├── scp/rsync (Tailscale P2P) ──→  │   scp -r source/ user@host:target/
  │                                  │
  ├── 完成消息 (Tether) ──────────→  │   {(文件传输)} action=confirm ...
  │                                  │
  │←── 确认/拒绝 (Tether) ─────────┤   {(文件传输)} action=ack|nack ...
```

## 接口设计

```bash
# 发送文件/目录到对方
tether_send_file local_file.txt ~/downloads/

# 发送到指定主机
tether_send_file --host zzsky-mbp ./my_project/ ~/backups/

# 目录斜杠语义同 scp
tether_send_file ./my_project/ ~/backups/   # 目录内容 → ~/backups/
tether_send_file ./my_project ~/backups/    # 目录本身 → ~/backups/my_project

# 选项
--rsync         # 用 rsync 替代 scp（断点续传 + 进度显示）
--strict        # 启用 sha256 校验（接收方验证后回传比对）
--auto-receive  # 接收方 watcher 自动验收入库，不等待 agent 确认
--confirm       # 收发双方手动确认后才传输（适用于敏感文件）
```

## 控制消息格式

所有控制消息通过 Tether POST `/message` 发送，type=info，内容采用 `{(文件传输)}...{(完)}` 格式。

### action=send（传输前通知）

```
{(文件传输)}
action=send
type=file|dir
name=xxx
source_size=NNN
target_path=/abs/path     （接收方展开为绝对路径）
items=NNN                 （仅目录）
auto_receive=true|false
{(完)}
```

### action=confirm（传输完成）

```
{(文件传输)}
action=confirm
type=file|dir
name=xxx
source_size=NNN
target_path=/abs/path
sha256=hex                （仅 --strict 模式）
auto_receive=true|false
{(完)}
```

### action=ack/nack（接收方确认）

```
{(文件传输)}
action=ack                （验收通过）
type=file|dir
name=xxx
--- 或 ---
{(文件传输)}
action=nack               （验收失败）
reason=size_mismatch|sha256_mismatch|not_found
computed_sha256=hex       （仅 sha256 失败时）
```

### action=recv_ok（接收方最终确认）

```
{(文件传输)}
action=recv_ok
name=xxx
```

## 接收方处理（watcher）

`process_messages()` 中识别 `{(文件传输)}`：

- `action=send`：记录即将到来的文件，不做其他操作
- `action=confirm`：
  - 如果 `auto_receive=true`：自动检查文件存在 + 大小匹配
  - 如果 `--strict`：额外计算 sha256 比对
  - 结果：发送 `ack` 或 `nack` 给发送方
  - `nack` 时发送方自动重试，最多 3 次
- `action=ack`：发送方收到后标记传输成功
- `action=nack`：发送方重试（递增重试计数，<=3 时重新 scp）

## 关于 relay

**文件数据不经过 relay。** 控制消息走 Tether（可经过 relay），文件始终 P2P。

如果 Tailscale 直连不通，可退化为 `rsync -e "ssh -o ProxyJump=relay"`。

## 实现计划

1. `tether_send_file.py` — CLI 工具，负责控制消息 + scp 执行
2. `tether_watcher.py` — `process_messages()` 中识别 `{(文件传输)}` 并处理确认
3. `POST /file` 端点：不做，scp 覆盖全场景

## 安全隐患与防护

| 风险 | 防护 |
|------|------|
| 路径穿越 | 目标路径由接收方展开为绝对路径，拒绝 `../` 和符号链接穿越 |
| 文件覆盖 | 默认同 scp（覆盖），可选 `--no-clobber` |
| 并发冲突 | 同一目标路径串行传输，用文件锁 |
| 大目录无反馈 | 可选 `--rsync` / `--progress` |
| sshd 未配置 | 首次使用报未配置 SSH key 提示 |
