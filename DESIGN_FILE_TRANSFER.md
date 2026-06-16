# Tether 文件传输设计

## 原则

1. **信令与数据分离**——控制消息走 Tether（可追溯、防丢），文件字节走 `scp`（原生加密、高性能）
2. **不经过 relay**——文件直接在两端间通过 Tailscale P2P 传输，relay 只传递控制消息
3. **目录是头等公民**——`scp -r` 原生支持目录递归上传，保持目录结构

## 架构

```
发送方                         接收方
  │                              │
  ├── 控制消息 (Tether) ──────→  │   {(文件传输)} name=xxx target=xxx ...
  │                              │
  ├── 文件数据 (scp P2P) ─────→  │   scp -r source/ user@host:target/
  │                              │
  │←── 确认消息 (Tether) ───────┤   收到，校验通过
```

## 接口设计

```bash
# 发送文件
tether_send_file --host zzsky-mbp local_file.txt ~/downloads/

# 发送目录（注意斜杠语义同 scp）
tether_send_file --host zzsky-mbp ./my_project/ ~/backups/       # 目录内容 → ~/backups/
tether_send_file --host zzsky-mbp ./my_project ~/backups/        # 目录本身 → ~/backups/my_project
```

## 控制消息格式

发送方先通过 Tether POST 一条 info 消息：

```json
{
  "from": "zzskyTPG3 (tp-小钉hermes)",
  "message": "{(文件传输)}
type=dir
name=my_project
target=~/backups/
size=1048576
items=12
sha256=可选
{(完)}"
}
```

接收方 watcher 收到后：
1. 从 `{(文件传输)}` 中提取元信息
2. 等待 scp 传输完成
3. 如果提供了 sha256 则校验
4. 发送一条确认消息

## 关于 relay

**文件数据不经过 relay。** 原因：

| 方式 | 问题 |
|------|------|
| 全量代收再转发 | relay 磁盘有限，大文件撑爆；延迟翻倍 |
| 流式 proxy | 实现复杂；relay 带宽成本高 |

**设计决定：** 控制消息走 Tether（可经过 relay），文件始终 P2P。

如果 Tailscale 直连不通（如两端在不同 NAT 后），可退化为 `rsync -e "ssh -o ProxyJump=relay"`，但这属于网络层问题，不在 tether 协议层解决。

## 安全隐患与防护

| 风险 | 防护 |
|------|------|
| 路径穿越 | 目标路径限制在 `~/` 内，拒绝绝对路径和 `../` |
| 文件覆盖 | 默认行为同 scp（覆盖），可选 `--no-clobber` |
| 并发冲突 | 同一目标串行传输，用文件锁 |
| 大目录无反馈 | 可选 `--progress` 显示进度 |

## 实现计划

1. `tether_send_file.py` — CLI 工具，负责控制消息 + scp 执行
2. `tether_watcher.py` — `process_messages()` 中识别 `{(文件传输)}` 并处理确认
3. 可选：`tether_server.py` 新增 `POST /file` 端点，支持通过 HTTP POST 接收文件（不走 scp 的备选路径）
