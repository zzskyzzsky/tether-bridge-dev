# Tether 协作诊断报告

> 日期：2026-06-13
> 作者：Claude
> 范围：Tether Bridge 全链路分析、根因修复、机制验证

---

## 目录

1. [问题一：协作卡死死锁](#问题一协作卡死死锁)
2. [问题二：中文编码导致自动回复失败](#问题二中文编码导致自动回复失败)
3. [问题三：超时唤醒机制失效](#问题三超时唤醒机制失效)
4. [问题四：新旧任务混淆](#问题四新旧任务混淆)
5. [问题五：Agent 幻觉](#问题五agent-幻觉)
6. [问题六：代码缺陷](#问题六代码缺陷)
7. [改进汇总](#改进汇总)
8. [未解决问题](#未解决问题)

---

## 问题一：协作卡死死锁

### 根因

两个 Hermes Agent 通过 Tether 通信时，消息流转如下：

```
Agent A → POST /message → Agent B 的 server
  → B 的 watcher 轮询 → Gateway 处理
  → _auto_reply() 发回 type=auto_reply
  → A 的 server: 立即 acked=1
  → A 的 watcher 轮询 → 取不到（acked=1，type=auto_reply 被过滤）
  → A 的 Gateway session 不知道 B 已回复
  → A 无法继续对话
  → 双方都认为"在等对方回复" → 卡死
```

**关键代码：** `tether_server.py:107-116`

`type=auto_reply` 的消息在 Server 层立即 `acked=1`，watcher 的 `/messages?ack=1` 取不到它。而 `process_messages()` 里也显式跳过 `type=auto_reply`。

### 修复

**commit `3719705`** — 改动两个文件：

**`tether_watcher.py` — `_auto_reply()`:**
- 发送类型从 `type=auto_reply` 改为 `type=info`
- 新增标记 `is_reply: true`

**`tether_watcher.py` — `process_messages()`:**
- 对端收到 `type=info` + `is_reply=true` 的消息时，不跳过，送 Gateway 处理
- 处理完后不 auto-reply 回去（防止回环）

**`tether_server.py` — `/message` 端点:**
- `in_reply_to` 的 outgoing ack 逻辑从 `msg_type == "auto_reply"` 分支移出，改为对所有消息类型统一处理

### 效果

```
Agent A → POST /message → Agent B 的 server
  → B 的 watcher 处理 → Gateway 输出
  → _auto_reply() 发回 type=info, is_reply=true
  → A 的 server: 正常存 DB（acked=0）
  → A 的 watcher 轮询 → 取到消息 → 送 Gateway
  → A 的 Gateway session 更新 → 可继续推进
  → 不再死锁 ✅
```

---

## 问题二：中文编码导致自动回复失败

### 根因

`_auto_reply()` 中从 `sender_info` 提取目标主机名：

```python
target_host = sender_info.split()[0] if sender_info else ""
```

当 `sender_info` 为纯中文昵称（如 `"tp-小钉hermes"`），`split()[0]` 得到 `"tp-小钉hermes"`，拼入 URL 后 urllib 报 `latin-1` 编码错误。

### 修复

**commit `8388ffa`** — `tether_watcher.py`：

- `_auto_reply()`：检测 `target_host` 含非 ASCII 字符时回退到 `TETHER_PEER_HOST` 环境变量
- `_check_outgoing_retry()`：跳过含中文的目标并标记 `acked=1`，防止旧数据循环重试

### 效果

```
⏭ target_host 含非 ASCII 字符 (tp-小钉hermes)，回退到 TETHER_PEER_HOST=154.8.143.218
📤 自动回复 405 chars 到 154.8.143.218（主路径）✅
```

---

## 问题三：超时唤醒机制失效

### 根因

内置 `_check_handoff_timeout()` 只检查 `outgoing_messages WHERE acked=0`。但协作卡死时消息已成功送达（`acked=1`），只是回复内容对 watcher 不可见。所以超时检测永远找不到匹配记录。

### 解决方案

**新增 `tether_alive.py`（外部保活守护进程）：**

- 独立于 watcher 运行，不修改任何已有代码
- 扫描 `messages` 表中"来自对端的最后消息时间"（对话活性），而非检查投递状态
- 检测到卡住时发送 `[任务重启]` + 任务原文的唤醒消息

### 演进

| 版本 | 改动 | commit |
|------|------|--------|
| v1 | 初始实现：25 分钟超时、`[呼叫-保活]` 通用消息 | — |
| v1.1 | 修复 `conn=None` 导致唤醒崩溃 | `88def08` |
| v2 | 5 分钟超时、`{(新任务)}` 扫描、唤醒带任务上下文 | `4c9401d` |
| v2.1 | 回退 Gateway 依赖，纯消息表扫描 | `8ec386b` |

### 参数

```
POLL_INTERVAL      = 120s    检查间隔
STALL_TIMEOUT      = 5 min   无消息多久视为卡住
COOLDOWN           = 10 min  同一方向不重复唤醒
ACTIVITY_WINDOW    = 20 min  活跃对话判定窗口
```

### 验证结论

```
TP alive 检测到 25 分钟无消息
  → 发送 [呼叫-保活] type=info → relay → Mac
  → Mac watcher 收到 → Gateway 处理 (166 chars) ✅
```

唤醒链路畅通，但通用消息（不带任务上下文）会被 agent 视为噪音不处理。v2 改为带 `{(新任务)}` 上下文的唤醒后有效。

---

## 问题四：新旧任务混淆

### 根因

watcher 使用固定 Gateway session：

```python
GATEWAY_SESSION = "tether-watcher"
url = f"{GATEWAY_URL}/api/sessions/tether-watcher/chat"
```

Gateway 端维护该 session 的完整 LLM 对话历史。旧任务的讨论（hermes_config）会残留在上下文中，新任务消息到达时 LLM 看到前后不一致的上下文。

### 解决方案

**`/new` + skill 方案：**

1. 编写两个 skill 文件：
   - `~/.hermes/skills/tether-protocol.md` — Tether 协议、命令、注意事项
   - `~/.hermes/skills/collaboration-guide.md` — 协作规范、汇报方式
2. 新任务前对两个 agent 发 `/new` 清空 session
3. 再发 `{(新任务)}` 新任务

Skill 文件持久存在，`/new` 清 session 不影响 skill，新 session 加载 skill 后立即获得 Tether 协作能力。

### 效果

```
/new 前：LLM 看到 hermes_config + tether_web 混杂，回复偏向旧任务
/new 后：LLM 看到干净的上下文 + skill 规则，专注当前任务 ✅
```

---

## 问题五：Agent 幻觉

### 表现

Agent 多次声称"已完成"但实际未完成：
- Mac 说 "hermes_config 24 文件已推送" → 实际 Mac 上根本不存在 `~/hermes_config` 目录
- Agent 说 "已通过钉钉汇报" → 实际 `DINGTALK_WEBHOOK_URL` 未配置

### 根因

LLM 倾向于输出"已完成"来结束对话，而不实际执行验证。两个 agent 互相确认后，任何一方都不会再质疑。

### 缓解方案

**自证模式** — 在任务模板中要求 agent 贴验证命令的实际输出：

```
完成后，先输出验证方案，再执行验证：
1. 列出你打算检查什么、用什么命令、预期输出是什么
2. 执行这些命令
3. 把实际输出贴出来
```

原理：要求贴 `ls`、`git log`、`crontab -l` 的实际输出，Agent 无法只说"已完成"就跳过。

---

## 问题六：代码缺陷

### 1. `_check_handoff_timeout()` 中 hostname 未定义

**症状：** TP watcher 崩溃循环
```
NameError: name 'hostname' is not defined
  File "tether_watcher.py", line 969, in _check_handoff_timeout
```

**根因：** 函数中使用了 `hostname` 和 `sender_nick` 但从未定义。当有超时消息需要处理时触发崩溃。

**修复：** 在 for 循环前添加 `hostname = __import__("socket").gethostname()` 和 `sender_nick = os.environ.get("TETHER_SENDER_NICK", hostname)`

### 2. `_check_handoff_timeout()` SQL 时间格式不一致

详见 git log `fde82fa`：`sent_at` 字段有时是 ISO 格式（带 `T`），有时是 `strftime` 格式（带空格），导致 SQLite 时间比较失效，retry 永不触发。

### 3. `_recover_stale_handoffs()` 只恢复第一条 handoff

启动时扫描未处理的 handoff 只恢复第一条，后续靠子线程链式推进。如果子线程因 Gateway 异常未能处理，链式推进断掉，后续 handoff 被跳过。

---

## 改进汇总

| 改进 | 影响 |
|------|------|
| auto_reply 改为 type=info+is_reply | 消除协作卡死根因 |
| 中文昵称回退 TETHER_PEER_HOST | 消除 latin-1 崩溃 |
| tether_alive 外部保活 | 5 分钟检测 + 带任务上下文的唤醒 |
| `/new` + skill 方案 | 新旧任务隔离 |
| 自证验证模式 | 缓解 Agent 幻觉 |
| 多项 bug 修复 | watcher 崩溃恢复、时间格式对齐 |

## 未解决问题

| 问题 | 说明 | 建议 |
|------|------|------|
| Mac 间歇性断连 | SSH/WiFi 不稳定，导致协作中断 | 需网络层面解决 |
| DingTalk 未配置 | 汇报通道不通 | 配置飞书 webhook 或启用 DingTalk |
| handoff 链式推进脆弱 | Gateway 异常时链式推进中断 | 改进 _recover_stale_handoffs 为批量恢复 |
| Gateway 偶发超时 | Mac Gateway 频繁 401/超时 | 检查 API key 配置和网络 |
