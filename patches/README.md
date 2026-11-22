# 补丁目录

补丁不再放 `/tmp`（会被清理、且 mac 侧搜不到）。这里只放**在仓库内可 git pull 拿到**的补丁。

基准：`c49f104`（main，含「中断文案前 40 字含中文即放行」）。

## handoff-retry-v4.patch（当前唯一在用的版本）

- 290 行，md5 `9fa9059a6aaf3c7f38176e6a3c28fa61`
- 改动文件：`tether_watcher.py`（5 个 hunk）、`tether_server.py`（2 个 hunk）
- **累积补丁**：在 `c49f104` 上一次性应用，含 v3 全部内容 + mac review 的 3 条 P2 修复，不要再单独打 v3
- 打完成品行数（实测）：`tether_watcher.py` 2211 行

包含内容：

1. handoff 失败不再无条件 ack：按 `_ACK_RETRY_BACKOFF`(60s) 退避重试，最多 `_ACK_MAX_ATTEMPTS`(3) 次，耗尽才 ack 并告警
2. 在途登记 `_HANDOFF_INFLIGHT`（900s TTL），退避期间不会被 `_recover_next_handoff` 重复捞出
3. 重试会话名 `-h{attempt}-{msg_id[:6]}`，与 info 路径的 `-r{n}` 完全分离，两条 handoff 并跑不串上下文
4. handoff 文件写点全部 tmp + `os.replace` 原子化（watcher 侧两处；P2-a 补上 `tether_server.py:121` 的跨进程写点）
5. 兜底恢复 30s 节流（P2b）
6. 损坏 handoff 文件自愈：解析失败且 mtime > 5s 则删除并 `_recover_next_handoff()`（P2-b）
7. `_drop_gateway_session` 移进 `if output:`，与 info 路径一致（P2-c）

## 已被取代

- `handoff-failure-ack.patch`（56 行，md5 `8884b95…`）：v3/v4 的严格子集，**不要再打**
- `handoff-retry-v2.patch`（184 行）：被 v3 取代
- `handoff-retry-v3.patch`（247 行）：被 v4 取代（v4 已累积包含）

## 应用与验证

```
git pull                     # 确认 HEAD = c49f104 或更新
git apply --check patches/handoff-retry-v4.patch
git apply patches/handoff-retry-v4.patch
python3 -m py_compile tether_watcher.py tether_server.py
```

注意：`tether_server.py` 改动需要重启 server 才生效；只重启 watcher 只覆盖 1–6 中的 watcher 侧部分。
