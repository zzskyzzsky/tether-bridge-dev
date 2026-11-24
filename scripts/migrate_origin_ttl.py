#!/usr/bin/env python3
"""tether 消息表迁移：加 origin / ttl 两列（sender provenance + 防环计数器）。

背景
----
1) origin：relay 转发时会把 sender 改写成 RELAY_SENDER（tether_relay.py:180），
   消息来源在落库后不可追溯。relay 侧注入 data["origin_sender"]=原始 sender，
   server 落 origin 列；老 relay 不传该字段时落空串，展示层回退到 sender。
2) ttl：relay 设 data["ttl"]=1，但 server 的 INSERT 不存、/messages 也不返回，
   watcher 的 msg.get("ttl") 永远取不到值 ⇒ 每跳都被判成第一跳，防环闸门 100% 失效。
   ⚠️ 加列本身不足以止血：还须改 watcher 出站写死值（tether_watcher.py:1139/:1795 = 1、
   :1886 = 2）为 ttl-1，并把递减结果回写库。本脚本只负责 schema。

设计约束
--------
* 表结构变更不可回滚的旧说法不成立：sqlite 3.53.4 支持 DROP COLUMN，rollback 真删列。
* 窗口期间 watcher 不停，会抢写锁 ⇒ 嫁接下 sqlite3 timeout=5 + 重试。
* 迁移前用 sqlite3 backup API 备份（不裸拷 WAL）。
* 验收判据不是字节数：integrity_check=ok + 两表行数与源完全一致。
* 老库的 ALTER 会在 _db() 每请求 init 里跑（热路径），本迁移落库后应由 init 一次性建，
  不要再把 DDL 留在请求路径。

用法
----
    python3 migrate_origin_ttl.py --db ~/.hermes/tether/tether.db            # dry-run
    python3 migrate_origin_ttl.py --db ~/.hermes/tether/tether.db --apply
    python3 migrate_origin_ttl.py --db ~/.hermes/tether/tether.db --rollback --apply

退出码：0 成功 / 1 失败（失败时已自动还原备份并保留备份文件路径输出）。
"""
import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime

TABLE = "messages"
NEW_COLUMNS = [
    # (列名, 定义) —— origin 用空串默认值，兼容老 relay 不传字段
    ("origin", "TEXT NOT NULL DEFAULT ''"),
    ("ttl", "INTEGER NOT NULL DEFAULT 1"),
]
TIMEOUT = 5
RETRIES = 6
RETRY_SLEEP = 2.0


def log(msg):
    print(f"[migrate] {msg}", flush=True)


def connect(path):
    conn = sqlite3.connect(path, timeout=TIMEOUT, isolation_level=None)
    conn.execute(f"PRAGMA busy_timeout = {TIMEOUT * 1000}")
    return conn


def with_retry(fn, what):
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last = e
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            log(f"{what}: 锁冲突（{e}），{RETRY_SLEEP}s 后重试 {attempt}/{RETRIES}")
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"{what} 连续 {RETRIES} 次锁冲突失败: {last}")


def counts(path):
    conn = connect(path)
    try:
        out = {}
        for t in (TABLE, "outgoing_messages"):
            try:
                out[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except sqlite3.OperationalError:
                out[t] = None
        return out
    finally:
        conn.close()


def columns(path):
    conn = connect(path)
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({TABLE})")]
    finally:
        conn.close()


def integrity(path):
    conn = connect(path)
    try:
        return conn.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        conn.close()


def backup(src):
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = f"{src}.bak-{ts}"
    s = connect(src)
    d = sqlite3.connect(dst)
    try:
        s.backup(d)
    finally:
        d.close()
        s.close()
    log(f"备份完成：{dst}（{os.path.getsize(dst)} B，不用字节数与源库比较）")
    return dst


def migrate(path, apply_, drop):
    before_cols = columns(path)
    before_counts = counts(path)
    before_ic = integrity(path)
    log(f"当前列：{before_cols}")
    log(f"当前行数：{before_counts}　integrity_check={before_ic}")
    if before_ic != "ok":
        log("源库 integrity_check 非 ok，终止")
        return 1

    targets = []
    for name, ddl in NEW_COLUMNS:
        if drop:
            if name in before_cols:
                targets.append((name, f"ALTER TABLE {TABLE} DROP COLUMN {name}"))
        else:
            if name not in before_cols:
                targets.append((name, f"ALTER TABLE {TABLE} ADD COLUMN {name} {ddl}"))

    if not targets:
        log("无待执行变更（列已存在/已删除），退出")
        return 0

    for name, sql in targets:
        log(("将执行" if apply_ else "dry-run") + f"：{sql}")

    if not apply_:
        log("dry-run 结束，未改动任何数据。加 --apply 才真正执行")
        return 0

    bak = backup(path)
    conn = connect(path)
    try:
        for name, sql in targets:
            with_retry(lambda s=sql: conn.execute(s), sql)
            log(f"已执行：{sql}")
    except Exception as e:
        log(f"❌ 迁移失败（{e}），开始还原备份 {bak}")
        conn.close()
        restore(bak, path)
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

    after_cols = columns(path)
    after_counts = counts(path)
    after_ic = integrity(path)
    log(f"迁移后列：{after_cols}")
    log(f"迁移后行数：{after_counts}　integrity_check={after_ic}")

    ok = after_ic == "ok" and after_counts == before_counts
    if not ok:
        log("❌ 验收不通过（integrity 或行数不一致），开始还原备份")
        restore(bak, path)
        return 1
    log(f"✅ 验收通过（integrity=ok，行数与迁移前一致）备份留存：{bak}")
    return 0


def restore(bak, path):
    s = sqlite3.connect(bak)
    d = connect(path)
    try:
        s.backup(d)
        log(f"已从备份还原：{bak} → {path}")
    finally:
        d.close()
        s.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.expanduser("~/.hermes/tether/tether.db"))
    ap.add_argument("--apply", action="store_true", help="真正执行（默认 dry-run）")
    ap.add_argument("--rollback", action="store_true", help="DROP 掉本迁移新增的列")
    a = ap.parse_args()
    if not os.path.exists(a.db):
        log(f"库不存在：{a.db}")
        return 1
    return migrate(a.db, a.apply, a.rollback)


if __name__ == "__main__":
    sys.exit(main())
