#!/usr/bin/env python3
"""收件去重 / auto-reply 成本闸 / ttl 可观测 的回归测试。

跑法： python3 tests/test_dedupe_and_cap.py
不碰生产库：全程用 TETHER_DB_PATH 指到临时文件，服务起在 19003。
"""
import json, os, subprocess, sqlite3, sys, tempfile, time, urllib.request, urllib.error

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 19003
TMPDB = os.path.join(tempfile.mkdtemp(prefix="tether_dedupe_"), "t.db")
BASE = f"http://127.0.0.1:{PORT}"
fails = []


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  ← {detail}"))
    if not cond:
        fails.append(name)


def post(text, auto_reply=False, sender="154.8.143.218"):
    body = json.dumps({"from": sender, "sender": sender, "message": text,
                       "content": text, "type": "info"}).encode()
    headers = {"Content-Type": "application/json"}
    if auto_reply:
        headers["X-Tether-Auto-Reply"] = "1"
    req = urllib.request.Request(f"{BASE}/message", data=body, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def get_messages():
    with urllib.request.urlopen(f"{BASE}/messages?type=all", timeout=10) as r:
        return json.loads(r.read())


def aged(key_prefix_len=0, seconds=0):
    """把 dedup_seen 里最新一行的 ts 往前推，模拟时间流逝。"""
    c = sqlite3.connect(TMPDB)
    c.execute("UPDATE dedup_seen SET ts = ts - ?", (seconds,))
    c.commit()
    c.close()


def main():
    env = dict(os.environ, TETHER_DB_PATH=TMPDB, TETHER_PEER_KEY="mac")
    srv = subprocess.Popen([sys.executable, os.path.join(ROOT, "tether_server.py"),
                            "--port", str(PORT)], env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(f"{BASE}/health", timeout=1)
                break
            except Exception:
                time.sleep(0.2)

        # 1) 同内容二次入站 → 去重（120 秒窗）
        r1 = post("去重用例 A")
        r2 = post("去重用例 A")
        check("普通消息：首次入库", r1.get("status") == "ok" and "dropped" not in r1, r1)
        check("普通消息：120 秒内同内容被去重", r2.get("dropped") == "dup", r2)

        # 2) 不同内容都放行
        check("不同内容不被误杀", "dropped" not in post("去重用例 B"), "")

        # 3) 超过 120 秒（但不足 600）→ 普通消息放行
        aged(seconds=200)
        r3 = post("去重用例 A")
        check("普通消息：200 秒后可再入站", "dropped" not in r3, r3)

        # 4) auto-reply 头 → 200 秒仍算重复（600 秒窗）
        r4 = post("回声窗用例 X", auto_reply=True)
        aged(seconds=200)
        r5 = post("回声窗用例 X", auto_reply=True)
        check("auto-reply：首次入库", "dropped" not in r4, r4)
        check("auto-reply：200 秒内仍判重（600 秒窗）", r5.get("dropped") == "dup", r5)

        # 5) 600 秒窗命中必须留诊断行
        c = sqlite3.connect(TMPDB)
        n_log = c.execute("SELECT COUNT(*) FROM dedup_log WHERE window_s=600").fetchone()[0]
        c.close()
        check("600 秒窗命中写入 dedup_log", n_log >= 1, n_log)

        # 6) 迁移前 /messages 不返回 ttl；迁移后返回
        m = get_messages()
        check("/messages 迁移前不含 ttl", all("ttl" not in x for x in m["messages"]), m["messages"][:1])

        rc = subprocess.run([sys.executable, os.path.join(ROOT, "scripts/migrate_origin_ttl.py"),
                             "--db", TMPDB, "--apply"], capture_output=True, text=True)
        check("migrate --apply rc=0", rc.returncode == 0, rc.stdout[-400:])
        post("迁移后可见性用例", auto_reply=False)
        m2 = get_messages()
        check("/messages 迁移后返回 ttl 字段",
              any("ttl" in x for x in m2["messages"]), m2["messages"][:1])

        # 7) 成本闸：cap=3 时第 4 条起拦下，且计数跨进程保持（写库）
        sys.path.insert(0, ROOT)
        os.environ["TETHER_DB_PATH"] = TMPDB
        os.environ["TETHER_AUTO_REPLY_HOURLY_CAP"] = "3"
        import tether_watcher as w
        w._send_notification = lambda *a, **k: None      # 测试不发通知
        allowed = []
        for _ in range(4):
            ok, n, cap = w._auto_reply_gate("mac")
            allowed.append(ok)
            if ok:
                w._auto_reply_record("mac")   # 模拟成功出站记账
        check("成本闸：cap=3 后第 4 条被拦",
              allowed == [True, True, True, False], str(allowed))
        c = sqlite3.connect(TMPDB)
        n = c.execute("SELECT COUNT(*) FROM auto_reply_log WHERE peer='mac'").fetchone()[0]
        c.close()
        check("成本闸计数落库（跨重启保持）", n >= 3, n)
        os.environ["TETHER_AUTO_REPLY_HOURLY_CAP"] = "30"
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=5)
        except Exception:
            srv.kill()

    print()
    if fails:
        print(f"❌ FAILED {len(fails)}: {fails}")
        return 1
    print("✅ ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
