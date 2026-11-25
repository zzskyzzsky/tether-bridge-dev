"""tether_web /api/clear 防回声回归（2026-11-25 无限互清事故）。

契约（入口三态）：
  1) 带 X-Tether-Clear-Forward 头（对端发起）→ 只清本地、不再回传；
  2) 带 ?relay=1（同上，手工 curl 用）→ 只清本地、不再回传；
  3) 无标记（本端发起）→ 清本地 + 向对端转发恰好 1 次且带标记头。
另断言：转发失败不被静默吞掉（要打出日志），且本端仍返回 200/已清空。
不触网、不动生产库：临时 DB + 本地假对端。
"""
import io
import os
import sqlite3
import sys
import threading
import tempfile
import contextlib
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tether_web as tw  # noqa: E402


class _FakePeer(BaseHTTPRequestHandler):
    hits = []

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n:
            self.rfile.read(n)
        _FakePeer.hits.append({
            "path": self.path,
            "hdr": self.headers.get("X-Tether-Clear-Forward"),
        })
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, *a):
        pass


def main():
    ok = True
    tmpdb = os.path.join(tempfile.mkdtemp(prefix="tether_clear_"), "t.db")
    conn = sqlite3.connect(tmpdb)
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, msg TEXT)")
    conn.execute("CREATE TABLE outgoing_messages (id INTEGER PRIMARY KEY, msg TEXT)")
    conn.commit()
    conn.close()

    orig_db, orig_peer = tw.DB_PATH, tw._peer_web_url
    tw.DB_PATH = tmpdb

    srv = HTTPServer(("127.0.0.1", 0), _FakePeer)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    tw._peer_web_url = lambda: f"http://127.0.0.1:{port}"
    client = tw.app.test_client()

    def reset():
        c = sqlite3.connect(tmpdb)
        c.execute("INSERT INTO messages (msg) VALUES ('x')")
        c.execute("INSERT INTO outgoing_messages (msg) VALUES ('y')")
        c.commit()
        c.close()

    def counts():
        c = sqlite3.connect(tmpdb)
        m = c.execute("select count(*) from messages").fetchone()[0]
        o = c.execute("select count(*) from outgoing_messages").fetchone()[0]
        c.close()
        return m, o

    def case(name, expect_hits, expect_codes, call, check_hdr=False):
        nonlocal ok
        reset()
        _FakePeer.hits.clear()
        r = call()
        cond = (r.status_code == 200 and len(_FakePeer.hits) == expect_hits
                and counts() == (0, 0) and r.get_json().get("status") == "ok")
        if check_hdr:
            cond = cond and _FakePeer.hits[0]["hdr"] == "1"
        print(("PASS" if cond else "FAIL"), name, "| 转发", len(_FakePeer.hits),
              "次 | 剩余", counts(), "| 返回", r.get_json().get("message"))
        ok = ok and cond

    try:
        case("带头（对端发起）只清本地",
             0, None, lambda: client.post("/api/clear", headers={"X-Tether-Clear-Forward": "1"}))
        case("?relay=1（对端发起）只清本地",
             0, None, lambda: client.post("/api/clear?relay=1"))
        case("无标记（本端发起）转发 1 次且带标记头",
             1, None, lambda: client.post("/api/clear"), check_hdr=True)

        # 4) 转发失败：必须打日志、本端仍返回成功
        reset()
        tw._peer_web_url = lambda: "http://127.0.0.1:1"  # 必然拒绝连接
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            r = client.post("/api/clear")
        out = buf.getvalue()
        cond = (r.status_code == 200 and "[clear] 转发对端失败" in out and counts() == (0, 0))
        print(("PASS" if cond else "FAIL"), "转发失败打日志不静默", "| 输出:", out.strip()[:60])
        ok = ok and cond
    finally:
        tw.DB_PATH, tw._peer_web_url = orig_db, orig_peer
        srv.shutdown()
        srv.server_close()

    print("✅ ALL PASS" if ok else "❌ FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
