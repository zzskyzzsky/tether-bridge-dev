"""tether_web /api/clear 防回声回归（2026-11-25 无限互清事故）。

契约：带 X-Tether-Clear-Forward 头（对端发起）→ 只清本地、不再回传；
      无头（本端发起）→ 清本地 + 向对端转发恰好 1 次且带该头。
不触网、不动生产库：临时 DB + 本地假对端。
"""
import os
import sqlite3
import sys
import threading
import tempfile
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

    try:
        # 1) 对端发起（带头）→ 本地清空、0 次回传
        reset()
        _FakePeer.hits.clear()
        r = client.post("/api/clear", headers={"X-Tether-Clear-Forward": "1"})
        cond = r.status_code == 200 and not _FakePeer.hits and counts() == (0, 0)
        print(("PASS" if cond else "FAIL"), "带头请求只清本地：转发", len(_FakePeer.hits), "次，剩余", counts())
        ok &= cond

        # 2) 本端发起（无头）→ 本地清空 + 转发 1 次且带标记头
        reset()
        _FakePeer.hits.clear()
        r = client.post("/api/clear")
        cond = (r.status_code == 200 and len(_FakePeer.hits) == 1
                and _FakePeer.hits[0]["hdr"] == "1" and counts() == (0, 0))
        print(("PASS" if cond else "FAIL"), "无头请求转发恰好 1 次且带防回声头：", _FakePeer.hits)
        ok &= cond
    finally:
        tw.DB_PATH, tw._peer_web_url = orig_db, orig_peer
        srv.shutdown()
        srv.server_close()

    print("✅ ALL PASS" if ok else "❌ FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
