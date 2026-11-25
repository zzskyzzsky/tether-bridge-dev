"""tether_net 的行为契约测试（不触网、不依赖本机 tailscale 状态）。"""
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tether_net as tn  # noqa: E402


class ResolvePeerTest(unittest.TestCase):
    def setUp(self):
        tn._cache.clear()

    def test_ip_passthrough(self):
        self.assertEqual(tn.resolve_peer("100.81.192.38"), "100.81.192.38")
        self.assertTrue(tn.is_ip("fd7a:115c:a1e0::7739:c026"))

    def test_netmap_hit(self):
        self.assertEqual(tn.resolve_peer("zzsky-mbp", runner=lambda h: "100.1.2.3"), "100.1.2.3")

    def test_falls_back_to_last_known(self):
        self.assertEqual(tn.resolve_peer("zzsky-mbp", runner=lambda h: None),
                         tn.LAST_KNOWN["zzsky-mbp"])

    def test_unknown_host_kept(self):
        self.assertEqual(tn.resolve_peer("no-such-host", runner=lambda h: None), "no-such-host")

    def test_explicit_fallback_wins_over_raw(self):
        self.assertEqual(tn.resolve_peer("no-such-host", fallback="10.9.9.9"), "10.9.9.9")

    def test_cache_hits_once(self):
        calls = []

        def runner(h):
            calls.append(h)
            return "100.9.9.9"

        for _ in range(3):
            self.assertEqual(tn.resolve_peer("zzsky-mbp", runner=runner), "100.9.9.9")
        self.assertEqual(len(calls), 1)

    def test_empty_input(self):
        self.assertEqual(tn.resolve_peer(""), "")


class ResolveHostPortTest(unittest.TestCase):
    def setUp(self):
        tn._cache.clear()
        now = time.monotonic()
        tn._cache["zzsky-mbp"] = ("100.81.192.38", now)
        tn._cache["zzskytpg3"] = ("100.102.54.90", now)

    def test_plain_hostport(self):
        self.assertEqual(tn.resolve_hostport("zzsky-mbp:9002"), "100.81.192.38:9002")

    def test_url(self):
        self.assertEqual(tn.resolve_hostport("http://zzsky-mbp:9002"), "http://100.81.192.38:9002")

    def test_url_with_path(self):
        self.assertEqual(tn.resolve_hostport("http://zzskytpg3:9002/api/clear"),
                         "http://100.102.54.90:9002/api/clear")

    def test_ip_url_untouched(self):
        self.assertEqual(tn.resolve_hostport("http://100.81.192.38:9002/api/clear"),
                         "http://100.81.192.38:9002/api/clear")

    def test_localhost_untouched(self):
        self.assertEqual(tn.resolve_hostport("http://127.0.0.1:9003/status"),
                         "http://127.0.0.1:9003/status")


class CliFailureTest(unittest.TestCase):
    def test_missing_binary_returns_none(self):
        tn._cache.clear()
        orig = tn.subprocess.run

        def boom(*a, **k):
            raise FileNotFoundError("tailscale")

        tn.subprocess.run = boom
        try:
            self.assertIsNone(tn._run_tailscale("zzsky-mbp"))
        finally:
            tn.subprocess.run = orig

    def test_nonzero_exit_returns_none(self):
        class P:
            returncode = 1
            stdout = ""

        orig = tn.subprocess.run
        tn.subprocess.run = lambda *a, **k: P()
        try:
            self.assertIsNone(tn._run_tailscale("zzsky-mbp"))
        finally:
            tn.subprocess.run = orig

    def test_cache_expires(self):
        """TTL 到期后必须重新查 netmap（节点换 IP 才能自愈）。"""
        tn._cache.clear()
        calls = []
        tn.CACHE_TTL, orig_ttl = 0.01, tn.CACHE_TTL
        try:
            tn.resolve_peer("zzsky-mbp", runner=lambda h: calls.append(1) or "100.1.1.1")
            time.sleep(0.05)
            tn.resolve_peer("zzsky-mbp", runner=lambda h: calls.append(1) or "100.2.2.2")
        finally:
            tn.CACHE_TTL = orig_ttl
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
