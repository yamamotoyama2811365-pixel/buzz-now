import ast
import json
import sqlite3
import unittest
from pathlib import Path
from urllib.parse import quote, parse_qs, urlsplit
from app import social_tracking as tracking


class TrackingTests(unittest.TestCase):
    def setUp(self):
        self.c = sqlite3.connect(":memory:")
        self.c.row_factory = sqlite3.Row
        self.c.execute("CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT)")
        tree = ast.parse((Path(__file__).parents[1] / "app/main.py").read_text())
        self.tree = tree
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_build_social_post_text")
        ns = {"quote": quote, "SOCIAL_PUBLIC_BASE_URL": "https://buzz-now-1.onrender.com",
              "_social_detail_path": lambda s: "/trend/" + quote(s),
              "_social_reason_from_row": lambda r, k: "関連報道：確認済みの見出し"}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "main.py", "exec"), ns)
        self.build = ns[fn.name]

    def tearDown(self):
        self.c.close()

    def test_individual_attempts_have_unique_tokens(self):
        tokens = {tracking.new_content_id() for _ in range(100)}
        self.assertEqual(len(tokens), 100)
        self.assertTrue(all(t.startswith("news_v1_") for t in tokens))

    def test_link_uses_correct_host_and_exact_tracking_token(self):
        token = tracking.new_content_id()
        text = self.build({"keyword": "話題", "slug": "話題"}, token)
        url = urlsplit(text.splitlines()[-1])
        self.assertEqual(url.netloc, "buzz-now-1.onrender.com")
        self.assertEqual(parse_qs(url.query), {"utm_source": ["x"], "utm_medium": ["social"],
                         "utm_campaign": ["prebuzz"], "utm_content": [token]})

    def test_preview_remains_backward_compatible(self):
        self.assertIn("utm_content=news_context_v1", self.build({"keyword": "x", "slug": "x"}))

    def test_tracking_value_is_url_encoded(self):
        text = self.build({"keyword": "x", "slug": "x"}, "a&other=bad")
        self.assertEqual(parse_qs(urlsplit(text.splitlines()[-1]).query)["utm_content"], ["a&other=bad"])

    def test_accepted_mapping_persists_without_credentials_or_fake_metrics(self):
        tracking.record(self.c, 7, "time", "token", {"ok": True, "post_id": "buffer-id", "secret": "do-not-store"}, "caption\nhttps://example.com/?utm_content=token")
        value = tracking.lookup(self.c, 7, "time")
        self.assertEqual(value["buffer_post_id"], "buffer-id")
        self.assertTrue(value["buffer_accepted"])
        self.assertEqual(value["metrics_status"], "not_collected")
        self.assertNotIn("do-not-store", json.dumps(value))
        self.assertNotIn("sessions", value)

    def test_failed_and_historical_attempts_not_claimed_as_success(self):
        tracking.record(self.c, 7, "time", "token", {"ok": False}, "caption\nhttps://example.com/")
        value = tracking.lookup(self.c, 7, "time")
        self.assertFalse(value["buffer_accepted"])
        self.assertIsNone(value["buffer_post_id"])
        self.assertIsNone(tracking.lookup(self.c, 7, "older"))

    def test_retry_write_is_idempotent(self):
        for _ in range(2):
            tracking.record(self.c, 7, "time", "token", {"ok": True, "post_id": "id"}, "url")
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM system_state").fetchone()[0], 1)

    def test_dispatcher_wires_token_and_storage_without_live_calls(self):
        fn = next(n for n in self.tree.body if isinstance(n, ast.FunctionDef) and n.name == "_auto_post_social_unscheduled")
        source = ast.unparse(fn)
        self.assertIn("_build_social_post_text(row, tracking_content)", source)
        self.assertIn("social_tracking.record(c, row['id'], ts, tracking_content, buffer_result, post_text)", source)
        self.assertIn("_social_post_allowed(c, row, now_dt)", source)


if __name__ == "__main__":
    unittest.main()
