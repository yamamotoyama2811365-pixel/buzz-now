import os
import unittest
from datetime import datetime, timezone

os.environ["DEMO_MODE"] = "false"
os.environ["REAL_DATA_MODE"] = "false"

from app import main


class BreakingIncidentNewsTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)

    def test_extracts_public_alias_from_arrest_headline(self):
        title = "【速報】「ヤマトリノ」として活動する田沢梨乃容疑者を麻薬取締法違反の疑いで現行犯逮捕"
        self.assertEqual(main._breaking_incident_subject(title), "ヤマトリノ")

    def test_accepts_fresh_trusted_arrest_report(self):
        article = {
            "title": "インフルエンサー「ヤマトリノ」を現行犯逮捕 コカイン所持の疑い",
            "publisher": "TBS NEWS DIG",
            "published_at": "Thu, 17 Sep 2026 10:30:00 GMT",
        }
        self.assertTrue(main._breaking_incident_article_ok(article, self.now))

    def test_rejects_rumour_only_drug_allegation(self):
        article = {
            "title": "人気インフルエンサーに薬物疑惑 SNSで憶測",
            "publisher": "TBS NEWS DIG",
            "published_at": "Thu, 17 Sep 2026 10:30:00 GMT",
        }
        self.assertFalse(main._breaking_incident_article_ok(article, self.now))

    def test_rejects_untrusted_sensitive_source(self):
        article = {
            "title": "人気タレントを逮捕 薬物所持の疑い",
            "publisher": "匿名まとめブログ",
            "published_at": "Thu, 17 Sep 2026 10:30:00 GMT",
        }
        self.assertFalse(main._breaking_incident_article_ok(article, self.now))

    def test_accepts_verified_entertainment_suspension(self):
        article = {
            "title": "俳優の山田太郎が活動休止を発表 所属事務所がコメント",
            "publisher": "ORICON NEWS",
            "published_at": "Thu, 17 Sep 2026 10:30:00 GMT",
        }
        self.assertTrue(main._breaking_incident_article_ok(article, self.now))
        self.assertEqual(main._breaking_incident_subject(article["title"]), "山田太郎")

    def test_rejects_unattributed_affair_rumour(self):
        article = {
            "title": "人気俳優に不倫疑惑 SNSで噂広がる",
            "publisher": "ORICON NEWS",
            "published_at": "Thu, 17 Sep 2026 10:30:00 GMT",
        }
        self.assertFalse(main._breaking_incident_article_ok(article, self.now))

    def test_routine_economic_topic_is_demoted(self):
        entertainment = main._search_intent_priority("山田太郎", "俳優が活動休止を発表", "芸能")
        economy = main._search_intent_priority("ドル円", "為替と金利の最新ニュース", "経済")
        self.assertGreater(entertainment, economy)

    def test_queries_are_freshness_bounded(self):
        self.assertTrue(main.BREAKING_INCIDENT_QUERIES)
        self.assertTrue(all("when:1d" in q for q in main.BREAKING_INCIDENT_QUERIES))


if __name__ == "__main__":
    unittest.main()
