
import os
import base64
import json
import logging
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from pathlib import Path
from urllib.parse import quote, urlparse
from xml.sax.saxutils import escape as xml_escape
import xml.etree.ElementTree as ET
from io import BytesIO

from fastapi import FastAPI, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx
import feedparser
from PIL import Image, ImageDraw, ImageFont

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "buzznow.db"
SITE_URL = os.getenv("SITE_URL", "http://localhost:8000").rstrip("/")

# Yahoo!リアルタイム検索を使う無料の Buzzing Now 候補収集。
# 公開HTMLで取得できる順位だけを使い、X APIは使わない。
YAHOO_BUZZ_ENABLED = os.getenv("YAHOO_BUZZ_ENABLED", "true").lower() == "true"
YAHOO_BUZZ_RANK_MIN = max(1, int(os.getenv("YAHOO_BUZZ_RANK_MIN", "30")))
YAHOO_BUZZ_RANK_MAX = max(YAHOO_BUZZ_RANK_MIN, int(os.getenv("YAHOO_BUZZ_RANK_MAX", "80")))
YAHOO_BUZZ_FALLBACK_COUNT = max(1, min(int(os.getenv("YAHOO_BUZZ_FALLBACK_COUNT", "10")), 30))
YAHOO_BUZZ_PROMOTE_LIMIT = max(1, min(int(os.getenv("YAHOO_BUZZ_PROMOTE_LIMIT", "10")), 10))
YAHOO_QUOTE_SCAN_LIMIT = max(1, min(int(os.getenv("YAHOO_QUOTE_SCAN_LIMIT", "8")), 20))
YAHOO_QUOTE_MIN_LIKES = max(0, int(os.getenv("YAHOO_QUOTE_MIN_LIKES", "300")))
YAHOO_QUOTE_MIN_REPOSTS = max(0, int(os.getenv("YAHOO_QUOTE_MIN_REPOSTS", "50")))
# V35.24: production automatic Buzzing Now quote-posting via Buffer -> X.
# Enabled by default because this build is the production implementation requested.
# Safety rails keep volume low and prevent duplicate / rapid-fire posts.
YAHOO_QUOTE_AUTO_ENABLED = os.getenv("YAHOO_QUOTE_AUTO_ENABLED", "true").lower() == "true"
YAHOO_QUOTE_AUTO_MIN_LIKES = max(0, int(os.getenv("YAHOO_QUOTE_AUTO_MIN_LIKES", "1000")))
YAHOO_QUOTE_AUTO_MIN_REPOSTS = max(0, int(os.getenv("YAHOO_QUOTE_AUTO_MIN_REPOSTS", "150")))
YAHOO_QUOTE_DAILY_CAP = max(1, int(os.getenv("YAHOO_QUOTE_DAILY_CAP", "6")))
YAHOO_QUOTE_GLOBAL_COOLDOWN_MINUTES = max(0, int(os.getenv("YAHOO_QUOTE_GLOBAL_COOLDOWN_MINUTES", "60")))
YAHOO_QUOTE_KEYWORD_COOLDOWN_HOURS = max(0, int(os.getenv("YAHOO_QUOTE_KEYWORD_COOLDOWN_HOURS", "24")))
YAHOO_QUOTE_MAX_PER_RUN = max(1, min(int(os.getenv("YAHOO_QUOTE_MAX_PER_RUN", "1")), 3))


SITE_NAME = os.getenv("SITE_NAME", "BUZZ NOW")

# Production runtime settings
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
APP_VERSION = os.getenv("APP_VERSION", "35.27.0")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

REAL_DATA_MODE = os.getenv("REAL_DATA_MODE","true").lower() == "true"
REAL_DATA_INTERVAL_MINUTES = int(os.getenv("REAL_DATA_INTERVAL_MINUTES","30"))

# V26: Make.com webhook bridge for BUZZ NOW social automation
MAKE_WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL", "").strip()

# V32 direct Buffer API
BUFFER_API_KEY = os.getenv("BUFFER_API_KEY", "").strip()
BUFFER_CHANNEL_ID = os.getenv("BUFFER_CHANNEL_ID", "6a9a680a065799be4686e3d9").strip()
BUFFER_API_URL = "https://api.buffer.com"
SOCIAL_TEST_ENABLED = os.getenv("SOCIAL_TEST_ENABLED", "false").lower() == "true"

# V30: production X auto-posting via Make -> Buffer -> X
SOCIAL_AUTO_ENABLED = os.getenv("SOCIAL_AUTO_ENABLED", "false").lower() == "true"
SOCIAL_MIN_PREBUZZ = float(os.getenv("SOCIAL_MIN_PREBUZZ", "85"))
SOCIAL_MIN_TRAFFIC = float(os.getenv("SOCIAL_MIN_TRAFFIC", "70"))
SOCIAL_MIN_CONFIDENCE = float(os.getenv("SOCIAL_MIN_CONFIDENCE", "50"))
SOCIAL_KEYWORD_COOLDOWN_HOURS = int(os.getenv("SOCIAL_KEYWORD_COOLDOWN_HOURS", "72"))
SOCIAL_GLOBAL_COOLDOWN_MINUTES = int(os.getenv("SOCIAL_GLOBAL_COOLDOWN_MINUTES", "60"))
SOCIAL_DAILY_CAP = int(os.getenv("SOCIAL_DAILY_CAP", "8"))
SOCIAL_MAX_POSTS_PER_RUN = int(os.getenv("SOCIAL_MAX_POSTS_PER_RUN", "1"))

# V30.5: AI visual for social posts.
# Keep the key only in Render Environment; never commit it to GitHub.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_IMAGE_MODEL = os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-2").strip()
SOCIAL_AI_IMAGE_ENABLED = os.getenv("SOCIAL_AI_IMAGE_ENABLED", "false").lower() == "true"
SOCIAL_AI_IMAGE_QUALITY = os.getenv("SOCIAL_AI_IMAGE_QUALITY", "low").strip()

# V19: article discovery / WHY NOW enrichment
NEWS_ENRICHMENT_ENABLED = os.getenv("NEWS_ENRICHMENT_ENABLED", "true").lower() == "true"
GDELT_NEWS_ENABLED = os.getenv("GDELT_NEWS_ENABLED", "true").lower() == "true"
GDELT_NEWS_LIMIT = max(0, min(15, int(os.getenv("GDELT_NEWS_LIMIT", "8"))))
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"
DEMO_INTERVAL_SECONDS = int(os.getenv("DEMO_INTERVAL_SECONDS", "30"))

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger=logging.getLogger("buzz-now")

app = FastAPI(title=SITE_NAME)
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")
templates = Jinja2Templates(directory=BASE / "templates")


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def _pg_sql(sql: str) -> str:
    """Translate the small SQLite SQL subset used by BUZZ NOW to PostgreSQL."""
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
    sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, flags=re.I)
    if re.search(r"^\s*INSERT\s+INTO", sql, flags=re.I) and "OR IGNORE" not in sql.upper():
        # Only statements that were originally INSERT OR IGNORE are marked below.
        pass
    return sql.replace("?", "%s")


class PostgresConnection:
    def __init__(self, url: str):
        import psycopg
        from psycopg.rows import dict_row
        self._con = psycopg.connect(url, row_factory=dict_row)

    def execute(self, sql, params=()):
        original = sql
        sql = _pg_sql(sql)
        if re.search(r"INSERT\s+OR\s+IGNORE\s+INTO", original, flags=re.I):
            sql = re.sub(r"INSERT\s+INTO", "INSERT INTO", sql, count=1, flags=re.I)
            sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return self._con.execute(sql, params)

    def executescript(self, script):
        # init_db contains simple CREATE TABLE / CREATE INDEX statements only.
        for statement in script.split(";"):
            statement = statement.strip()
            if statement:
                self.execute(statement)

    def commit(self):
        self._con.commit()

    def rollback(self):
        self._con.rollback()

    def close(self):
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._con.commit()
        else:
            self._con.rollback()
        self._con.close()
        return False


def db():
    # Render production: durable PostgreSQL. Local development: SQLite fallback.
    if DATABASE_URL:
        return PostgresConnection(DATABASE_URL)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).strip().lower()
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^\w\-ぁ-んァ-ヶ一-龠々ー]", "", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or quote(text, safe="")


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS trends(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT UNIQUE NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            why_now TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '総合',
            pre_buzz_score REAL NOT NULL DEFAULT 0,
            buzz_score REAL NOT NULL DEFAULT 0,
            acceleration REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT '🌱 前兆',
            first_detected_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_indexable INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS related_keywords(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            UNIQUE(trend_id, keyword)
        );

        CREATE TABLE IF NOT EXISTS sources(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            publisher TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            published_at TEXT DEFAULT '',
            source_label TEXT DEFAULT '単独情報',
            UNIQUE(trend_id, url)
        );
        
        CREATE TABLE IF NOT EXISTS trend_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            pre_buzz_score REAL NOT NULL,
            buzz_score REAL NOT NULL,
            acceleration REAL NOT NULL,
            recorded_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS system_state(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS social_posts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            pre_buzz_score REAL NOT NULL DEFAULT 0,
            traffic_potential REAL NOT NULL DEFAULT 0,
            post_text TEXT NOT NULL,
            make_status INTEGER NOT NULL DEFAULT 0,
            posted_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_social_posts_trend_time
            ON social_posts(trend_id, posted_at);
        CREATE INDEX IF NOT EXISTS idx_social_posts_posted_at
            ON social_posts(posted_at);

        CREATE TABLE IF NOT EXISTS social_images(
            trend_id INTEGER PRIMARY KEY,
            image_b64 TEXT NOT NULL,
            mime_type TEXT NOT NULL DEFAULT 'image/png',
            model TEXT NOT NULL DEFAULT '',
            prompt TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS social_image_derivatives(
            trend_id INTEGER PRIMARY KEY,
            jpeg_b64 TEXT NOT NULL,
            byte_length INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS traffic_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            impressions INTEGER NOT NULL DEFAULT 0,
            clicks INTEGER NOT NULL DEFAULT 0,
            pageviews INTEGER NOT NULL DEFAULT 0,
            ctr REAL NOT NULL DEFAULT 0,
            traffic_potential REAL NOT NULL DEFAULT 0,
            recorded_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS traffic_totals(
            trend_id INTEGER PRIMARY KEY,
            impressions INTEGER NOT NULL DEFAULT 0,
            clicks INTEGER NOT NULL DEFAULT 0,
            pageviews INTEGER NOT NULL DEFAULT 0,
            last_ctr REAL NOT NULL DEFAULT 0,
            traffic_potential REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS growth_state(
            trend_id INTEGER PRIMARY KEY,
            level INTEGER NOT NULL DEFAULT 0,
            quality_score REAL NOT NULL DEFAULT 0,
            decision TEXT NOT NULL DEFAULT '観察中',
            last_reason TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS growth_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            old_level INTEGER NOT NULL,
            new_level INTEGER NOT NULL,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS predictions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            predicted_pre_buzz REAL NOT NULL,
            predicted_buzz REAL NOT NULL,
            predicted_acceleration REAL NOT NULL,
            predicted_traffic_potential REAL NOT NULL,
            predicted_pageviews INTEGER NOT NULL DEFAULT 0,
            horizon_ticks INTEGER NOT NULL DEFAULT 6,
            ticks_elapsed INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            evaluated_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS prediction_results(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER UNIQUE NOT NULL,
            trend_id INTEGER NOT NULL,
            actual_buzz REAL NOT NULL,
            actual_traffic_potential REAL NOT NULL,
            actual_pageviews INTEGER NOT NULL,
            buzz_gain REAL NOT NULL,
            traffic_gain REAL NOT NULL,
            pv_gain INTEGER NOT NULL,
            hit INTEGER NOT NULL,
            score REAL NOT NULL,
            evaluated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS model_state(
            key TEXT PRIMARY KEY,
            value REAL NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS source_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_id TEXT NOT NULL,
            keyword TEXT NOT NULL,
            source_score REAL NOT NULL DEFAULT 0,
            raw_metric REAL NOT NULL DEFAULT 0,
            source_url TEXT DEFAULT '',
            collected_at TEXT NOT NULL,
            UNIQUE(source, external_id)
        );

        CREATE TABLE IF NOT EXISTS collector_state(
            source TEXT PRIMARY KEY,
            last_status TEXT NOT NULL DEFAULT 'never',
            last_message TEXT NOT NULL DEFAULT '',
            last_count INTEGER NOT NULL DEFAULT 0,
            last_run_at TEXT DEFAULT ''
        );
        
        CREATE TABLE IF NOT EXISTS confidence_state(
            trend_id INTEGER PRIMARY KEY,
            source_count INTEGER NOT NULL DEFAULT 0,
            confidence_score REAL NOT NULL DEFAULT 0,
            confidence_label TEXT NOT NULL DEFAULT '単独シグナル',
            corroborated INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS source_snapshots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            keyword TEXT NOT NULL,
            match_key TEXT NOT NULL,
            source_score REAL NOT NULL DEFAULT 0,
            raw_metric REAL NOT NULL DEFAULT 0,
            captured_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_source_snapshots_key_time
        ON source_snapshots(match_key,captured_at);

        CREATE TABLE IF NOT EXISTS propagation_state(
            trend_id INTEGER PRIMARY KEY,
            first_source TEXT DEFAULT '',
            first_seen_at TEXT DEFAULT '',
            second_source TEXT DEFAULT '',
            second_seen_at TEXT DEFAULT '',
            propagation_minutes REAL DEFAULT NULL,
            source_sequence TEXT DEFAULT '',
            velocity_30m REAL NOT NULL DEFAULT 0,
            velocity_1h REAL NOT NULL DEFAULT 0,
            velocity_3h REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS monetization_state(
            trend_id INTEGER PRIMARY KEY,
            monetize_score REAL NOT NULL DEFAULT 0,
            monetize_grade TEXT NOT NULL DEFAULT 'C',
            intent_category TEXT NOT NULL DEFAULT 'general',
            recommended_mode TEXT NOT NULL DEFAULT 'adsense',
            reason TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS monetization_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trend_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            provider TEXT NOT NULL DEFAULT '',
            slot TEXT NOT NULL DEFAULT '',
            value REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );


        CREATE TABLE IF NOT EXISTS v9_signal_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            keyword TEXT NOT NULL,
            match_key TEXT NOT NULL,
            source_score REAL NOT NULL DEFAULT 0,
            raw_metric REAL NOT NULL DEFAULT 0,
            captured_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_v9_signal_history_lookup
        ON v9_signal_history(source, match_key, captured_at);

        CREATE TABLE IF NOT EXISTS v9_velocity_state(
            trend_id INTEGER PRIMARY KEY,
            velocity_30m REAL NOT NULL DEFAULT 0,
            velocity_1h REAL NOT NULL DEFAULT 0,
            velocity_3h REAL NOT NULL DEFAULT 0,
            velocity_score REAL NOT NULL DEFAULT 0,
            velocity_label TEXT NOT NULL DEFAULT '観測開始',
            first_source TEXT NOT NULL DEFAULT '',
            first_seen_at TEXT NOT NULL DEFAULT '',
            source_sequence TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );
        """)

        count = c.execute("SELECT COUNT(*) AS n FROM trends").fetchone()["n"]
        if count == 0:
            seeds = [
                ("AIグラス", "AI機能を搭載したスマートグラスへの関心が高まっています。",
                 "複数のテクノロジー領域で関連語の増加が見られる想定サンプルです。",
                 "テクノロジー", 84, 71, 0.34, "⚡ 加速中"),
                ("透明感メイク", "SNSで広がりやすい美容系キーワードのサンプルです。",
                 "美容・コスメ文脈で関連ワードが増え始めた想定です。",
                 "美容", 78, 66, 0.29, "🌱 前兆"),
                ("札幌新店", "札幌の新規オープン店舗を探す検索需要を想定したサンプルです。",
                 "地域名と新店情報は検索意図が明確になりやすいテーマです。",
                 "北海道", 73, 64, 0.22, "🚀 急上昇"),
            ]
            for s in seeds:
                keyword, summary, why_now, category, pre, buzz, acc, status = s
                slug = slugify(keyword)
                ts = now_iso()
                c.execute("""
                    INSERT INTO trends(
                        keyword,slug,summary,why_now,category,
                        pre_buzz_score,buzz_score,acceleration,status,
                        first_detected_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (keyword, slug, summary, why_now, category, pre, buzz, acc, status, ts, ts))
                trend_id = c.execute("SELECT id FROM trends WHERE slug=?", (slug,)).fetchone()["id"]
                related = {
                    "AIグラス":["AIグラスとは","スマートグラス","AIウェアラブル"],
                    "透明感メイク":["透明感メイク 方法","透明感コスメ","ツヤ肌メイク"],
                    "札幌新店":["札幌 新店","札幌 グルメ 新店","札幌 オープン"]
                }[keyword]
                for r in related:
                    c.execute("INSERT OR IGNORE INTO related_keywords(trend_id,keyword) VALUES(?,?)",(trend_id,r))


DEMO_KEYWORDS = [
    ("AIピン", "テクノロジー"),
    ("透明感リップ", "美容"),
    ("札幌カフェ新店", "北海道"),
    ("平成レトロ", "エンタメ"),
    ("朝活ルーティン", "ライフスタイル"),
    ("韓国ヘア", "美容"),
    ("生成AI副業", "ビジネス"),
    ("推し活バッグ", "ファッション"),
    ("睡眠ルーティン", "ライフスタイル"),
    ("札幌ラーメン新店", "北海道"),
    ("ショートドラマ", "エンタメ"),
    ("AI議事録", "ビジネス"),
]

def classify(pre, buzz, acc):
    if acc >= 0.34 and pre >= 75:
        return "⚡ 加速中"
    if buzz >= 78:
        return "🔥 爆発中"
    if pre >= 72:
        return "🚀 急上昇"
    return "🌱 前兆"

def ensure_demo_keywords():
    ts = now_iso()
    with db() as c:
        existing = {r["keyword"] for r in c.execute("SELECT keyword FROM trends").fetchall()}
        for keyword, category in DEMO_KEYWORDS:
            if keyword in existing:
                continue
            pre = random.randint(48, 82)
            buzz = random.randint(35, 75)
            acc = round(random.uniform(0.05, 0.34), 2)
            c.execute("""
                INSERT INTO trends(
                    keyword,slug,summary,why_now,category,
                    pre_buzz_score,buzz_score,acceleration,status,
                    first_detected_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (
                keyword, slugify(keyword),
                f"{keyword}に関する検索・SNS上の関心が高まり始めている想定データです。",
                "複数のシグナルが同時に伸び始めている状態を再現しています。",
                category, pre, buzz, acc, classify(pre,buzz,acc), ts, ts
            ))


def calc_traffic_potential(pre_buzz, buzz, acceleration, pageviews, ctr):
    """
    0-100のアクセス期待値。
    V3では仮想ロジック。本番ではSearch Console等の実績データに置き換える。
    """
    momentum = min(100, max(0, pre_buzz * 0.45 + buzz * 0.25 + max(0, acceleration) * 100 * 0.20))
    traction = min(100, math.log1p(max(pageviews, 0)) / math.log(5000) * 100 if pageviews > 0 else 0)
    ctr_score = min(100, max(0, ctr * 10))
    return round(min(100, momentum * 0.65 + traction * 0.25 + ctr_score * 0.10), 1)


def simulate_traffic(c, trend, ts):
    pre = float(trend["pre_buzz_score"])
    buzz = float(trend["buzz_score"])
    acc = float(trend["acceleration"])

    base_impressions = max(0, int((pre * 2.8 + buzz * 1.7) * random.uniform(0.7, 1.35)))
    if acc > 0.25:
        base_impressions = int(base_impressions * random.uniform(1.15, 1.55))

    ctr = max(0.4, min(12.0, random.gauss(4.5 + min(pre, 100)/35, 1.1)))
    clicks = int(base_impressions * (ctr / 100))
    pageviews = max(clicks, int(clicks * random.uniform(1.05, 1.35)))

    old = c.execute("SELECT * FROM traffic_totals WHERE trend_id=?", (trend["id"],)).fetchone()
    total_impr = (old["impressions"] if old else 0) + base_impressions
    total_clicks = (old["clicks"] if old else 0) + clicks
    total_pv = (old["pageviews"] if old else 0) + pageviews
    current_ctr = (total_clicks / total_impr * 100) if total_impr else 0
    potential = calc_traffic_potential(pre, buzz, acc, total_pv, current_ctr)

    c.execute("""
        INSERT INTO traffic_history(
            trend_id,impressions,clicks,pageviews,ctr,traffic_potential,recorded_at
        ) VALUES(?,?,?,?,?,?,?)
    """, (trend["id"], base_impressions, clicks, pageviews, round(ctr,2), potential, ts))

    c.execute("""
        INSERT INTO traffic_totals(
            trend_id,impressions,clicks,pageviews,last_ctr,traffic_potential,updated_at
        ) VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(trend_id) DO UPDATE SET
            impressions=excluded.impressions,
            clicks=excluded.clicks,
            pageviews=excluded.pageviews,
            last_ctr=excluded.last_ctr,
            traffic_potential=excluded.traffic_potential,
            updated_at=excluded.updated_at
    """, (trend["id"], total_impr, total_clicks, total_pv, round(current_ctr,2), potential, ts))


def safe_related_candidates(keyword: str):
    """
    V4 demo only: generates search-intent labels, not factual claims.
    Production will replace this with actual related-query data.
    """
    suffixes = ["とは", "なぜ話題", "最新", "いつから", "意味", "関連", "評判"]
    return [f"{keyword} {s}" for s in suffixes]


def auto_grow_pages(c, ts):
    rows = c.execute("""
        SELECT t.*, COALESCE(x.pageviews,0) AS pageviews,
               COALESCE(x.impressions,0) AS impressions,
               COALESCE(x.last_ctr,0) AS ctr,
               COALESCE(x.traffic_potential,0) AS traffic_potential
        FROM trends t
        LEFT JOIN traffic_totals x ON x.trend_id=t.id
    """).fetchall()

    for r in rows:
        state = c.execute("SELECT * FROM growth_state WHERE trend_id=?", (r["id"],)).fetchone()
        old_level = state["level"] if state else 0

        tp = float(r["traffic_potential"])
        pv = int(r["pageviews"])
        ctr = float(r["ctr"])
        pre = float(r["pre_buzz_score"])

        # Guardrail: only strengthen pages showing both trend and traffic signals.
        quality = min(100, tp * 0.55 + min(100, pv / 8) * 0.20 + min(100, ctr * 10) * 0.10 + pre * 0.15)

        if quality >= 78 and pv >= 80:
            level, decision = 3, "強化"
            reason = "トレンド・PV・検索反応が強いため、関連検索意図を追加"
        elif quality >= 62 and pv >= 30:
            level, decision = 2, "強化"
            reason = "アクセスが伸び始めたため、補助的な関連検索意図を追加"
        elif quality >= 45:
            level, decision = 1, "維持"
            reason = "一定の反応があるためページを維持"
        else:
            level, decision = 0, "観察中"
            reason = "まだ十分なアクセスシグナルがないため自動拡張しない"

        # Do not auto-noindex in V4. Flag only; indexing changes remain a later guarded step.
        c.execute("""
            INSERT INTO growth_state(trend_id,level,quality_score,decision,last_reason,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(trend_id) DO UPDATE SET
              level=excluded.level,
              quality_score=excluded.quality_score,
              decision=excluded.decision,
              last_reason=excluded.last_reason,
              updated_at=excluded.updated_at
        """, (r["id"], level, round(quality,1), decision, reason, ts))

        if level > old_level:
            candidates = safe_related_candidates(r["keyword"])
            max_terms = 3 if level == 2 else 6 if level >= 3 else 0
            for term in candidates[:max_terms]:
                c.execute(
                    "INSERT OR IGNORE INTO related_keywords(trend_id,keyword) VALUES(?,?)",
                    (r["id"], term)
                )

        if level != old_level:
            c.execute("""
                INSERT INTO growth_log(trend_id,old_level,new_level,decision,reason,recorded_at)
                VALUES(?,?,?,?,?,?)
            """,(r["id"],old_level,level,decision,reason,ts))


def ensure_model_state(c):
    defaults = {
        "weight_pre_buzz": 0.45,
        "weight_buzz": 0.25,
        "weight_acceleration": 0.20,
        "weight_traction": 0.10,
        "hit_rate": 0.0,
        "evaluated_predictions": 0.0
    }
    for k,v in defaults.items():
        c.execute("INSERT OR IGNORE INTO model_state(key,value) VALUES(?,?)",(k,v))


def create_predictions(c, ts):
    """V15: create a real +3 hour forecast from the signals available now.

    The stored predicted_* values are TARGET values for three hours later, not
    copies of the current state.  We intentionally keep the existing table
    schema so V15 can be deployed without a database migration.
    """
    ensure_model_state(c)
    rows=c.execute("""
      SELECT t.id,t.pre_buzz_score,t.buzz_score,t.acceleration,
             COALESCE(x.traffic_potential,0) AS traffic_potential,
             COALESCE(x.pageviews,0) AS pageviews,
             COALESCE(v.velocity_30m,0) AS velocity_30m,
             COALESCE(v.velocity_1h,0) AS velocity_1h,
             COALESCE(v.velocity_3h,0) AS velocity_3h,
             COALESCE(v.velocity_score,0) AS velocity_score,
             COALESCE(cs.confidence_score,0) AS confidence_score,
             COALESCE(cs.source_count,0) AS source_count
      FROM trends t
      LEFT JOIN traffic_totals x ON x.trend_id=t.id
      LEFT JOIN v9_velocity_state v ON v.trend_id=t.id
      LEFT JOIN confidence_state cs ON cs.trend_id=t.id
    """).fetchall()

    for r in rows:
        pending=c.execute("""
          SELECT id FROM predictions WHERE trend_id=? AND status='pending'
        """,(r["id"],)).fetchone()
        if pending:
            continue

        pre=float(r["pre_buzz_score"] or 0)
        buzz=float(r["buzz_score"] or 0)
        acc=float(r["acceleration"] or 0)
        tp=float(r["traffic_potential"] or 0)
        pv=int(r["pageviews"] or 0)
        v30=float(r["velocity_30m"] or 0)
        v60=float(r["velocity_1h"] or 0)
        v180=float(r["velocity_3h"] or 0)
        vel=float(r["velocity_score"] or 0)
        conf=float(r["confidence_score"] or 0)
        sources=int(r["source_count"] or 0)

        # Only make a forecast when BUZZ NOW has a meaningful early signal.
        if pre < 55 or conf < 20:
            continue

        # Weighted recent momentum. Positive and negative movement are both
        # preserved. This is a forecast signal, not measured search volume.
        momentum = v30*0.50 + v60*0.30 + v180*0.20
        confidence_factor = 0.45 + min(1.0, conf/100.0)*0.55
        source_factor = 1.0 + min(0.20, max(0, sources-1)*0.08)

        predicted_pre=max(0.0,min(100.0, pre + momentum*0.22*confidence_factor))
        predicted_buzz=max(0.0,min(100.0, buzz + momentum*0.18*confidence_factor))
        predicted_acc=max(-1.0,min(1.0, acc + momentum/180.0))
        predicted_tp=max(0.0,min(100.0,
            tp + momentum*0.20*confidence_factor + max(0.0, vel-50.0)*0.05
        ))

        # Forecast the V13 opportunity-PV three hours ahead.  This remains a
        # forecast, not Google Analytics/Search Console measured traffic.
        growth=max(0.45,min(2.40, 1.0 + momentum/80.0))
        predicted_pv=int(max(0, round(pv * growth * source_factor)))

        c.execute("""
          INSERT INTO predictions(
            trend_id,predicted_pre_buzz,predicted_buzz,predicted_acceleration,
            predicted_traffic_potential,predicted_pageviews,horizon_ticks,
            ticks_elapsed,status,created_at
          ) VALUES(?,?,?,?,?,?,6,0,'pending',?)
        """,(
            r["id"],round(predicted_pre,1),round(predicted_buzz,1),round(predicted_acc,2),
            round(predicted_tp,1),predicted_pv,ts
        ))


def evaluate_predictions(c, ts):
    """V15: compare a forecast with REAL collected state after >= 3 hours."""
    ensure_model_state(c)
    now=datetime.fromisoformat(ts.replace('Z','+00:00'))

    pending=c.execute("""
      SELECT p.*, t.buzz_score AS actual_buzz,
             COALESCE(x.traffic_potential,0) AS actual_tp,
             COALESCE(x.pageviews,0) AS actual_pv
      FROM predictions p
      JOIN trends t ON t.id=p.trend_id
      LEFT JOIN traffic_totals x ON x.trend_id=p.trend_id
      WHERE p.status='pending'
    """).fetchall()

    for p in pending:
        try:
            created=datetime.fromisoformat(str(p["created_at"]).replace('Z','+00:00'))
            if created.tzinfo is None:
                created=created.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if (now-created).total_seconds() < 3*3600:
            continue

        # Here "gain" is prediction error (actual - forecast).  Keeping the
        # legacy column names preserves the current API/UI without migration.
        buzz_error=float(p["actual_buzz"])-float(p["predicted_buzz"])
        traffic_error=float(p["actual_tp"])-float(p["predicted_traffic_potential"])
        pv_error=int(p["actual_pv"])-int(p["predicted_pageviews"])

        buzz_abs=abs(buzz_error)
        traffic_abs=abs(traffic_error)
        pv_base=max(250, int(p["predicted_pageviews"] or 0))
        pv_pct_abs=abs(pv_error)/pv_base*100.0

        # HIT means the three-hour forecast landed inside practical tolerances.
        # It is deliberately stricter than the old "did it rise?" rule.
        hit=int(buzz_abs <= 8.0 and traffic_abs <= 10.0 and pv_pct_abs <= 35.0)
        score=max(0.0,min(100.0,
            100.0 - buzz_abs*3.0 - traffic_abs*2.2 - min(45.0,pv_pct_abs*0.65)
        ))

        c.execute("""
          INSERT OR IGNORE INTO prediction_results(
            prediction_id,trend_id,actual_buzz,actual_traffic_potential,
            actual_pageviews,buzz_gain,traffic_gain,pv_gain,hit,score,evaluated_at
          ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,(
            p["id"],p["trend_id"],p["actual_buzz"],p["actual_tp"],p["actual_pv"],
            round(buzz_error,1),round(traffic_error,1),pv_error,hit,round(score,1),ts
        ))
        c.execute("""UPDATE predictions SET status='evaluated',evaluated_at=? WHERE id=?""",
                  (ts,p["id"]))

    total=c.execute("SELECT COUNT(*) AS n FROM prediction_results").fetchone()["n"]
    hits=c.execute("SELECT COUNT(*) AS n FROM prediction_results WHERE hit=1").fetchone()["n"]
    hit_rate=(hits/total*100.0) if total else 0.0
    c.execute("""
      INSERT INTO model_state(key,value) VALUES('hit_rate',?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """,(hit_rate,))
    c.execute("""
      INSERT INTO model_state(key,value) VALUES('evaluated_predictions',?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """,(float(total),))

def cautiously_tune_model(c):
    """
    Transparent bounded adjustment. No black-box ML yet.
    Only starts after enough evaluated demo samples.
    """
    total=c.execute("SELECT COUNT(*) AS n FROM prediction_results").fetchone()["n"]
    if total < 30:
        return

    recent=c.execute("""
      SELECT r.hit,p.predicted_pre_buzz,p.predicted_buzz,p.predicted_acceleration,
             p.predicted_traffic_potential
      FROM prediction_results r
      JOIN predictions p ON p.id=r.prediction_id
      ORDER BY r.id DESC LIMIT 100
    """).fetchall()
    if not recent:
        return

    hit_acc=[abs(float(r["predicted_acceleration"])) for r in recent if r["hit"]]
    miss_acc=[abs(float(r["predicted_acceleration"])) for r in recent if not r["hit"]]
    if hit_acc and miss_acc:
        avg_hit=sum(hit_acc)/len(hit_acc)
        avg_miss=sum(miss_acc)/len(miss_acc)
        row=c.execute("SELECT value FROM model_state WHERE key='weight_acceleration'").fetchone()
        w=float(row["value"]) if row else .20
        if avg_hit > avg_miss:
            w=min(.30,w+.005)
        else:
            w=max(.12,w-.005)
        c.execute("""
          INSERT INTO model_state(key,value) VALUES('weight_acceleration',?)
          ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,(w,))


GOOGLE_TRENDS_RSS = "https://trends.google.com/trending/rss?geo=JP"
GOOGLE_TRENDS_RSS_FALLBACK = "https://trends.google.co.jp/trending/rss?geo=JP"
WIKIMEDIA_TOP = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/ja.wikipedia.org/all-access/{year}/{month}/{day}"

def _clean_keyword(s: str) -> str:
    return " ".join((s or "").replace("_"," ").split()).strip()

def _collector_state(c, source, status, message, count, ts):
    c.execute("""
      INSERT INTO collector_state(source,last_status,last_message,last_count,last_run_at)
      VALUES(?,?,?,?,?)
      ON CONFLICT(source) DO UPDATE SET
        last_status=excluded.last_status,
        last_message=excluded.last_message,
        last_count=excluded.last_count,
        last_run_at=excluded.last_run_at
    """,(source,status,message,count,ts))


def infer_category(keyword: str, context: str = "") -> str:
    """Lightweight category classifier for ranking UI.
    Uses the buzzword plus collected source titles; no external AI/API call.
    """
    s = f"{keyword or ''} {context or ''}".lower()

    groups = [
        ("スポーツ", [
            "野球","サッカー","試合","選手","投手","打者","監督","優勝","リーグ",
            "wbc","mlb","npb","jリーグ","bリーグ","バスケ","バスケット","テニス",
            "ゴルフ","相撲","オリンピック","高校野球","ファイターズ","日本ハム",
            "阪神","巨人","ドジャース","大谷","サッカー日本代表"
        ]),
        ("美容", [
            "美容","コスメ","化粧","メイク","スキンケア","美肌","毛穴","脱毛",
            "ネイル","ヘア","髪","肌荒れ","日焼け止め","美容医療","整形",
            "クリニック","ダイエット","香水","フレグランス"
        ]),
        ("ビジネス", [
            "株価","株式","日経","為替","円安","円高","金利","決算","企業","経済",
            "ipo","m&a","買収","上場","投資","ビットコイン","暗号資産","仮想通貨",
            "openai","chatgpt","生成ai","人工知能","apple","google","microsoft",
            "amazon","meta","tesla","半導体","nvidia","エヌビディア"
        ]),
        ("エンタメ", [
            "芸能","映画","ドラマ","アニメ","漫画","マンガ","俳優","女優","声優",
            "歌手","アイドル","音楽","ライブ","テレビ","番組","放送","プロデューサー",
            "監督作品","主演","出演","nhk","tbs","フジテレビ","日本テレビ","テレビ朝日",
            "netflix","youtube","youtuber","vtuber","渡鬼","紅白","舞台","映画祭"
        ]),
        ("北海道", [
            "北海道","札幌","函館","旭川","帯広","釧路","苫小牧","小樽","千歳",
            "北広島","ニセコ","すすきの","大通公園","新千歳","石狩","恵庭"
        ]),
    ]

    for category, words in groups:
        if any(w.lower() in s for w in words):
            return category

    return "ニュース・時事"


def upsert_real_trend(c, keyword, source_name, source_score, raw_metric, source_url, external_id, ts):
    keyword=_clean_keyword(keyword)
    if not keyword or len(keyword) > 80:
        return False

    c.execute("""
      INSERT INTO source_items(source,external_id,keyword,source_score,raw_metric,source_url,collected_at)
      VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(source,external_id) DO UPDATE SET
        keyword=excluded.keyword,
        source_score=excluded.source_score,
        raw_metric=excluded.raw_metric,
        source_url=excluded.source_url,
        collected_at=excluded.collected_at
    """,(source_name,external_id,keyword,source_score,raw_metric,source_url,ts))

    c.execute("""
      INSERT INTO source_snapshots(source,keyword,match_key,source_score,raw_metric,captured_at)
      VALUES(?,?,?,?,?,?)
    """,(source_name,keyword,normalize_match_key(keyword),source_score,raw_metric,ts))

    # Source score is used only as an observed signal. It is not treated as a factual article claim.
    pre=min(100, max(45, source_score))
    buzz=min(100, max(35, source_score * 0.90))
    acceleration=round(max(0.05, min(0.90, source_score/140)),2)
    status=classify(pre,buzz,acceleration)

    # IMPORTANT: different surface forms can normalize to the same slug
    # (e.g. punctuation differences). PostgreSQL correctly rejects duplicate
    # values on trends.slug, so resolve both keyword and slug BEFORE INSERT.
    try:
        slug=slugify(keyword)
    except Exception:
        slug=quote(keyword, safe="")

    row=c.execute(
        "SELECT * FROM trends WHERE keyword=? OR slug=? ORDER BY CASE WHEN keyword=? THEN 0 ELSE 1 END LIMIT 1",
        (keyword, slug, keyword)
    ).fetchone()

    if row:
        new_pre=max(float(row["pre_buzz_score"]), pre)
        new_buzz=max(float(row["buzz_score"]), buzz)
        new_acc=max(float(row["acceleration"]), acceleration)
        inferred_category = infer_category(keyword)
        c.execute("""
          UPDATE trends
          SET pre_buzz_score=?,buzz_score=?,acceleration=?,status=?,
              category=CASE
                WHEN COALESCE(category,'') IN ('','総合') THEN ?
                ELSE category
              END,
              updated_at=?
          WHERE id=?
        """,(round(new_pre,1),round(new_buzz,1),round(new_acc,2),
             classify(new_pre,new_buzz,new_acc),inferred_category,ts,row["id"]))
        return True

    c.execute("""
      INSERT INTO trends(
        keyword,slug,summary,why_now,category,pre_buzz_score,buzz_score,
        acceleration,status,first_detected_at,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """,(
        keyword,slug,
        f"{keyword} に関する検索・閲覧の増加シグナルを検出しています。",
        f"{source_name} の公開データで上昇シグナルを確認しました。詳細は元データをご確認ください。",
        infer_category(keyword),round(pre,1),round(buzz,1),round(acceleration,2),status,ts,ts
    ))
    return True

def _collector_headers(source="generic"):
    # Browser-like headers improve compatibility with public endpoints.
    # No cookies or authentication are used.
    base = {
        "User-Agent": "Mozilla/5.0 (compatible; BUZZ-NOW/1.0; +https://buzz-now.onrender.com)",
        "Accept-Language": "ja,en-US;q=0.8,en;q=0.6",
        "Cache-Control": "no-cache",
    }
    if source == "rss":
        base["Accept"] = "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.7"
    else:
        base["Accept"] = "application/json, */*;q=0.8"
    return base


def _short_preview(response):
    try:
        s = response.text.replace("\n", " ").replace("\r", " ").strip()
        return s[:160]
    except Exception:
        return ""


def _google_trends_traffic(entry):
    """Extract Google Trends RSS approximate traffic as a numeric bucket when available."""
    candidates = [
        getattr(entry, "ht_approx_traffic", None),
        getattr(entry, "approx_traffic", None),
    ]
    try:
        for item in getattr(entry, "tags", []) or []:
            if isinstance(item, dict) and "traffic" in str(item.get("term", "")).lower():
                candidates.append(item.get("label"))
    except Exception:
        pass
    import re
    for value in candidates:
        if value is None:
            continue
        text = str(value).replace(",", "").strip().upper()
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([KMB]?)", text)
        if not m:
            continue
        number = float(m.group(1))
        mult = {"": 1.0, "K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}.get(m.group(2), 1.0)
        return number * mult
    return None


def _xml_local_name(tag):
    return str(tag).split("}")[-1].split(":")[-1]


def _xml_child_text(node, wanted):
    wanted = wanted.lower()
    for child in list(node):
        if _xml_local_name(child.tag).lower() == wanted:
            return (child.text or "").strip()
    return ""


def _extract_google_trends_related_news(xml_bytes):
    """Extract article metadata embedded in Google Trends Trending Now RSS.

    No article bodies are copied. We only keep publisher/title/url metadata that
    Google Trends already associates with the trend item.
    """
    out = {}
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return out

    for item in root.iter():
        if _xml_local_name(item.tag).lower() != "item":
            continue
        keyword = _clean_keyword(_xml_child_text(item, "title"))
        if not keyword:
            continue
        articles = []
        for child in list(item):
            if _xml_local_name(child.tag).lower() != "news_item":
                continue
            title = _xml_child_text(child, "news_item_title")
            url = _xml_child_text(child, "news_item_url")
            publisher = _xml_child_text(child, "news_item_source")
            if not title or not url:
                continue
            articles.append({
                "publisher": publisher or "関連メディア",
                "title": " ".join(title.split())[:220],
                "url": url[:1200],
                "published_at": "",
                "source_label": "Google Trends 関連記事",
            })
        if articles:
            out[keyword] = articles[:8]
    return out


def _find_trend_row(c, keyword):
    keyword = _clean_keyword(keyword)
    if not keyword:
        return None
    try:
        slug = slugify(keyword)
    except Exception:
        slug = quote(keyword, safe="")
    return c.execute(
        "SELECT * FROM trends WHERE keyword=? OR slug=? ORDER BY CASE WHEN keyword=? THEN 0 ELSE 1 END LIMIT 1",
        (keyword, slug, keyword),
    ).fetchone()


def _safe_publisher_from_url(url):
    try:
        host = (urlparse(url).hostname or "").lower().replace("www.", "")
        return host[:120] or "関連メディア"
    except Exception:
        return "関連メディア"



def _parse_news_datetime(value):
    """Parse common RSS/GDELT date formats into an aware UTC datetime."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _keyword_tokens_for_news(keyword):
    """Conservative keyword tokenization for relevance checks."""
    kw = _clean_keyword(keyword)
    raw = [x for x in re.split(r"[\\s　・/／|｜]+", kw) if x]
    stop = {"の","と","で","に","を","は","が","へ","から","まで","最新","ニュース","速報"}
    tokens = [normalize_match_key(x) for x in raw if normalize_match_key(x) and x not in stop]
    return tokens or ([normalize_match_key(kw)] if normalize_match_key(kw) else [])


def _article_relevance(keyword, article, now_dt=None):
    """Return (accepted, score, reason) for one article metadata item."""
    now_dt = now_dt or datetime.now(timezone.utc)
    title = " ".join(str(article.get("title") or "").split()).strip()
    if not title:
        return False, 0, "title_empty"
    title_key = normalize_match_key(title)
    tokens = _keyword_tokens_for_news(keyword)
    if not tokens:
        return False, 0, "keyword_empty"
    matched = [t for t in tokens if t and t in title_key]
    if len(matched) != len(tokens):
        return False, round(100 * len(matched) / max(1, len(tokens)), 1), "keyword_mismatch"
    label = str(article.get("source_label") or "")
    published = _parse_news_datetime(article.get("published_at"))
    freshness = 0
    if published:
        age_hours = max(0.0, (now_dt - published).total_seconds() / 3600.0)
        if age_hours <= 72:
            freshness = 25
        elif age_hours <= 168:
            freshness = 10
        else:
            return False, 75, "older_than_7d"
    elif "Google Trends" in label:
        freshness = 15
    phrase = normalize_match_key(_clean_keyword(keyword))
    phrase_bonus = 20 if phrase and phrase in title_key else 0
    score = min(100.0, 70.0 + freshness + phrase_bonus)
    return True, round(score, 1), "accepted"


def _filter_relevant_articles(keyword, articles, maxrecords=8):
    accepted = []
    rejected = []
    seen_titles = set()
    now_dt = datetime.now(timezone.utc)
    for a in articles or []:
        ok, score, reason = _article_relevance(keyword, a, now_dt)
        title = " ".join(str(a.get("title") or "").split()).strip()
        key = normalize_match_key(title)
        if key and key in seen_titles:
            continue
        if ok:
            seen_titles.add(key)
            item = dict(a)
            item["relevance_score"] = score
            accepted.append(item)
        else:
            rejected.append({"title": title[:120], "reason": reason, "score": score})
    accepted.sort(key=lambda a: (float(a.get("relevance_score") or 0), _parse_news_datetime(a.get("published_at")) or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
    return accepted[:maxrecords], rejected


def _clear_auto_news_for_trend(c, trend_id, keyword):
    """Remove weak auto-news rows created by older versions before rebuilding."""
    c.execute("""
      DELETE FROM sources
      WHERE trend_id=?
        AND (
          source_label LIKE 'Bing News%%'
          OR source_label LIKE 'Google News%%'
          OR source_label='関連報道'
          OR source_label='Google Trends 関連記事'
        )
    """, (trend_id,))
    c.execute("DELETE FROM related_keywords WHERE trend_id=? AND keyword LIKE ?", (trend_id, f"{_clean_keyword(keyword)} %"))


def _store_article_sources(c, keyword, articles, ts):
    trend = _find_trend_row(c, keyword)
    if not trend:
        return 0
    relevant, rejected = _filter_relevant_articles(keyword, articles, 8)
    inserted = 0
    for a in relevant:
        url = str(a.get("url") or "").strip()
        title = " ".join(str(a.get("title") or "").split()).strip()
        if not url.startswith(("http://", "https://")) or not title:
            continue
        publisher = " ".join(str(a.get("publisher") or "").split()).strip() or _safe_publisher_from_url(url)
        published_at = str(a.get("published_at") or "").strip()
        label = str(a.get("source_label") or "関連報道").strip()[:80]
        before = c.execute("SELECT id FROM sources WHERE trend_id=? AND url=?", (trend["id"], url)).fetchone()
        c.execute("""
          INSERT INTO sources(trend_id,publisher,title,url,published_at,source_label)
          VALUES(?,?,?,?,?,?)
          ON CONFLICT(trend_id,url) DO UPDATE SET
            publisher=excluded.publisher,
            title=excluded.title,
            published_at=CASE WHEN excluded.published_at<>'' THEN excluded.published_at ELSE sources.published_at END,
            source_label=excluded.source_label
        """, (trend["id"], publisher[:160], title[:260], url[:1400], published_at[:80], label))
        if not before:
            inserted += 1
    _refresh_why_now_from_sources(c, trend["id"], keyword, ts)
    return inserted

def _refresh_why_now_from_sources(c, trend_id, keyword, ts):
    rows = c.execute("""
      SELECT publisher,title,url,published_at,source_label
      FROM sources
      WHERE trend_id=?
      ORDER BY CASE WHEN published_at='' THEN 1 ELSE 0 END, published_at DESC, id DESC
      LIMIT 10
    """, (trend_id,)).fetchall()
    if not rows:
        return

    # Deduplicate by title and count independent publishers. We deliberately use
    # cautious language: coverage timing can correlate with search growth but does
    # not prove causation.
    unique = []
    seen = set()
    publishers = set()
    for r in rows:
        title = " ".join(str(r["title"] or "").split()).strip()
        if not title:
            continue
        key = normalize_match_key(title)
        if key in seen:
            continue
        seen.add(key)
        unique.append(title)
        pub = " ".join(str(r["publisher"] or "").split()).strip()
        if pub:
            publishers.add(pub)

    if not unique:
        return
    shown = [t[:70] + ("…" if len(t) > 70 else "") for t in unique[:3]]
    if len(shown) == 1:
        topics = f"「{shown[0]}」という関連情報が確認されています。"
    else:
        topics = "、".join(f"「{x}」" for x in shown) + "などの関連情報が確認されています。"

    if len(publishers) >= 2:
        coverage = f"直近の公開情報では、{len(publishers)}媒体以上から関連する記事・発表を確認。"
    else:
        coverage = "直近の公開情報で、関連する記事・発表を確認しています。"

    why = (
        f"{keyword}について、{coverage}{topics} "
        "BUZZ NOWでも検索・閲覧シグナルの上昇を検知しており、これらの情報公開と近いタイミングで注目が高まっている可能性があります。"
    )
    c.execute("UPDATE trends SET why_now=?, summary=?, updated_at=? WHERE id=?", (
        why[:1200],
        f"{keyword}の最新トレンドを、検索・閲覧シグナルと関連する公開情報から整理しています。"[:400],
        ts,
        trend_id,
    ))


def _gdelt_query_variants(keyword):
    """Build conservative GDELT queries from exact -> slightly broader.

    We only use returned article metadata (title/domain/url/date), never article bodies.
    """
    kw = _clean_keyword(keyword)
    if not kw:
        return []
    safe = kw.replace('"', ' ').strip()
    compact = re.sub(r"\s+", "", safe)
    variants = [f'"{safe}" sourcelang:japanese']
    if compact and compact != safe:
        variants.append(f'"{compact}" sourcelang:japanese')
    # Last fallback keeps every keyword token but relaxes the phrase constraint.
    variants.append(f'{safe} sourcelang:japanese')
    # Some Japanese publishers are not consistently tagged with language metadata.
    variants.append(f'"{safe}"')
    out=[]
    seen=set()
    for q in variants:
        q=" ".join(q.split())
        if q and q not in seen:
            seen.add(q)
            out.append(q)
    return out


def _fetch_gdelt_articles_for_keyword(keyword, maxrecords=8):
    """Fetch recent related article metadata for one keyword from GDELT."""
    maxrecords=max(1,min(12,int(maxrecords)))
    collected=[]
    seen_urls=set()
    seen_titles=set()
    for query in _gdelt_query_variants(keyword):
        params={
            "query": query,
            "mode": "artlist",
            "maxrecords": str(maxrecords),
            "timespan": "48h",
            "sort": "datedesc",
            "format": "json",
        }
        try:
            r=httpx.get(
                GDELT_DOC_API,
                params=params,
                timeout=12,
                follow_redirects=True,
                headers=_collector_headers("json"),
            )
            if r.status_code != 200:
                logger.warning("GDELT HTTP %s keyword=%s query=%s preview=%s", r.status_code, keyword, query, _short_preview(r))
                continue
            payload=r.json()
            for a in (payload.get("articles") or []):
                url=str(a.get("url") or "").strip()
                title=" ".join(str(a.get("title") or "").split()).strip()
                if not url.startswith(("http://","https://")) or not title:
                    continue
                tkey=normalize_match_key(title)
                if url in seen_urls or tkey in seen_titles:
                    continue
                seen_urls.add(url)
                seen_titles.add(tkey)
                publisher=str(a.get("domain") or "").strip() or _safe_publisher_from_url(url)
                collected.append({
                    "publisher": publisher,
                    "title": title,
                    "url": url,
                    "published_at": str(a.get("seendate") or "").strip(),
                    "source_label": "関連報道",
                })
                if len(collected) >= maxrecords:
                    break
            # If the exact/compact query already finds enough useful results, stop.
            if len(collected) >= min(3,maxrecords):
                break
        except Exception as e:
            logger.warning("GDELT collector failed keyword=%s query=%s: %s", keyword, query, e)
    return collected[:maxrecords]





# V24: Bing News RSS fallback + strict relevance filtering for Render environments where Google News can return HTTP 429.
BING_NEWS_RSS_SEARCH = "https://www.bing.com/news/search"

def _fetch_bing_news_rss_for_keyword(keyword, maxrecords=8):
    """Fetch Bing News RSS metadata only; article bodies are never copied."""
    kw=_clean_keyword(keyword)
    if not kw:
        return [], "empty keyword"
    params={"q": kw, "format":"RSS", "mkt":"ja-JP", "setlang":"ja"}
    try:
        r=httpx.get(BING_NEWS_RSS_SEARCH, params=params, timeout=12, follow_redirects=True, headers=_collector_headers("rss"))
        if r.status_code != 200:
            return [], f"HTTP {r.status_code}: {_short_preview(r)}"
        feed=feedparser.parse(r.content)
        out=[]; seen=set()
        for e in list(feed.entries)[:maxrecords*3]:
            title=" ".join(str(getattr(e,"title","") or "").split()).strip()
            url=str(getattr(e,"link","") or "").strip()
            if not title or not url.startswith(("http://","https://")):
                continue
            key=normalize_match_key(title)
            if key in seen:
                continue
            seen.add(key)
            publisher="Bing News 掲載メディア"
            src=getattr(e,"source",None)
            if src:
                try:
                    publisher=str(src.get("title") or src.get("href") or publisher) if isinstance(src,dict) else str(getattr(src,"title",publisher))
                except Exception:
                    pass
            if " - " in title:
                maybe=title.rsplit(" - ",1)[-1].strip()
                if maybe and len(maybe)<100:
                    publisher=maybe
            published=str(getattr(e,"published","") or getattr(e,"updated","") or "")
            out.append({"publisher":publisher,"title":title,"url":url,"published_at":published,"source_label":"Bing News 関連記事"})
            if len(out)>=maxrecords:
                break
        return out, f"ok {len(out)} entries"
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"

# V21: resilient WHY NOW discovery via Google News RSS metadata fallback.
GOOGLE_NEWS_RSS_SEARCH = "https://news.google.com/rss/search"

def _fetch_google_news_rss_for_keyword(keyword, maxrecords=8):
    """Fetch recent Google News RSS result metadata. No article bodies are copied."""
    kw=_clean_keyword(keyword)
    if not kw:
        return [], "empty keyword"
    params={"q": kw, "hl":"ja", "gl":"JP", "ceid":"JP:ja"}
    try:
        r=httpx.get(GOOGLE_NEWS_RSS_SEARCH, params=params, timeout=12, follow_redirects=True, headers=_collector_headers("rss"))
        if r.status_code != 200:
            return [], f"HTTP {r.status_code}: {_short_preview(r)}"
        feed=feedparser.parse(r.content)
        out=[]; seen=set()
        for e in list(feed.entries)[:maxrecords*2]:
            title=" ".join(str(getattr(e,"title","") or "").split()).strip()
            url=str(getattr(e,"link","") or "").strip()
            if not title or not url.startswith(("http://","https://")):
                continue
            key=normalize_match_key(title)
            if key in seen:
                continue
            seen.add(key)
            publisher="Google News 掲載メディア"
            src=getattr(e,"source",None)
            if src:
                try:
                    publisher=str(src.get("title") or src.get("href") or publisher) if isinstance(src,dict) else str(getattr(src,"title",publisher))
                except Exception:
                    pass
            # Many Google News titles end with " - publisher". Use that as a safe display fallback.
            if " - " in title:
                maybe=title.rsplit(" - ",1)[-1].strip()
                if maybe and len(maybe)<100:
                    publisher=maybe
            published=str(getattr(e,"published","") or "")
            out.append({"publisher":publisher,"title":title,"url":url,"published_at":published,"source_label":"Google News 関連記事"})
            if len(out)>=maxrecords:
                break
        return out, f"ok {len(out)} entries"
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"

def _derive_related_from_sources(c, trend_id, keyword):
    """Create conservative related-search phrases from collected article titles."""
    rows=c.execute("SELECT title FROM sources WHERE trend_id=? ORDER BY id DESC LIMIT 12",(trend_id,)).fetchall()
    if not rows:
        return 0
    base=_clean_keyword(keyword)
    candidates=[f"{base} なぜ話題", f"{base} 最新", f"{base} ニュース"]
    # Extract quoted/bracketed named phrases only; avoid inventing facts.
    for r in rows:
        title=str(r["title"] or "")
        for pat in [r"[「『【]([^」』】]{2,28})[」』】]", r"([A-Za-z0-9ぁ-んァ-ヶ一-龠々ー]{3,20})"]:
            for m in re.findall(pat,title):
                term=" ".join(str(m).split()).strip()
                if term and normalize_match_key(term)!=normalize_match_key(base):
                    candidates.append(f"{base} {term}")
    inserted=0; seen=set()
    for term in candidates:
        key=normalize_match_key(term)
        if not key or key in seen:
            continue
        seen.add(key)
        before=c.execute("SELECT id FROM related_keywords WHERE trend_id=? AND keyword=?",(trend_id,term)).fetchone()
        c.execute("INSERT OR IGNORE INTO related_keywords(trend_id,keyword) VALUES(?,?)",(trend_id,term[:120]))
        if not before:
            inserted+=1
        if inserted>=8:
            break
    return inserted

def _record_news_diagnostic(c, trend_id, provider, message, ts):
    c.execute("""INSERT INTO system_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
              (f"news_diag:{trend_id}:{provider}", f"{ts} | {message}"[:1000]))


def _mark_news_checked(c, trend_id, ts):
    key=f"news_checked:{trend_id}"
    c.execute("""
      INSERT INTO system_state(key,value) VALUES(?,?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """,(key,ts))


def _news_check_is_due(c, trend_id, hours=2):
    row=c.execute("SELECT value FROM system_state WHERE key=?",(f"news_checked:{trend_id}",)).fetchone()
    if not row:
        return True
    try:
        last=datetime.fromisoformat(str(row["value"]).replace("Z","+00:00"))
        if last.tzinfo is None:
            last=last.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc)-last >= timedelta(hours=hours)
    except Exception:
        return True


def _enrich_keyword_news(c, keyword, ts, force=False, include_gdelt=True):
    """V24: high-precision news enrichment with relevance diagnostics."""
    if not NEWS_ENRICHMENT_ENABLED:
        return 0
    trend=_find_trend_row(c,keyword)
    if not trend:
        return 0
    if not force and not _news_check_is_due(c,trend["id"]):
        return 0
    _clear_auto_news_for_trend(c, trend["id"], keyword)
    total=0
    try:
        bnews,msg=_fetch_bing_news_rss_for_keyword(keyword,12)
        accepted,rejected=_filter_relevant_articles(keyword,bnews,8)
        diag=f"{msg} | accepted={len(accepted)} rejected={len(rejected)}"
        if rejected:
            diag += " | reject_examples=" + ",".join(f"{x['reason']}:{x['title'][:32]}" for x in rejected[:3])
        _record_news_diagnostic(c,trend["id"],"bing_news",diag,ts)
        if accepted:
            total += _store_article_sources(c,keyword,accepted,ts)
    except Exception as e:
        _record_news_diagnostic(c,trend["id"],"bing_news",f"error {type(e).__name__}: {e}",ts)
    if include_gdelt and GDELT_NEWS_ENABLED:
        try:
            articles=_fetch_gdelt_articles_for_keyword(keyword,12)
            accepted,rejected=_filter_relevant_articles(keyword,articles,8)
            _record_news_diagnostic(c,trend["id"],"gdelt",f"raw={len(articles)} accepted={len(accepted)} rejected={len(rejected)}",ts)
            if accepted:
                total += _store_article_sources(c,keyword,accepted,ts)
        except Exception as e:
            _record_news_diagnostic(c,trend["id"],"gdelt",f"error {type(e).__name__}: {e}",ts)
    if total == 0:
        try:
            gnews,msg=_fetch_google_news_rss_for_keyword(keyword,12)
            accepted,rejected=_filter_relevant_articles(keyword,gnews,8)
            _record_news_diagnostic(c,trend["id"],"google_news",f"{msg} | accepted={len(accepted)} rejected={len(rejected)}",ts)
            if accepted:
                total += _store_article_sources(c,keyword,accepted,ts)
        except Exception as e:
            _record_news_diagnostic(c,trend["id"],"google_news",f"error {type(e).__name__}: {e}",ts)
    if total > 0:
        _derive_related_from_sources(c,trend["id"],keyword)
    else:
        fallback=(f"{keyword}はBUZZ NOWの公開データ分析で上昇シグナルを検知しています。" "現在、注目上昇の理由として十分に関連性の高い最新記事は確認できていません。" "新しい公式発表・報道を継続して確認しています。")
        c.execute("UPDATE trends SET why_now=?, updated_at=? WHERE id=?",(fallback,ts,trend["id"]))
    _mark_news_checked(c,trend["id"],ts)
    return total

def collect_fast_news(c, ts, limit=6):
    """Fast routine news enrichment using Bing/Google RSS only.

    GDELT is intentionally excluded from scheduled collection because its public
    endpoint rate-limits repeated keyword queries. GDELT remains available for
    explicit force diagnostics/detail checks where include_gdelt=True.
    """
    if not NEWS_ENRICHMENT_ENABLED:
        return 0
    limit=max(0, min(10, int(limit)))
    if limit <= 0:
        return 0

    trends=c.execute("""
      SELECT id,keyword,pre_buzz_score,buzz_score,updated_at
      FROM trends
      WHERE is_indexable=1 AND pre_buzz_score>=55
      ORDER BY pre_buzz_score DESC, acceleration DESC, buzz_score DESC
      LIMIT ?
    """, (limit,)).fetchall()

    total=0
    for trend in trends:
        keyword=_clean_keyword(trend["keyword"])
        if not keyword or len(keyword)>60:
            continue
        total += _enrich_keyword_news(c, keyword, ts, include_gdelt=False)
    return total

def collect_gdelt_news(c, ts, limit=None):
    """Manual/diagnostic GDELT enrichment. Not used by routine collection."""
    if not NEWS_ENRICHMENT_ENABLED or not GDELT_NEWS_ENABLED:
        return 0
    limit = GDELT_NEWS_LIMIT if limit is None else max(0, min(3, int(limit)))
    if limit <= 0:
        return 0
    trends=c.execute("""
      SELECT id,keyword,pre_buzz_score,buzz_score,updated_at
      FROM trends
      WHERE is_indexable=1 AND pre_buzz_score>=55
      ORDER BY pre_buzz_score DESC, acceleration DESC, buzz_score DESC
      LIMIT ?
    """, (limit,)).fetchall()
    total=0
    for trend in trends:
        keyword=_clean_keyword(trend["keyword"])
        if not keyword or len(keyword)>60:
            continue
        total += _enrich_keyword_news(c, keyword, ts, include_gdelt=True)
    return total

def collect_google_trends(c, ts):
    source = "google_trends_jp"
    endpoints = [
        GOOGLE_TRENDS_RSS,
        GOOGLE_TRENDS_RSS_FALLBACK,
    ]
    errors = []

    for url in endpoints:
        try:
            r = httpx.get(
                url,
                timeout=15,
                follow_redirects=True,
                headers=_collector_headers("rss"),
            )
            if r.status_code != 200:
                errors.append(f"{r.status_code} {url} {_short_preview(r)}")
                logger.warning("Google Trends collector HTTP %s url=%s preview=%s",
                               r.status_code, url, _short_preview(r))
                continue

            feed = feedparser.parse(r.content)
            entries = list(feed.entries[:30])
            related_news = _extract_google_trends_related_news(r.content) if NEWS_ENRICHMENT_ENABLED else {}

            if not entries:
                content_type = r.headers.get("content-type", "")
                bozo = getattr(feed, "bozo", 0)
                err = f"0 entries / content-type={content_type} / bozo={bozo} / preview={_short_preview(r)}"
                errors.append(err)
                logger.warning("Google Trends RSS parsed zero entries: %s", err)
                continue

            count = 0
            total = max(1, len(entries))
            for idx, e in enumerate(entries):
                kw = _clean_keyword(getattr(e, "title", ""))
                if not kw:
                    continue
                score = max(52, 98 - idx * (44 / max(1, total - 1)))
                ext = str(getattr(e, "id", "") or getattr(e, "link", "") or kw)
                link = str(getattr(e, "link", "") or url)
                traffic = _google_trends_traffic(e)
                # Prefer Google's approximate traffic bucket. If unavailable, use inverse rank
                # only as a weak fallback; V11 also scores appearance/newness/propagation.
                raw_metric = traffic if traffic is not None else float(total - idx)
                if upsert_real_trend(c, kw, "Google Trends", score, raw_metric, link, ext, ts):
                    count += 1
                    if related_news.get(kw):
                        _store_article_sources(c, kw, related_news[kw], ts)

            if count > 0:
                msg = f"Google Trends RSS取得成功 / {count}件 / {url}"
                _collector_state(c, source, "ok", msg, count, ts)
                logger.info(msg)
                return count

            errors.append(f"parsed {len(entries)} entries but inserted 0 / {url}")

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors.append(msg)
            logger.exception("Google Trends collector failed url=%s", url)

    final = " | ".join(errors)[-900:] if errors else "unknown error"
    _collector_state(c, source, "error", final, 0, ts)
    logger.error("Google Trends collector exhausted fallbacks: %s", final)
    return 0


def collect_wikimedia(c, ts):
    from datetime import datetime, timedelta, timezone
    source = "wikimedia_ja"

    # Try yesterday first, then two days ago in case the daily dump is delayed.
    days = [
        datetime.now(timezone.utc) - timedelta(days=1),
        datetime.now(timezone.utc) - timedelta(days=2),
    ]
    errors = []

    for d in days:
        url = WIKIMEDIA_TOP.format(
            year=d.strftime("%Y"),
            month=d.strftime("%m"),
            day=d.strftime("%d"),
        )
        try:
            r = httpx.get(
                url,
                timeout=15,
                follow_redirects=True,
                headers=_collector_headers("json"),
            )
            if r.status_code != 200:
                errors.append(f"{r.status_code} {d.strftime('%Y-%m-%d')} {_short_preview(r)}")
                logger.warning("Wikimedia collector HTTP %s url=%s preview=%s",
                               r.status_code, url, _short_preview(r))
                continue

            payload = r.json()
            items = payload.get("items") or []
            articles = (items[0].get("articles") if items else []) or []

            if not articles:
                errors.append(f"200 but no articles {d.strftime('%Y-%m-%d')}")
                logger.warning("Wikimedia returned no articles for %s", d.strftime("%Y-%m-%d"))
                continue

            blocked = {"メインページ", "特別:検索", "Special:Search", "Main Page", "Main_Page"}
            clean = [
                a for a in articles
                if _clean_keyword(a.get("article", "")) not in blocked
            ][:50]

            max_views = max([int(a.get("views", 0) or 0) for a in clean] or [1])
            count = 0

            for idx, a in enumerate(clean):
                kw = _clean_keyword(a.get("article", ""))
                views = int(a.get("views", 0) or 0)
                if not kw:
                    continue
                rank_signal = max(45, 88 - idx * 1.0)
                view_signal = min(100, (views / max_views) * 100)
                score = round(rank_signal * 0.7 + view_signal * 0.3, 1)
                page_url = "https://ja.wikipedia.org/wiki/" + quote(
                    str(a.get("article", "")).replace(" ", "_")
                )
                ext = f"{d.strftime('%Y%m%d')}:{a.get('article','')}"
                if upsert_real_trend(
                    c, kw, "Wikimedia Pageviews", score, views,
                    page_url, ext, ts
                ):
                    count += 1

            if count > 0:
                msg = f"Wikimedia Pageviews取得成功 / {count}件 / {d.strftime('%Y-%m-%d')}"
                _collector_state(c, source, "ok", msg, count, ts)
                logger.info(msg)
                return count

            errors.append(f"articles={len(articles)} but inserted 0 {d.strftime('%Y-%m-%d')}")

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors.append(msg)
            logger.exception("Wikimedia collector failed url=%s", url)

    final = " | ".join(errors)[-900:] if errors else "unknown error"
    _collector_state(c, source, "error", final, 0, ts)
    logger.error("Wikimedia collector exhausted fallbacks: %s", final)
    return 0


def normalize_match_key(keyword: str) -> str:
    s=_clean_keyword(keyword).lower()
    for ch in [" ","　","・","-","_","/","／","(",")","（","）","[","]","【","】","!","！","?","？"]:
        s=s.replace(ch,"")
    return s

def refresh_confidence(c, ts):
    """
    Cross-source corroboration.
    Exact normalized keyword matches are intentionally conservative in V8.
    Fuzzy/entity matching comes later to avoid false merges.
    """
    trends=c.execute("SELECT id,keyword,pre_buzz_score,buzz_score,acceleration FROM trends").fetchall()
    items=c.execute("""
      SELECT source,keyword,source_score,raw_metric,collected_at
      FROM source_items
      ORDER BY id DESC
    """).fetchall()

    by_key={}
    for item in items:
        k=normalize_match_key(item["keyword"])
        if not k:
            continue
        by_key.setdefault(k,{})
        # Keep the newest/highest signal per source.
        old=by_key[k].get(item["source"])
        if old is None or float(item["source_score"])>float(old["source_score"]):
            by_key[k][item["source"]]=item

    for t in trends:
        k=normalize_match_key(t["keyword"])
        matches=by_key.get(k,{})
        source_count=len(matches)
        source_scores=[float(x["source_score"]) for x in matches.values()]

        if source_count >= 3:
            label="複数ソース一致"
            source_bonus=28
        elif source_count == 2:
            label="2ソース一致"
            source_bonus=18
        elif source_count == 1:
            label="単独シグナル"
            source_bonus=4
        else:
            label="デモ/未確認"
            source_bonus=0

        signal_avg=(sum(source_scores)/len(source_scores)) if source_scores else 0
        acc=max(0,float(t["acceleration"]))
        confidence=min(100,
            signal_avg*0.55
            + min(100,acc*120)*0.20
            + float(t["pre_buzz_score"])*0.10
            + source_bonus
        )
        corroborated=int(source_count>=2)

        c.execute("""
          INSERT INTO confidence_state(
            trend_id,source_count,confidence_score,confidence_label,corroborated,updated_at
          ) VALUES(?,?,?,?,?,?)
          ON CONFLICT(trend_id) DO UPDATE SET
            source_count=excluded.source_count,
            confidence_score=excluded.confidence_score,
            confidence_label=excluded.confidence_label,
            corroborated=excluded.corroborated,
            updated_at=excluded.updated_at
        """,(t["id"],source_count,round(confidence,1),label,corroborated,ts))

        # Cross-source agreement may strengthen an existing observed trend,
        # but does not invent any event/factual explanation.
        if corroborated:
            boosted_pre=min(100,max(float(t["pre_buzz_score"]),confidence))
            boosted_acc=min(.95,max(float(t["acceleration"]),0.28 + .08*(source_count-2)))
            c.execute("""
              UPDATE trends SET pre_buzz_score=?,acceleration=?,status=?,updated_at=?
              WHERE id=?
            """,(
              round(boosted_pre,1),round(boosted_acc,2),
              classify(boosted_pre,float(t["buzz_score"]),boosted_acc),ts,t["id"]
            ))


def _parse_iso(s):
    from datetime import datetime
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z","+00:00"))
    except Exception:
        return None

def _velocity_for_window(rows, now_dt, minutes):
    """
    Score delta per hour using the oldest snapshot inside the requested window
    and the latest snapshot. Positive = accelerating, negative = cooling.
    """
    from datetime import timedelta
    cutoff=now_dt-timedelta(minutes=minutes)
    usable=[r for r in rows if (_parse_iso(r["captured_at"]) or now_dt) >= cutoff]
    if len(usable)<2:
        return 0.0
    usable=sorted(usable,key=lambda x:x["captured_at"])
    a,b=usable[0],usable[-1]
    ta,tb=_parse_iso(a["captured_at"]),_parse_iso(b["captured_at"])
    if not ta or not tb or tb<=ta:
        return 0.0
    hours=max((tb-ta).total_seconds()/3600,1/60)
    return round((float(b["source_score"])-float(a["source_score"]))/hours,2)

def refresh_propagation(c, ts):
    from datetime import datetime, timezone
    now_dt=_parse_iso(ts) or datetime.now(timezone.utc)

    trends=c.execute("SELECT id,keyword FROM trends").fetchall()
    all_rows=c.execute("""
      SELECT source,keyword,match_key,source_score,raw_metric,captured_at
      FROM source_snapshots
      ORDER BY captured_at ASC,id ASC
    """).fetchall()

    by_key={}
    for r in all_rows:
        by_key.setdefault(r["match_key"],[]).append(r)

    for t in trends:
        key=normalize_match_key(t["keyword"])
        rows=by_key.get(key,[])
        if not rows:
            continue

        # First observation for each source.
        first_by_source={}
        for r in rows:
            s=r["source"]
            if s not in first_by_source:
                first_by_source[s]=r

        ordered=sorted(first_by_source.values(),key=lambda x:x["captured_at"])
        first=ordered[0] if ordered else None
        second=ordered[1] if len(ordered)>1 else None

        first_dt=_parse_iso(first["captured_at"]) if first else None
        second_dt=_parse_iso(second["captured_at"]) if second else None
        prop=None
        if first_dt and second_dt:
            prop=round(max(0,(second_dt-first_dt).total_seconds()/60),1)

        sequence=" → ".join([x["source"] for x in ordered[:5]])

        # Aggregate latest source scores into one cross-source timeline.
        # For each timestamp use the average score observed in that collection wave.
        waves={}
        for r in rows:
            waves.setdefault(r["captured_at"],[]).append(float(r["source_score"]))
        aggregate=[
          {"captured_at":k,"source_score":sum(v)/len(v)}
          for k,v in waves.items()
        ]

        v30=_velocity_for_window(aggregate,now_dt,30)
        v60=_velocity_for_window(aggregate,now_dt,60)
        v180=_velocity_for_window(aggregate,now_dt,180)

        c.execute("""
          INSERT INTO propagation_state(
            trend_id,first_source,first_seen_at,second_source,second_seen_at,
            propagation_minutes,source_sequence,velocity_30m,velocity_1h,velocity_3h,updated_at
          ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(trend_id) DO UPDATE SET
            first_source=excluded.first_source,
            first_seen_at=excluded.first_seen_at,
            second_source=excluded.second_source,
            second_seen_at=excluded.second_seen_at,
            propagation_minutes=excluded.propagation_minutes,
            source_sequence=excluded.source_sequence,
            velocity_30m=excluded.velocity_30m,
            velocity_1h=excluded.velocity_1h,
            velocity_3h=excluded.velocity_3h,
            updated_at=excluded.updated_at
        """,(
          t["id"],
          first["source"] if first else "",
          first["captured_at"] if first else "",
          second["source"] if second else "",
          second["captured_at"] if second else "",
          prop,sequence,v30,v60,v180,ts
        ))

    # Keep snapshot history bounded in demo/local usage.
    c.execute("""
      DELETE FROM source_snapshots
      WHERE id NOT IN (
        SELECT id FROM source_snapshots ORDER BY id DESC LIMIT 12000
      )
    """)


MONETIZE_RULES = {
    "beauty": {
        "words":["美容","コスメ","メイク","リップ","スキンケア","脱毛","クリニック","肌","ヘア"],
        "base":88, "mode":"affiliate"
    },
    "travel": {
        "words":["ホテル","旅行","航空","温泉","宿","観光","ツアー","旅館"],
        "base":82, "mode":"affiliate"
    },
    "jobs": {
        "words":["転職","求人","アルバイト","副業","仕事","採用"],
        "base":90, "mode":"affiliate"
    },
    "finance": {
        "words":["クレジット","カード","証券","投資","保険","ローン","FX"],
        "base":94, "mode":"affiliate"
    },
    "shopping": {
        "words":["iPhone","スマホ","家電","バッグ","商品","新作","発売","ガジェット"],
        "base":78, "mode":"affiliate"
    },
    "food": {
        "words":["グルメ","ラーメン","カフェ","レストラン","焼肉","寿司","新店"],
        "base":62, "mode":"hybrid"
    },
    "entertainment": {
        "words":["芸能","ドラマ","映画","アニメ","アイドル","俳優","歌手"],
        "base":35, "mode":"adsense"
    },
}

def classify_monetize_intent(keyword, category=""):
    text=(keyword+" "+(category or "")).lower()
    best=("general",42,"adsense")
    for name,r in MONETIZE_RULES.items():
        hits=sum(1 for w in r["words"] if w.lower() in text)
        if hits and r["base"] + min(8,(hits-1)*4) > best[1]:
            best=(name,r["base"] + min(8,(hits-1)*4),r["mode"])
    return best

def refresh_monetization(c, ts):
    rows=c.execute("""
      SELECT t.id,t.keyword,t.category,t.pre_buzz_score,t.buzz_score,t.acceleration,
             COALESCE(tt.traffic_potential,0) AS traffic_potential,
             COALESCE(cs.confidence_score,0) AS confidence_score
      FROM trends t
      LEFT JOIN traffic_totals tt ON tt.trend_id=t.id
      LEFT JOIN confidence_state cs ON cs.trend_id=t.id
    """).fetchall()
    for r in rows:
        intent,commercial,mode=classify_monetize_intent(r["keyword"],r["category"])
        demand=(float(r["traffic_potential"])*0.38 +
                float(r["pre_buzz_score"])*0.20 +
                float(r["buzz_score"])*0.12 +
                float(r["confidence_score"])*0.20 +
                min(100,max(0,float(r["acceleration"])*100))*0.10)
        score=min(100,round(commercial*0.58+demand*0.42,1))
        grade="S" if score>=85 else "A" if score>=72 else "B" if score>=58 else "C"
        recommended=mode
        if mode=="affiliate" and score<58:
            recommended="adsense"
        elif mode=="affiliate" and score>=72:
            recommended="affiliate"
        elif mode=="hybrid":
            recommended="hybrid"
        reason=f"{intent} intent / commercial {commercial} / demand {round(demand,1)}"
        c.execute("""
          INSERT INTO monetization_state(
            trend_id,monetize_score,monetize_grade,intent_category,recommended_mode,reason,updated_at
          ) VALUES(?,?,?,?,?,?,?)
          ON CONFLICT(trend_id) DO UPDATE SET
            monetize_score=excluded.monetize_score,
            monetize_grade=excluded.monetize_grade,
            intent_category=excluded.intent_category,
            recommended_mode=excluded.recommended_mode,
            reason=excluded.reason,
            updated_at=excluded.updated_at
        """,(r["id"],score,grade,intent,recommended,reason,ts))



def _v9_parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _v11_window_signal(rows, minutes, now_dt):
    """Composite movement points for a window.

    Uses provider-local metric/rank movement plus appearance recency. This avoids pretending
    that daily Wikimedia totals are a 30-minute counter while still capturing a newly emerging
    signal. Returned value is a signal-point delta, not a literal percentage.
    """
    if not rows:
        return 0.0
    by_source = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)

    source_signals = []
    for source_rows in by_source.values():
        source_rows = sorted(source_rows, key=lambda r: str(r["captured_at"]))
        latest = source_rows[-1]
        latest_dt = _v9_parse_dt(latest["captured_at"])
        first_dt = _v9_parse_dt(source_rows[0]["captured_at"])
        if not latest_dt:
            continue

        target = latest_dt - timedelta(minutes=minutes)
        candidates = [r for r in source_rows[:-1] if (_v9_parse_dt(r["captured_at"]) or latest_dt) <= target]
        signal = 0.0

        if candidates:
            base = candidates[-1]
            latest_raw, base_raw = latest["raw_metric"], base["raw_metric"]
            if latest_raw is not None and base_raw is not None and float(base_raw) != 0 and float(latest_raw) != float(base_raw):
                pct = ((float(latest_raw) - float(base_raw)) / abs(float(base_raw))) * 100.0
                signal += max(-60.0, min(60.0, pct * 0.35))

            # source_score is rank-derived for Google Trends and therefore useful as a weak
            # movement signal even when traffic remains in the same bucket.
            score_delta = float(latest["source_score"] or 0) - float(base["source_score"] or 0)
            signal += max(-25.0, min(25.0, score_delta * 1.25))
        elif first_dt:
            age_minutes = max(0.0, (now_dt - first_dt).total_seconds() / 60.0)
            if age_minutes <= minutes:
                # A newly appearing signal is itself meaningful for pre-buzz detection.
                signal += 18.0 if minutes <= 30 else (12.0 if minutes <= 60 else 7.0)

        source_signals.append(signal)

    if not source_signals:
        return 0.0
    return round(sum(source_signals) / len(source_signals), 2)


def refresh_v9_velocity(c, ts):
    """V11 REAL VELOCITY: movement + new appearance + cross-source propagation."""
    now_dt = _v9_parse_dt(ts) or datetime.now(timezone.utc)
    trends = c.execute("SELECT id, keyword, acceleration FROM trends").fetchall()
    for trend in trends:
        key = normalize_match_key(trend["keyword"])
        rows = c.execute("""
            SELECT source, source_score, raw_metric, captured_at
            FROM v9_signal_history
            WHERE match_key=?
            ORDER BY captured_at ASC
        """, (key,)).fetchall()
        if not rows:
            continue

        v30 = _v11_window_signal(rows, 30, now_dt)
        v60 = _v11_window_signal(rows, 60, now_dt)
        v180 = _v11_window_signal(rows, 180, now_dt)

        first_by_source = {}
        for r in rows:
            source = r["source"]
            if source not in first_by_source:
                first_by_source[source] = r["captured_at"]
        ordered = sorted(first_by_source.items(), key=lambda x: x[1])
        first_source = ordered[0][0] if ordered else ""
        first_seen_at = ordered[0][1] if ordered else ""
        source_sequence = " → ".join(source for source, _ in ordered)

        # Propagation bonus: independent providers seeing the same normalized keyword.
        propagation_bonus = min(18.0, max(0, len(ordered) - 1) * 9.0)
        velocity_score = round(max(0.0, min(100.0,
            50.0 + v30 * 0.65 + v60 * 0.25 + v180 * 0.10 + propagation_bonus
        )), 1)

        if velocity_score >= 78 or v30 >= 35:
            label = "急加速"
        elif velocity_score >= 66 or v30 >= 20:
            label = "加速中"
        elif velocity_score >= 56 or v30 >= 7 or propagation_bonus > 0:
            label = "上昇中"
        elif velocity_score <= 38 or v30 <= -15:
            label = "減速中"
        else:
            label = "観測中"

        c.execute("""
            INSERT INTO v9_velocity_state(
                trend_id, velocity_30m, velocity_1h, velocity_3h,
                velocity_score, velocity_label, first_source,
                first_seen_at, source_sequence, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(trend_id) DO UPDATE SET
                velocity_30m=excluded.velocity_30m,
                velocity_1h=excluded.velocity_1h,
                velocity_3h=excluded.velocity_3h,
                velocity_score=excluded.velocity_score,
                velocity_label=excluded.velocity_label,
                first_source=excluded.first_source,
                first_seen_at=excluded.first_seen_at,
                source_sequence=excluded.source_sequence,
                updated_at=excluded.updated_at
        """, (trend["id"], v30, v60, v180, velocity_score, label,
              first_source, first_seen_at, source_sequence, ts))

        if label in ("急加速", "加速中"):
            new_acc = min(1.0, max(float(trend["acceleration"] or 0), velocity_score / 100.0))
            c.execute("UPDATE trends SET acceleration=?, updated_at=? WHERE id=?",
                      (round(new_acc, 3), ts, trend["id"]))


def snapshot_v9_sources(c, ts):
    """Copy the latest source signals into an append-only V9 history table."""
    latest = c.execute("""
        SELECT source, keyword, source_score, raw_metric
        FROM source_items
        WHERE collected_at=?
    """, (ts,)).fetchall()

    # Avoid duplicate rows for the same source/keyword within the same minute.
    minute_key = str(ts)[:16]
    for r in latest:
        key = normalize_match_key(r["keyword"])
        exists = c.execute("""
            SELECT 1 FROM v9_signal_history
            WHERE source=? AND match_key=? AND substr(captured_at,1,16)=?
            LIMIT 1
        """, (r["source"], key, minute_key)).fetchone()
        if not exists:
            c.execute("""
                INSERT INTO v9_signal_history(
                    source, keyword, match_key, source_score, raw_metric, captured_at
                ) VALUES(?,?,?,?,?,?)
            """, (
                r["source"], r["keyword"], key,
                float(r["source_score"] or 0),
                float(r["raw_metric"] or 0),
                ts
            ))

    # Keep 90 days only.
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    c.execute("DELETE FROM v9_signal_history WHERE captured_at < ?", (cutoff,))




def refresh_real_traffic_forecast(c, ts):
    """V13: REAL SIGNAL based Traffic Potential + predicted daily PV.

    This is not measured site traffic. It is a deterministic forecast derived from
    the signals BUZZ NOW actually collects (trend scores, REAL VELOCITY,
    confidence and source spread). No random numbers are used.
    """
    rows = c.execute("""
        SELECT
            t.id,
            COALESCE(t.pre_buzz_score,0) AS pre_buzz_score,
            COALESCE(t.buzz_score,0) AS buzz_score,
            COALESCE(t.acceleration,0) AS acceleration,
            COALESCE(v.velocity_score,0) AS velocity_score,
            COALESCE(v.velocity_30m,0) AS velocity_30m,
            COALESCE(v.velocity_1h,0) AS velocity_1h,
            COALESCE(v.velocity_3h,0) AS velocity_3h,
            COALESCE(cs.source_count,0) AS source_count,
            COALESCE(cs.confidence_score,0) AS confidence_score,
            COALESCE(cs.corroborated,0) AS corroborated
        FROM trends t
        LEFT JOIN v9_velocity_state v ON v.trend_id=t.id
        LEFT JOIN confidence_state cs ON cs.trend_id=t.id
    """).fetchall()

    for r in rows:
        pre = max(0.0, min(100.0, float(r["pre_buzz_score"] or 0)))
        buzz = max(0.0, min(100.0, float(r["buzz_score"] or 0)))
        vel = max(0.0, min(100.0, float(r["velocity_score"] or 0)))
        conf = max(0.0, min(100.0, float(r["confidence_score"] or 0)))
        acc = max(-1.0, min(1.0, float(r["acceleration"] or 0)))
        v30 = float(r["velocity_30m"] or 0)
        v60 = float(r["velocity_1h"] or 0)
        v180 = float(r["velocity_3h"] or 0)
        sources = max(0, int(r["source_count"] or 0))
        corroborated = 1 if r["corroborated"] else 0

        # Search-demand potential from real collected signals. 50 on velocity is
        # the neutral observation baseline, so only movement above/below it adds
        # or subtracts meaningfully.
        velocity_component = max(0.0, min(100.0, 50.0 +
            v30 * 0.65 + v60 * 0.25 + v180 * 0.10))
        source_bonus = min(12.0, sources * 4.0) + (6.0 if corroborated else 0.0)
        acceleration_component = max(0.0, min(100.0, 50.0 + acc * 70.0))

        potential = (
            pre * 0.28 +
            buzz * 0.18 +
            vel * 0.24 +
            velocity_component * 0.12 +
            conf * 0.10 +
            acceleration_component * 0.08 +
            source_bonus
        )
        potential = round(max(0.0, min(100.0, potential)), 1)

        # Predicted daily exposure/PV. This is intentionally deterministic and
        # only represents opportunity, not measured Google Analytics traffic.
        movement = max(0.35, min(2.20, 1.0 + v30 / 70.0 + v60 / 180.0))
        confidence_factor = 0.55 + (conf / 100.0) * 0.45
        source_factor = 1.0 + min(0.35, max(0, sources - 1) * 0.12)
        predicted_pv = int(max(0, round((potential ** 2) * 0.72 * movement * confidence_factor * source_factor)))

        # Compatibility fields: impressions/clicks are also forecasts here.
        predicted_impressions = int(round(predicted_pv * 9.0))
        predicted_ctr = round(max(0.8, min(12.0, 2.2 + potential / 24.0)), 2)
        predicted_clicks = int(round(predicted_impressions * predicted_ctr / 100.0))

        c.execute("""
            INSERT INTO traffic_history(
                trend_id,impressions,clicks,pageviews,ctr,traffic_potential,recorded_at
            ) VALUES(?,?,?,?,?,?,?)
        """, (r["id"], predicted_impressions, predicted_clicks,
              predicted_pv, predicted_ctr, potential, ts))

        c.execute("""
            INSERT INTO traffic_totals(
                trend_id,impressions,clicks,pageviews,last_ctr,traffic_potential,updated_at
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(trend_id) DO UPDATE SET
                impressions=excluded.impressions,
                clicks=excluded.clicks,
                pageviews=excluded.pageviews,
                last_ctr=excluded.last_ctr,
                traffic_potential=excluded.traffic_potential,
                updated_at=excluded.updated_at
        """, (r["id"], predicted_impressions, predicted_clicks,
              predicted_pv, predicted_ctr, potential, ts))

def _parse_iso_datetime(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _social_detail_url(slug: str) -> str:
    return f"{SITE_URL}/trend/{quote(str(slug), safe='-_%')}"


def _social_short_url(trend_id: int) -> str:
    return f"{SITE_URL}/t/{int(trend_id)}"


def _social_image_url(trend_id: int) -> str:
    return f"{SITE_URL}/social-image/{int(trend_id)}.jpg"


def _build_ai_visual_prompt(row) -> str:
    keyword = str(row["keyword"] or "").strip()
    why_now = str(row["why_now"] or "").strip()
    category = str(row["category"] or "総合").strip() if "category" in row.keys() else "総合"
    context = why_now[:500] if why_now else "This topic is showing a rapid rise in search and viewing signals."

    return f"""
Create one compelling horizontal editorial news photograph/visual for a Japanese trend-detection social post.

Trending topic: {keyword}
Category: {category}
Context: {context}

Important safety and rights rules:
- Do NOT depict, imitate, or recreate the recognizable face or likeness of any real person, celebrity, politician, athlete, creator, or private individual.
- If a real person is central to the topic, represent the surrounding event or context instead: anonymous silhouettes, back-of-head figures, hands, studio equipment, venue, city scene, symbolic objects, documents, screens without copyrighted content, or other non-identifying visual cues.
- Do NOT copy a real press photograph, entertainment still, social-media screenshot, website screenshot, logo, trademark, poster, or copyrighted artwork.
- No readable names, captions, headlines, watermarks, logos, UI, or text inside the image.
- Do not imply factual details that are not supplied in the context.

Visual direction:
- photorealistic editorial photography
- contemporary Japanese news / culture atmosphere where relevant
- strong single focal point
- dramatic but credible lighting
- clean composition suitable for X
- landscape 3:2 composition
- no text
""".strip()


def _generate_ai_social_image(row) -> tuple[bytes, str, str]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    prompt = _build_ai_visual_prompt(row)
    payload = {
        "model": OPENAI_IMAGE_MODEL,
        "prompt": prompt,
        "size": "1536x1024",
        "quality": SOCIAL_AI_IMAGE_QUALITY,
        "n": 1,
    }

    response = httpx.post(
        "https://api.openai.com/v1/images/generations",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "BUZZ-NOW/30.5",
        },
        json=payload,
        timeout=180,
        follow_redirects=True,
    )
    response.raise_for_status()
    data = response.json()
    items = data.get("data") or []
    if not items:
        raise RuntimeError("OpenAI image response contained no image")

    item = items[0]
    if item.get("b64_json"):
        return base64.b64decode(item["b64_json"]), "image/png", prompt

    # Compatibility fallback in case an API response supplies a temporary URL.
    if item.get("url"):
        img_res = httpx.get(item["url"], timeout=120, follow_redirects=True)
        img_res.raise_for_status()
        mime = (img_res.headers.get("content-type") or "image/png").split(";")[0]
        return img_res.content, mime, prompt

    raise RuntimeError("OpenAI image response had neither b64_json nor url")


def _ensure_social_safe_card(c, row, ts: str):
    """Use a BUZZ NOW-owned data card instead of an AI context image when confidence is low."""
    existing = c.execute(
        "SELECT trend_id,mime_type,created_at FROM social_images WHERE trend_id=?",
        (row["id"],),
    ).fetchone()
    if existing:
        return {
            "ok": True,
            "cached": True,
            "image_url": _social_image_url(row["id"]),
            "mode": "safe_data_card",
        }

    image_bytes = _build_social_card_png(row)
    image_b64 = base64.b64encode(image_bytes).decode("ascii")

    c.execute("""
        INSERT INTO social_images(
            trend_id,image_b64,mime_type,model,prompt,created_at
        ) VALUES(?,?,?,?,?,?)
        ON CONFLICT(trend_id) DO UPDATE SET
            image_b64=excluded.image_b64,
            mime_type=excluded.mime_type,
            model=excluded.model,
            prompt=excluded.prompt,
            created_at=excluded.created_at
    """, (
        row["id"],
        image_b64,
        "image/png",
        "buzz-now-safe-data-card",
        "Low-confidence trend signal card. No factual event depiction.",
        ts,
    ))

    return {
        "ok": True,
        "cached": False,
        "image_url": _social_image_url(row["id"]),
        "mode": "safe_data_card",
    }


def _ensure_social_ai_image(c, row, ts: str):
    """Generate once and persist in PostgreSQL so Buffer can fetch a stable HTTPS URL."""
    existing = c.execute(
        "SELECT trend_id,mime_type,created_at FROM social_images WHERE trend_id=?",
        (row["id"],),
    ).fetchone()
    if existing:
        return {
            "ok": True,
            "cached": True,
            "image_url": _social_image_url(row["id"]),
        }

    if not SOCIAL_AI_IMAGE_ENABLED:
        return {"ok": False, "reason": "SOCIAL_AI_IMAGE_ENABLED=false"}
    if not OPENAI_API_KEY:
        return {"ok": False, "reason": "OPENAI_API_KEY missing"}

    image_bytes, mime_type, prompt = _generate_ai_social_image(row)
    image_b64 = base64.b64encode(image_bytes).decode("ascii")

    c.execute("""
        INSERT INTO social_images(
            trend_id,image_b64,mime_type,model,prompt,created_at
        ) VALUES(?,?,?,?,?,?)
        ON CONFLICT(trend_id) DO UPDATE SET
            image_b64=excluded.image_b64,
            mime_type=excluded.mime_type,
            model=excluded.model,
            prompt=excluded.prompt,
            created_at=excluded.created_at
    """, (
        row["id"], image_b64, mime_type, OPENAI_IMAGE_MODEL, prompt, ts
    ))

    return {
        "ok": True,
        "cached": False,
        "image_url": _social_image_url(row["id"]),
    }


def _social_font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc" if bold else "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc" if bold else "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansJP-Bold.ttf" if bold else "/usr/share/fonts/opentype/noto/NotoSansJP-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            if os.path.exists(path):
                return ImageFont.truetype(path, size=size)
        except Exception:
            pass
    return ImageFont.load_default()


def _fit_text(draw, value: str, font, max_width: int, max_chars: int = 26) -> str:
    value = str(value or "").strip().replace("\\n", " ")
    if len(value) > max_chars:
        value = value[:max_chars - 1] + "…"
    while value:
        try:
            box = draw.textbbox((0, 0), value, font=font)
            if box[2] - box[0] <= max_width:
                return value
        except Exception:
            return value
        value = value[:-2] + "…" if len(value) > 2 else value[:-1]
    return ""


def _build_social_card_png(row) -> bytes:
    """Generate a BUZZ NOW-owned 1200x675 social card.

    This intentionally uses BUZZ NOW's own trend data instead of copying
    third-party celebrity/news photos.
    """
    width, height = 1200, 675
    img = Image.new("RGB", (width, height), (10, 13, 20))
    draw = ImageDraw.Draw(img)

    # simple dashboard-style panels
    draw.rounded_rectangle((46, 42, 1154, 633), radius=34, fill=(18, 23, 34), outline=(67, 77, 96), width=2)
    draw.rounded_rectangle((82, 88, 1118, 186), radius=24, fill=(27, 34, 49))
    draw.rounded_rectangle((82, 414, 430, 572), radius=24, fill=(25, 31, 45))
    draw.rounded_rectangle((447, 414, 795, 572), radius=24, fill=(25, 31, 45))
    draw.rounded_rectangle((812, 414, 1118, 572), radius=24, fill=(25, 31, 45))

    title_font = _social_font(44, True)
    keyword_font = _social_font(68, True)
    status_font = _social_font(31, True)
    label_font = _social_font(25, False)
    score_font = _social_font(54, True)
    small_font = _social_font(22, False)

    keyword = _fit_text(draw, row["keyword"], keyword_font, 960, 24)
    status = re.sub(r"^[^ぁ-んァ-ヶ一-龠A-Za-z0-9]+\\s*", "", str(row["status"] or "急上昇")).strip() or "急上昇"
    pre = int(round(float(row["pre_buzz_score"] or 0)))
    traffic = int(round(float(row["traffic_potential"] or 0)))
    confidence = int(round(float(row["confidence_score"] or 0)))

    draw.text((86, 108), "BUZZ NOW  /  SNS SIGNAL", font=title_font, fill=(245, 247, 250))
    draw.text((86, 222), keyword, font=keyword_font, fill=(255, 255, 255))
    draw.text((88, 326), f"SIGNAL: {status}", font=status_font, fill=(214, 220, 230))

    draw.text((112, 438), "PRE-BUZZ", font=label_font, fill=(160, 169, 184))
    draw.text((112, 484), str(pre), font=score_font, fill=(255, 255, 255))

    draw.text((477, 438), "TRAFFIC", font=label_font, fill=(160, 169, 184))
    draw.text((477, 484), str(traffic), font=score_font, fill=(255, 255, 255))

    draw.text((842, 438), "CONFIDENCE", font=label_font, fill=(160, 169, 184))
    draw.text((842, 484), str(confidence), font=score_font, fill=(255, 255, 255))

    draw.text((86, 594), "buzz-now.onrender.com", font=small_font, fill=(125, 135, 150))

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def _social_reason_from_row(row, keyword: str) -> str:
    """Create one short, cautious X reason line from an actually collected article title."""
    try:
        keys = set(row.keys())
    except Exception:
        keys = set()

    title = str(row["reason_title"] or "").strip() if "reason_title" in keys else ""
    if not title:
        return "関連報道の増加が要因か。"

    # Basic cleanup.
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"\s*[|｜]\s*[^|｜]{1,40}$", "", title).strip()
    title = re.sub(r"\s*[-–—]\s*[^-–—]{1,35}$", "", title).strip()
    title = re.sub(r"(?:\.\.\.|…)+\s*$", "", title).strip()

    # Prefer the leading news hook before an ellipsis; titles are often feed-truncated.
    hook = re.split(r"(?:\.\.\.|…)", title, maxsplit=1)[0].strip()
    if len(hook) < 8:
        hook = title

    # Avoid repeating the buzzword itself in the reason line.
    hook = hook.replace(keyword, "").strip(" 　「」『』:：-–—|｜・")
    hook = re.sub(r"(?:\.\.\.|…)+\s*$", "", hook).strip()

    # Remove a dangling Japanese quote particle when feed truncation ends on it.
    hook = re.sub(r"(?:と|が|を|に|で|へ|は)$", "", hook).strip()

    if len(hook) > 34:
        hook = hook[:34].rstrip("、。・:：-–— ")
    if not hook:
        return "関連報道の増加が要因か。"

    # Make clear this is an inferred trigger from coverage, not an asserted fact.
    return f"「{hook}」などの関連記事が要因か。"

def _build_social_post_text(row) -> str:
    keyword = str(row["keyword"]).strip()
    pre = int(round(float(row["pre_buzz_score"] or 0)))
    traffic = int(round(float(row["traffic_potential"] or 0)))
    confidence = int(round(float(row["confidence_score"] or 0))) if "confidence_score" in row.keys() else 0
    first_source = str(row["first_source"] or "").strip() if "first_source" in row.keys() else ""
    detail_url = _social_short_url(row["id"])

    # Low-confidence ranking topics are allowed to post, but ONLY as signal reports.
    # Example: 「こめお 食中毒」 must not be turned into a factual allegation.
    if confidence < SOCIAL_MIN_CONFIDENCE:
        source_line = f"{first_source}で" if first_source else "公開データ上で"
        return (
            "🚨 BUZZNOW SNS捜査官｜検索シグナル急上昇\n"
            f"いま「{keyword}」という検索ワードが上昇中。\n"
            f"{source_line}動きを検知。現時点では関連情報を確認中です。\n"
            f"Pre-Buzz：{pre} / Traffic：{traffic} / Confidence：{confidence}\n"
            f"追跡ページ → {detail_url}"
        )

    status = str(row["status"] or "急上昇")
    status_plain = re.sub(r"^[^ぁ-んァ-ヶ一-龠A-Za-z0-9]+\s*", "", status).strip() or "急上昇"
    reason = _social_reason_from_row(row, keyword)
    return (
        f"🚨 BUZZNOW SNS捜査官｜{status_plain}を検知\n"
        f"いま「{keyword}」がバズり中。\n"
        f"{reason}\n"
        f"シグナル上昇 / Pre-Buzz：{pre} / Traffic：{traffic}\n"
        f"なぜ今話題？ → {detail_url}"
    )


def _visible_top_rows(c, limit: int = 10):
    """Return the exact same ordering used by /api/traffic-ranking and the visible TOP list."""
    limit = max(1, min(int(limit), 50))
    return c.execute("""
        SELECT
            t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.buzz_score,t.acceleration,
            t.status,t.why_now,t.updated_at,
            COALESCE(tt.impressions,0) AS impressions,
            COALESCE(tt.clicks,0) AS clicks,
            COALESCE(tt.pageviews,0) AS pageviews,
            COALESCE(tt.traffic_potential,0) AS traffic_potential,
            COALESCE(cf.confidence_score,0) AS confidence_score,
            COALESCE(cf.source_count,0) AS source_count,
            COALESCE(cf.confidence_label,'デモ/未確認') AS confidence_label,
            COALESCE(ps.first_source,'') AS first_source,
            (
                SELECT s.title
                FROM sources s
                WHERE s.trend_id=t.id
                  AND COALESCE(TRIM(s.title),'')<>''
                ORDER BY
                  CASE WHEN COALESCE(TRIM(s.published_at),'')='' THEN 1 ELSE 0 END,
                  s.published_at DESC,
                  s.id DESC
                LIMIT 1
            ) AS reason_title
        FROM trends t
        LEFT JOIN traffic_totals tt ON tt.trend_id=t.id
        LEFT JOIN confidence_state cf ON cf.trend_id=t.id
        LEFT JOIN propagation_state ps ON ps.trend_id=t.id
        ORDER BY
          COALESCE(tt.traffic_potential,0) DESC,
          COALESCE(cf.confidence_score,0) DESC,
          COALESCE(tt.pageviews,0) DESC
        LIMIT ?
    """, (limit,)).fetchall()


def _social_candidate_rows(c, limit: int = 10):
    """Choose normal X candidates ONLY from the current visible TOP10.

    Critical V35.27 fix:
    V35.26 applied eligibility filters BEFORE LIMIT, so when some visible TOP10
    rows were ineligible, lower-ranked topics could slide upward into the social
    candidate list. That is how a topic outside the user's visible TOP10 could post.

    Now:
      1) freeze the exact visible TOP10 first
      2) apply posting eligibility inside that frozen TOP10
      3) never reach rank 11+
    """
    visible = _visible_top_rows(c, limit=min(max(1, int(limit)), 10))
    candidates = []

    for row in visible:
        pre_ok = float(row["pre_buzz_score"] or 0) >= SOCIAL_MIN_PREBUZZ
        traffic_ok = float(row["traffic_potential"] or 0) >= SOCIAL_MIN_TRAFFIC
        status_ok = "下降" not in str(row["status"] or "")

        if pre_ok and traffic_ok and status_ok:
            candidates.append(row)

    return candidates


def _social_post_allowed(c, row, now_dt):
    # Daily safety cap.
    daily_cutoff = (now_dt - timedelta(hours=24)).isoformat()
    daily_count = c.execute(
        "SELECT COUNT(*) AS n FROM social_posts WHERE make_status=1 AND posted_at>=?",
        (daily_cutoff,),
    ).fetchone()["n"]
    if int(daily_count or 0) >= max(0, SOCIAL_DAILY_CAP):
        return False, "daily_cap"

    # Global cooldown so one collector run cannot turn into a noisy posting burst.
    last_any = c.execute(
        "SELECT posted_at FROM social_posts WHERE make_status=1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if last_any:
        last_dt = _parse_iso_datetime(last_any["posted_at"])
        if last_dt and now_dt - last_dt < timedelta(minutes=max(0, SOCIAL_GLOBAL_COOLDOWN_MINUTES)):
            return False, "global_cooldown"

    # Keyword cooldown prevents repeating the same topic over and over.
    last_same = c.execute(
        "SELECT posted_at FROM social_posts WHERE make_status=1 AND trend_id=? ORDER BY id DESC LIMIT 1",
        (row["id"],),
    ).fetchone()
    if last_same:
        last_dt = _parse_iso_datetime(last_same["posted_at"])
        if last_dt and now_dt - last_dt < timedelta(hours=max(0, SOCIAL_KEYWORD_COOLDOWN_HOURS)):
            return False, "keyword_cooldown"

    return True, "ok"


def auto_post_social(c, ts: str):
    """V30 production social dispatcher.

    Uses only BUZZ NOW's own scored trend state. It sends qualifying topics to the
    existing Make webhook, where Buffer publishes to X. Deduplication, cooldowns
    and a daily cap live in PostgreSQL so restarts do not reset posting history.
    """
    result = {
        "enabled": SOCIAL_AUTO_ENABLED,
        "sent": 0,
        "skipped": [],
        "errors": [],
    }
    if not SOCIAL_AUTO_ENABLED:
        result["reason"] = "SOCIAL_AUTO_ENABLED=false"
        return result
    if not BUFFER_API_KEY or not BUFFER_CHANNEL_ID:
        result["reason"] = "buffer_not_configured"
        return result

    now_dt = _parse_iso_datetime(ts) or datetime.now(timezone.utc)
    candidates = _social_candidate_rows(c, limit=10)

    for row in candidates:
        if result["sent"] >= max(0, SOCIAL_MAX_POSTS_PER_RUN):
            break

        allowed, reason = _social_post_allowed(c, row, now_dt)
        if not allowed:
            result["skipped"].append({"keyword": row["keyword"], "reason": reason})
            continue

        post_text = _build_social_post_text(row)

        # Generate a contextual visual only for a topic that is actually about to post.
        # If image generation is disabled or fails, posting safely falls back to text+link.
        image_result = {"ok": False, "reason": "not_attempted"}
        try:
            confidence = float(row["confidence_score"] or 0)
            if confidence < SOCIAL_MIN_CONFIDENCE:
                image_result = _ensure_social_safe_card(c, row, ts)
            else:
                image_result = _ensure_social_ai_image(c, row, ts)
        except Exception as image_exc:
            logger.exception("V35.26 social image failed for %s", row["keyword"])
            image_result = {"ok": False, "reason": str(image_exc)[:300]}

        payload = {
            "keyword": row["keyword"],
            "pre_buzz_score": round(float(row["pre_buzz_score"] or 0), 1),
            "traffic_potential": round(float(row["traffic_potential"] or 0), 1),
            "status": row["status"],
            "why_now": row["why_now"] or "",
            "detail_url": _social_short_url(row["id"]),
            "image_url": image_result.get("image_url", "") if image_result.get("ok") else "",
            "image_ready": bool(image_result.get("ok")),
            "post_text": post_text,
            "source": "buzz-now-v33-buffer-direct-auto",
            "sent_at": ts,
        }

        try:
            # V33 production route:
            # BUZZ NOW -> cached AI JPEG -> Buffer official API -> X.
            # Make's Buffer module is intentionally not used.
            image_url = payload.get("image_url", "") if payload.get("image_ready") else ""
            if image_url:
                _prewarm_social_jpeg(row["id"])

            buffer_result = _send_to_buffer_direct(
                post_text,
                image_url,
                "shareNow",
            )
            ok = bool(buffer_result.get("ok"))

            c.execute("""
                INSERT INTO social_posts(
                    trend_id,keyword,pre_buzz_score,traffic_potential,
                    post_text,make_status,posted_at
                ) VALUES(?,?,?,?,?,?,?)
            """, (
                row["id"], row["keyword"], payload["pre_buzz_score"],
                payload["traffic_potential"], post_text, 1 if ok else 0, ts
            ))

            if ok:
                result["sent"] += 1
                result["last_keyword"] = row["keyword"]
                result["last_post_id"] = buffer_result.get("post_id")
                logger.info("V33 Buffer direct auto-post sent: %s", row["keyword"])
            else:
                result["errors"].append({
                    "keyword": row["keyword"],
                    "error": buffer_result.get("reason", "Buffer returned not-ok"),
                })
        except Exception as exc:
            logger.exception("V33 Buffer direct auto-post failed for %s", row["keyword"])
            result["errors"].append({"keyword": row["keyword"], "error": str(exc)[:300]})

    return result



def _ensure_yahoo_buzz_tables(c):
    c.execute("""
      CREATE TABLE IF NOT EXISTS yahoo_buzz_candidates(
        id BIGSERIAL PRIMARY KEY,
        keyword TEXT NOT NULL,
        rank INTEGER NOT NULL,
        source_url TEXT NOT NULL,
        detected_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        UNIQUE(keyword, rank)
      )
    """)


def _strip_html_text(value: str) -> str:
    from html import unescape
    value = re.sub(r"<script\b[^>]*>.*?</script>", " ", value, flags=re.I | re.S)
    value = re.sub(r"<style\b[^>]*>.*?</style>", " ", value, flags=re.I | re.S)
    value = re.sub(r"<[^>]+>", " ", value)
    value = unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _parse_yahoo_realtime_ranks(html_text: str):
    """Parse rank/keyword pairs from Yahoo!リアルタイム検索 public HTML.
    No private endpoint/API is used.
    """
    pairs = []
    seen = set()

    # Anchor text on the public page commonly renders as "1キーワード", "11 キーワード".
    for m in re.finditer(r"<a\b[^>]*>(.*?)</a>", html_text, flags=re.I | re.S):
        label = _strip_html_text(m.group(1))
        mm = re.match(r"^\s*(\d{1,3})\s*(.+?)\s*$", label)
        if not mm:
            continue
        rank = int(mm.group(1))
        keyword = mm.group(2).strip()
        if rank < 1 or rank > 100 or not keyword or len(keyword) > 80:
            continue
        key = (rank, keyword)
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"rank": rank, "keyword": keyword})

    pairs.sort(key=lambda x: x["rank"])
    return pairs


def collect_yahoo_realtime_buzz(c, ts: str):
    """Free Buzzing Now discovery from Yahoo!リアルタイム検索 public page.
    Stores candidates only. It never posts to X.
    """
    if not YAHOO_BUZZ_ENABLED:
        return {"ok": False, "reason": "YAHOO_BUZZ_ENABLED=false", "count": 0}

    url = "https://search.yahoo.co.jp/realtime"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BUZZ-NOW/1.0; +https://buzz-now.onrender.com)",
        "Accept-Language": "ja,en;q=0.8",
    }

    try:
        with httpx.Client(timeout=15.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()
            ranks = _parse_yahoo_realtime_ranks(r.text)

        if not ranks:
            return {"ok": False, "reason": "No ranks found in public HTML", "count": 0}

        # Ideal window is 30-80 as requested. Public HTML may expose fewer ranks.
        selected = [
            x for x in ranks
            if YAHOO_BUZZ_RANK_MIN <= x["rank"] <= YAHOO_BUZZ_RANK_MAX
        ]

        # Safe fallback: use the lowest-ranked portion that is actually visible publicly.
        # Example: if only 1-20 are exposed, use 11-20 rather than the already-saturated top 10.
        fallback_used = False
        if not selected:
            fallback_used = True
            selected = ranks[-YAHOO_BUZZ_FALLBACK_COUNT:]

        _ensure_yahoo_buzz_tables(c)
        c.execute("UPDATE yahoo_buzz_candidates SET active=0")

        stored = 0
        for item in selected:
            kw = item["keyword"]
            rank = int(item["rank"])
            c.execute("""
              INSERT INTO yahoo_buzz_candidates(
                keyword,rank,source_url,detected_at,last_seen_at,active
              ) VALUES(?,?,?,?,?,1)
              ON CONFLICT(keyword,rank) DO UPDATE SET
                source_url=excluded.source_url,
                last_seen_at=excluded.last_seen_at,
                active=1
            """, (kw, rank, url, ts, ts))
            stored += 1

        return {
            "ok": True,
            "count": stored,
            "public_rank_count": len(ranks),
            "min_public_rank": min(x["rank"] for x in ranks),
            "max_public_rank": max(x["rank"] for x in ranks),
            "requested_window": [YAHOO_BUZZ_RANK_MIN, YAHOO_BUZZ_RANK_MAX],
            "fallback_used": fallback_used,
            "selected": selected,
        }

    except Exception as exc:
        logger.exception("Yahoo realtime Buzzing Now collection failed")
        return {"ok": False, "reason": str(exc)[:220], "count": 0}



def _yahoo_rank_signal_score(rank: int) -> float:
    """Convert a Yahoo realtime rank into a BUZZ NOW signal score.
    Lower rank number = stronger signal. Keeps 30-80 useful without pretending
    Yahoo rank is a literal search-volume metric.
    """
    rank = max(1, min(int(rank), 100))
    # rank 1 -> 98, rank 20 -> ~88, rank 50 -> ~72, rank 80 -> ~57
    return round(max(52.0, min(98.0, 98.0 - (rank - 1) * 0.52)), 1)



def _buzzing_now_quality(c, trend_id: int):
    """Strict gate for Buzzing Now article/X use.
    A current social trend needs at least one independently collected article
    with a parseable publication time within 7 days.
    """
    rows = c.execute("""
      SELECT publisher,title,url,published_at,source_label
      FROM sources
      WHERE trend_id=?
      ORDER BY id DESC
      LIMIT 20
    """, (trend_id,)).fetchall()

    now_dt = datetime.now(timezone.utc)
    fresh_72h = []
    fresh_7d = []
    publishers = set()

    for r in rows:
        published = _parse_news_datetime(r["published_at"])
        if not published:
            continue
        age_h = max(0.0, (now_dt - published).total_seconds() / 3600.0)
        if age_h <= 168:
            fresh_7d.append(dict(r))
            publishers.add(str(r["publisher"] or "").strip())
        if age_h <= 72:
            fresh_72h.append(dict(r))

    qualified = len(fresh_7d) >= 1
    if not qualified:
        reason = "fresh_independent_source_missing"
    elif fresh_72h:
        reason = "fresh_72h_source_confirmed"
    else:
        reason = "fresh_7d_source_confirmed"

    return {
        "qualified": qualified,
        "reason": reason,
        "fresh_72h_count": len(fresh_72h),
        "fresh_7d_count": len(fresh_7d),
        "publisher_count": len([p for p in publishers if p]),
        "example_titles": [x["title"] for x in fresh_7d[:3]],
    }


def promote_yahoo_buzz_candidates(limit: int = None):
    """Turn Yahoo candidates into BUZZ NOW pages only when current independent
    reporting supports the topic. Social-only phrases stay as candidates for the
    later quote-post/original-post pipeline.
    """
    limit = YAHOO_BUZZ_PROMOTE_LIMIT if limit is None else max(1, min(int(limit), 10))
    ts = now_iso()

    promoted = []
    held = []
    errors = []

    with db() as c:
        _ensure_yahoo_buzz_tables(c)
        rows = c.execute("""
          SELECT keyword,rank,source_url,last_seen_at
          FROM yahoo_buzz_candidates
          WHERE active=1
          ORDER BY rank ASC
          LIMIT ?
        """, (limit,)).fetchall()

        for row in rows:
            keyword = _clean_keyword(row["keyword"])
            rank = int(row["rank"] or 100)
            if not keyword:
                continue

            try:
                score = _yahoo_rank_signal_score(rank)

                upsert_real_trend(
                    c,
                    keyword,
                    "yahoo_realtime_buzz",
                    score,
                    float(rank),
                    str(row["source_url"] or "https://search.yahoo.co.jp/realtime"),
                    f"rank-{rank}-{normalize_match_key(keyword)[:80]}",
                    ts,
                )

                trend = _find_trend_row(c, keyword)
                if not trend:
                    held.append({"keyword": keyword, "rank": rank, "reason": "trend_not_created"})
                    continue

                # Force a current independent-source check for Buzzing Now.
                news_count = _enrich_keyword_news(c, keyword, ts, force=True, include_gdelt=False)
                trend = _find_trend_row(c, keyword)
                quality = _buzzing_now_quality(c, trend["id"])

                if not quality["qualified"]:
                    c.execute("""
                      UPDATE trends
                      SET status='⚡ ソーシャル急上昇',
                          summary=?,
                          updated_at=?
                      WHERE id=?
                    """, (
                        f"{keyword}はYahoo!リアルタイム検索で上昇中です。"
                        "現在は独立した最新報道による十分な裏取りが取れていないため、"
                        "BUZZ NOWでは「バズり中」記事・X引用投稿の対象外として監視を継続します。",
                        ts,
                        trend["id"],
                    ))
                    held.append({
                        "keyword": keyword,
                        "rank": rank,
                        "signal_score": score,
                        "reason": quality["reason"],
                        "fresh_72h_count": quality["fresh_72h_count"],
                        "fresh_7d_count": quality["fresh_7d_count"],
                    })
                    continue

                c.execute("""
                  UPDATE trends
                  SET status='🔥 バズり中',
                      summary=?,
                      updated_at=?
                  WHERE id=?
                """, (
                    f"{keyword}はYahoo!リアルタイム検索で上昇し、"
                    "独立した最新の公開情報も確認できた「バズり中」トピックです。"
                    "BUZZ NOWでは、なぜ今注目されているのかを整理します。",
                    ts,
                    trend["id"],
                ))

                trend2 = _find_trend_row(c, keyword)
                promoted.append({
                    "keyword": keyword,
                    "rank": rank,
                    "signal_score": score,
                    "slug": trend2["slug"],
                    "detail_url": f"{SITE_URL}/trend/{trend2['slug']}",
                    "news_sources_added": int(news_count or 0),
                    "quality": quality,
                    "why_now": trend2["why_now"] or "",
                })

            except Exception as exc:
                logger.exception("Yahoo Buzz promotion failed keyword=%s", keyword)
                errors.append({
                    "keyword": keyword,
                    "rank": rank,
                    "error": str(exc)[:220],
                })

        c.commit()

    return {
        "ok": True,
        "version": APP_VERSION,
        "posted_to_x": False,
        "promoted_count": len(promoted),
        "held_count": len(held),
        "promoted": promoted,
        "held": held,
        "errors": errors,
    }


@app.get("/api/buzzing-now/promote-preview")
def yahoo_buzz_promote_preview(limit: int = YAHOO_BUZZ_PROMOTE_LIMIT):
    """Manual safe test: creates/enriches Buzzing Now site pages, never posts to X."""
    return promote_yahoo_buzz_candidates(limit)


@app.get("/api/buzzing-now/articles")
def yahoo_buzz_articles(limit: int = 20):
    """Only return current Buzzing Now pages that pass the strict fresh-source gate."""
    limit = max(1, min(int(limit), 100))
    with db() as c:
        rows = c.execute("""
          SELECT
            y.keyword,
            y.rank,
            y.last_seen_at,
            t.id AS trend_id,
            t.slug,
            t.status,
            t.summary,
            t.why_now,
            t.pre_buzz_score,
            t.buzz_score,
            t.acceleration,
            t.category
          FROM yahoo_buzz_candidates y
          JOIN trends t ON t.keyword=y.keyword
          WHERE y.active=1
            AND t.status='🔥 バズり中'
          ORDER BY y.rank ASC
          LIMIT ?
        """, (limit,)).fetchall()

        items = []
        for r in rows:
            quality = _buzzing_now_quality(c, r["trend_id"])
            if not quality["qualified"]:
                continue
            item = dict(r)
            item.pop("trend_id", None)
            item["quality"] = quality
            item["detail_url"] = f"{SITE_URL}/trend/{item['slug']}"
            items.append(item)

    return {
        "ok": True,
        "version": APP_VERSION,
        "items": items,
    }



def _parse_compact_count(value: str) -> int:
    value = str(value or "").strip().replace(",", "")
    if not value:
        return 0
    multiplier = 1
    if value.endswith("万"):
        multiplier = 10000
        value = value[:-1]
    elif value.endswith("千"):
        multiplier = 1000
        value = value[:-1]
    try:
        return int(float(value) * multiplier)
    except Exception:
        return 0


def _metric_from_text(label: str, plain: str) -> int:
    patterns = [
        rf"{re.escape(label)}\s*([0-9][0-9,\.]*[万千]?)",
        rf"{re.escape(label)}[^0-9]{{0,12}}([0-9][0-9,\.]*[万千]?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, plain, flags=re.I)
        if m:
            return _parse_compact_count(m.group(1))
    return 0



def _extract_yahoo_card_metrics(raw_html: str, tweet_id: str):
    """Read Yahoo's public click-metadata for a specific tweet card.

    Yahoo embeds fields like:
      twid:<id>;reply:<n>;retweet:<n>;like:<n>;quote:<n>
    in data-cl-params. This is much more reliable than scraping visible metric
    labels because some search-result layouts omit those labels.
    """
    from html import unescape

    raw = unescape(raw_html or "")
    tweet_id = re.sub(r"\D", "", str(tweet_id or ""))
    if not tweet_id:
        return {"reply": 0, "retweet": 0, "like": 0, "quote": 0, "verified": False}

    best = {"reply": 0, "retweet": 0, "like": 0, "quote": 0, "verified": False}

    # Search only metadata fragments explicitly bound to this tweet id.
    for m in re.finditer(rf"twid:{re.escape(tweet_id)};", raw, flags=re.I):
        frag = raw[m.start(): min(len(raw), m.start() + 1200)]

        def grab(name):
            mm = re.search(rf"(?:^|;){name}:(\d+)", frag, flags=re.I)
            return int(mm.group(1)) if mm else 0

        current = {
            "reply": grab("reply"),
            "retweet": grab("retweet"),
            "like": grab("like"),
            "quote": grab("quote"),
            "verified": True,
        }

        # Keep the strongest rendering if the same tweet appears more than once.
        if (
            current["like"] + current["retweet"] * 2 + current["reply"] + current["quote"] * 2
            >
            best["like"] + best["retweet"] * 2 + best["reply"] + best["quote"] * 2
        ):
            best = current

    return best


def _extract_yahoo_tweet_candidates(html_text: str, keyword: str):
    """Find keyword-relevant X posts from Yahoo realtime public HTML.

    V35.19 reads Yahoo's own public tweet-card metadata (reply/retweet/like/quote)
    when present, so strong posts can be identified even when visible labels are
    not rendered in the HTML text.
    No X API is used.
    """
    from html import unescape

    raw = unescape(html_text or "")
    found = {}
    keyword_norm = normalize_match_key(keyword)

    patterns = [
        r'/realtime/search/tweet/([0-9]{8,25})',
        r'https?://(?:www\.)?(?:x\.com|twitter\.com)/[^/"\'<>\s]+/status/([0-9]{8,25})',
        r'https?%3A%2F%2F(?:www\.)?(?:x\.com|twitter\.com)%2F[^%/"\'<>\s]+%2Fstatus%2F([0-9]{8,25})',
    ]

    for pattern in patterns:
        for m in re.finditer(pattern, raw, flags=re.I):
            tweet_id = m.group(1)
            if tweet_id in found:
                continue

            left = max(0, m.start() - 2200)
            right = min(len(raw), m.end() + 3200)
            context_html = raw[left:right]

            first_gt = context_html.find(">")
            first_lt = context_html.find("<")
            if first_gt >= 0 and (first_lt < 0 or first_gt < first_lt):
                context_html = context_html[first_gt + 1:]

            plain = _strip_html_text(context_html)
            plain_norm = normalize_match_key(plain)

            if not keyword_norm or keyword_norm not in plain_norm:
                continue

            keyword_pos = plain.find(keyword)
            if keyword_pos < 0:
                keyword_pos = plain.casefold().find(str(keyword).casefold())
            if keyword_pos < 0:
                keyword_pos = min(len(plain) // 2, 500)

            s = max(0, keyword_pos - 240)
            e = min(len(plain), keyword_pos + len(keyword) + 650)
            focused = plain[s:e]

            # First choice: exact public metadata attached to this tweet card.
            meta = _extract_yahoo_card_metrics(context_html, tweet_id)

            likes = int(meta.get("like") or 0)
            reposts = int(meta.get("retweet") or 0)
            replies = int(meta.get("reply") or 0)
            quotes = int(meta.get("quote") or 0)

            # Fallback for layouts that only render human-visible metric labels.
            if not meta.get("verified"):
                likes = _metric_from_text("いいね数", focused)
                reposts = max(
                    _metric_from_text("リポスト数", focused),
                    _metric_from_text("リツイート数", focused),
                )
                replies = _metric_from_text("返信数", focused)
                quotes = _metric_from_text("引用数", focused)

                if likes == 0:
                    likes = _metric_from_text("いいね数", plain)
                if reposts == 0:
                    reposts = max(
                        _metric_from_text("リポスト数", plain),
                        _metric_from_text("リツイート数", plain),
                    )
                if replies == 0:
                    replies = _metric_from_text("返信数", plain)

            engagement_score = likes + reposts * 2 + replies + quotes * 2
            strong_enough = (
                likes >= YAHOO_QUOTE_MIN_LIKES
                or reposts >= YAHOO_QUOTE_MIN_REPOSTS
            )

            is_reply = bool(re.search(r"返信先\s*[:：]", focused[:320]))

            found[tweet_id] = {
                "tweet_id": tweet_id,
                "tweet_url": f"https://x.com/i/web/status/{tweet_id}",
                "likes": likes,
                "reposts": reposts,
                "replies": replies,
                "quotes": quotes,
                "engagement_score": engagement_score,
                "keyword_relevant": True,
                "metric_verified": bool(meta.get("verified")),
                "source_type": "yahoo_keyword_search",
                "strong_enough": strong_enough,
                "is_reply": is_reply,
                "snippet": focused[:850],
            }

    items = list(found.values())
    items.sort(
        key=lambda x: (
            bool(x.get("strong_enough")),
            bool(x.get("metric_verified")),
            x["engagement_score"],
            x["likes"],
            x["reposts"],
        ),
        reverse=True,
    )
    return items



def _extract_yahoo_popular_keyword_posts(html_text: str, keyword: str):
    """Extract keyword-relevant posts specifically from Yahoo's 「人気ポスト」 section.

    This is the source we actually want for quote-posting. It avoids selecting
    ordinary latest-search results with 0 likes just because they contain the keyword.
    No X API is used.
    """
    from html import unescape

    raw = unescape(html_text or "")
    keyword_norm = normalize_match_key(keyword)
    if not keyword_norm:
        return []

    section = ""
    # Pick a 人気ポスト occurrence that is followed by tweet detail links / metrics.
    for m in re.finditer("人気ポスト", raw):
        candidate = raw[m.start(): min(len(raw), m.start() + 180000)]
        if "/realtime/search/tweet/" in candidate and ("いいね数" in candidate or "リポスト数" in candidate):
            section = candidate
            break

    if not section:
        return []

    # Stop before unrelated site sections when possible.
    stop_positions = []
    for stop_word in ("電車遅延", "トレンド", "急上昇ワード"):
        p = section.find(stop_word, 20)
        if p > 0:
            stop_positions.append(p)
    if stop_positions:
        section = section[:min(stop_positions)]

    results = {}
    for m in re.finditer(r'/realtime/search/tweet/([0-9]{8,25})[^"\'<>\s]*', section, flags=re.I):
        tweet_id = m.group(1)
        if tweet_id in results:
            continue

        # Popular cards are compact; keep a local window to avoid neighboring cards.
        left = max(0, m.start() - 1600)
        right = min(len(section), m.end() + 2400)
        card_html = section[left:right]

        first_gt = card_html.find(">")
        first_lt = card_html.find("<")
        if first_gt >= 0 and (first_lt < 0 or first_gt < first_lt):
            card_html = card_html[first_gt + 1:]

        plain = _strip_html_text(card_html)
        plain_norm = normalize_match_key(plain)

        if keyword_norm not in plain_norm:
            continue

        keyword_pos = plain.find(keyword)
        if keyword_pos < 0:
            keyword_pos = min(len(plain) // 2, 500)

        s = max(0, keyword_pos - 260)
        e = min(len(plain), keyword_pos + len(keyword) + 720)
        focused = plain[s:e]

        replies = _metric_from_text("返信数", focused)
        reposts = max(
            _metric_from_text("リポスト数", focused),
            _metric_from_text("リツイート数", focused),
        )
        likes = _metric_from_text("いいね数", focused)

        # Fallback to local card if metric labels sit just outside focused text.
        if replies == 0:
            replies = _metric_from_text("返信数", plain)
        if reposts == 0:
            reposts = max(
                _metric_from_text("リポスト数", plain),
                _metric_from_text("リツイート数", plain),
            )
        if likes == 0:
            likes = _metric_from_text("いいね数", plain)

        engagement_score = likes + reposts * 2 + replies
        strong_enough = (
            likes >= YAHOO_QUOTE_MIN_LIKES
            or reposts >= YAHOO_QUOTE_MIN_REPOSTS
        )

        is_reply = bool(re.search(r"返信先\s*[:：]", focused[:320]))

        results[tweet_id] = {
            "tweet_id": tweet_id,
            "tweet_url": f"https://x.com/i/web/status/{tweet_id}",
            "likes": likes,
            "reposts": reposts,
            "replies": replies,
            "engagement_score": engagement_score,
            "keyword_relevant": True,
            "source_type": "yahoo_popular_post",
            "strong_enough": strong_enough,
            "is_reply": is_reply,
            "snippet": focused[:850],
        }

    items = list(results.values())
    items.sort(
        key=lambda x: (
            bool(x.get("strong_enough")),
            x["engagement_score"],
            x["likes"],
            x["reposts"],
        ),
        reverse=True,
    )
    return items



def _meta_content(raw_html: str, keys):
    """Extract content from matching meta tags regardless of attribute order."""
    from html import unescape
    raw = raw_html or ""
    wanted = {str(x).lower() for x in keys}

    for tag in re.findall(r"<meta\b[^>]*>", raw, flags=re.I):
        attrs = {}
        for m in re.finditer(
            r"([:\w-]+)\s*=\s*(?:\"([^\"]*)\"|'([^']*)')",
            tag,
            flags=re.I | re.S,
        ):
            attrs[m.group(1).lower()] = unescape(
                m.group(2) if m.group(2) is not None else m.group(3)
            )

        tag_key = (
            attrs.get("property")
            or attrs.get("name")
            or attrs.get("itemprop")
            or ""
        ).lower()

        if tag_key in wanted and attrs.get("content"):
            return re.sub(r"\s+", " ", attrs["content"]).strip()

    return ""


def _verify_yahoo_tweet_detail(candidate: dict, keyword: str):
    """Bind candidate ID to Yahoo's exact public tweet-detail page before quoting.

    Search-result HTML can place neighboring tweet text near the same tweet ID.
    This exact-page verification prevents that neighbor text from becoming a
    quote target. No X API is used.
    """
    tweet_id = re.sub(r"\D", "", str((candidate or {}).get("tweet_id") or ""))
    if not tweet_id:
        return {
            **(candidate or {}),
            "detail_verified": False,
            "detail_reason": "tweet_id_missing",
        }

    url = f"https://search.yahoo.co.jp/realtime/search/tweet/{tweet_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BUZZ-NOW/1.0; +https://buzz-now.onrender.com)",
        "Accept-Language": "ja,en;q=0.8",
    }

    try:
        with httpx.Client(timeout=15.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url)
            r.raise_for_status()

        raw = r.text
        plain = _strip_html_text(raw)
        keyword_norm = normalize_match_key(keyword)

        exact_text = _meta_content(
            raw,
            ("og:description", "twitter:description", "description"),
        )

        if not exact_text or keyword_norm not in normalize_match_key(exact_text):
            pos = plain.find(keyword)
            if pos >= 0:
                exact_text = plain[
                    max(0, pos - 320):
                    min(len(plain), pos + len(keyword) + 850)
                ]

        exact_text = re.sub(r"\s+", " ", exact_text or "").strip()
        exact_norm = normalize_match_key(exact_text)

        detail_keyword_relevant = bool(
            keyword_norm and exact_norm and keyword_norm in exact_norm
        )

        # IMPORTANT:
        # Do NOT scan the entire Yahoo detail-page HTML for "in_reply_to=".
        # The page can contain reply/intent links from surrounding UI or related
        # posts even when the exact tweet itself is not a reply.
        # Only the exact tweet text/metadata is trusted for reply classification.
        reply_match = re.search(r"返信先\s*[:：]\s*@?", exact_text[:220])
        detail_is_reply = bool(reply_match)
        reply_evidence = reply_match.group(0) if reply_match else ""

        meta = _extract_yahoo_card_metrics(raw, tweet_id)
        updated = dict(candidate or {})

        if meta.get("verified"):
            updated["likes"] = int(meta.get("like") or updated.get("likes") or 0)
            updated["reposts"] = int(meta.get("retweet") or updated.get("reposts") or 0)
            updated["replies"] = int(meta.get("reply") or updated.get("replies") or 0)
            updated["quotes"] = int(meta.get("quote") or updated.get("quotes") or 0)
            updated["engagement_score"] = (
                updated["likes"]
                + updated["reposts"] * 2
                + updated["replies"]
                + updated["quotes"] * 2
            )
            updated["metric_verified"] = True

        updated["detail_url"] = str(r.url)
        updated["detail_verified"] = bool(detail_keyword_relevant and exact_text)
        updated["detail_keyword_relevant"] = detail_keyword_relevant
        updated["detail_is_reply"] = detail_is_reply
        updated["is_reply"] = detail_is_reply
        updated["reply_evidence"] = reply_evidence
        updated["detail_snippet"] = exact_text[:1000]

        if exact_text:
            updated["snippet"] = exact_text[:850]

        updated["strong_enough"] = bool(
            int(updated.get("likes") or 0) >= YAHOO_QUOTE_MIN_LIKES
            or int(updated.get("reposts") or 0) >= YAHOO_QUOTE_MIN_REPOSTS
        )

        updated["detail_reason"] = (
            "exact_tweet_detail_verified"
            if updated["detail_verified"]
            else "exact_tweet_keyword_not_confirmed"
        )
        return updated

    except Exception as exc:
        logger.exception("Yahoo tweet-detail verification failed tweet_id=%s", tweet_id)
        updated = dict(candidate or {})
        updated.update({
            "detail_verified": False,
            "detail_keyword_relevant": False,
            "detail_is_reply": True,
            "is_reply": True,
            "detail_url": url,
            "detail_reason": str(exc)[:220],
        })
        return updated



def _fetch_yahoo_quote_candidates(keyword: str):
    """Fetch Yahoo realtime public search page, then verify strong candidates
    against their exact Yahoo tweet-detail page. No X API is used.
    """
    url = "https://search.yahoo.co.jp/realtime/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BUZZ-NOW/1.0; +https://buzz-now.onrender.com)",
        "Accept-Language": "ja,en;q=0.8",
    }

    try:
        with httpx.Client(timeout=15.0, follow_redirects=True, headers=headers) as client:
            r = client.get(url, params={"p": keyword, "rkf": "1"})
            r.raise_for_status()

        popular_items = _extract_yahoo_popular_keyword_posts(r.text, keyword)
        keyword_items = _extract_yahoo_tweet_candidates(r.text, keyword)

        merged = {}
        for item in popular_items + keyword_items:
            tid = item.get("tweet_id")
            if not tid:
                continue
            old = merged.get(tid)
            if old is None or int(item.get("engagement_score") or 0) > int(old.get("engagement_score") or 0):
                merged[tid] = item

        discovered = list(merged.values())
        discovered.sort(
            key=lambda x: (
                bool(x.get("strong_enough")),
                bool(x.get("metric_verified")),
                int(x.get("engagement_score") or 0),
            ),
            reverse=True,
        )

        verified = []
        strong_checked = 0

        for item in discovered:
            if item.get("strong_enough") and item.get("metric_verified") and strong_checked < 3:
                verified.append(_verify_yahoo_tweet_detail(item, keyword))
                strong_checked += 1
            else:
                weak = dict(item)
                weak.setdefault("detail_verified", False)
                weak.setdefault("detail_reason", "not_strong_enough_for_detail_check")
                verified.append(weak)

        verified.sort(
            key=lambda x: (
                bool(x.get("detail_verified")),
                bool(x.get("strong_enough")),
                not bool(x.get("detail_is_reply")),
                int(x.get("engagement_score") or 0),
            ),
            reverse=True,
        )

        verified_strong = [
            x for x in verified
            if x.get("detail_verified")
            and x.get("detail_keyword_relevant")
            and not x.get("detail_is_reply")
            and x.get("metric_verified")
            and x.get("strong_enough")
        ]

        source_mode = (
            "exact_tweet_detail_verified"
            if verified_strong
            else "no_exact_quote_target"
        )

        items = verified[:YAHOO_QUOTE_SCAN_LIMIT]

        return {
            "ok": True,
            "search_url": str(r.url),
            "source_mode": source_mode,
            "popular_count": len(popular_items),
            "verified_strong_count": len(verified_strong),
            "count": len(items),
            "items": items,
        }

    except Exception as exc:
        logger.exception("Yahoo quote-candidate fetch failed keyword=%s", keyword)
        return {
            "ok": False,
            "search_url": f"{url}?p={quote(keyword)}&rkf=1",
            "source_mode": "error",
            "popular_count": 0,
            "verified_strong_count": 0,
            "count": 0,
            "items": [],
            "reason": str(exc)[:220],
        }


def _compact_metric_ja(n: int) -> str:
    n = max(0, int(n or 0))
    if n >= 10000:
        v = n / 10000.0
        if v >= 10:
            return f"{v:.0f}万"
        return f"{v:.1f}万".replace(".0万", "万")
    if n >= 1000:
        return f"{n / 1000.0:.1f}千".replace(".0千", "千")
    return str(n)


def _sns_detective_comment(keyword: str, detail_url: str, target: dict | None):
    """SNS捜査官 3-pattern selector.
    冷静7：煽り3。元投稿との因果関係は断定しない。
    """
    target = target or {}
    likes = int(target.get("likes") or 0)
    reposts = int(target.get("reposts") or 0)

    if not target.get("tweet_id"):
        return {
            "template": "候補なし",
            "text": (
                "🕵️ SNS捜査メモ\n"
                f"「{keyword}」を追跡中。\n"
                "現時点では引用条件を満たす元投稿を確定できていません。\n"
                f"背景はこちら → {detail_url}"
            ),
            "likes": 0,
            "reposts": 0,
        }

    if likes >= 30000 or reposts >= 3000:
        template = "速報型"
        text = (
            "🚨 BUZZNOW SNS捜査官｜バズり中\n"
            f"これ、かなり伸びてる。「{keyword}」がリアルタイムで急上昇。\n"
            f"この投稿も{_compact_metric_ja(likes)}いいねまで拡散。関連する公開情報も追跡中。\n"
            f"なぜ今話題？ → {detail_url}"
        )
    elif likes >= 10000 or reposts >= 1000:
        template = "人間っぽい型"
        text = (
            "この投稿、かなり動いてる。👀\n"
            f"「{keyword}」がリアルタイムで上昇中。\n"
            f"この投稿も{_compact_metric_ja(likes)}いいねまで伸びてる。まだ動きそうなので追跡します。\n"
            f"背景はこちら → {detail_url}"
        )
    else:
        template = "捜査官型"
        text = (
            "🕵️ SNS捜査メモ\n"
            f"「{keyword}」を追跡中。この投稿も大きく反応を集めています。\n"
            "関連する公開情報とあわせて話題の経緯を整理しました。\n"
            f"🔎 {detail_url}"
        )

    if len(text) > 250:
        text = text[:247].rstrip() + "…"

    return {
        "template": template,
        "text": text,
        "likes": likes,
        "reposts": reposts,
    }


@app.get("/api/buzzing-now/quote-preview")
def yahoo_buzz_quote_preview(limit: int = 5):
    """SAFE preview only.
    - reads Yahoo public pages
    - finds possible original X post IDs
    - creates the SNS detective text preview
    - NEVER sends anything to Buffer or X
    """
    limit = max(1, min(int(limit), 10))
    previews = []

    with db() as c:
        rows = c.execute("""
          SELECT
            y.keyword,
            y.rank,
            t.id AS trend_id,
            t.slug,
            t.status,
            t.why_now
          FROM yahoo_buzz_candidates y
          JOIN trends t ON t.keyword=y.keyword
          WHERE y.active=1
            AND t.status='🔥 バズり中'
          ORDER BY y.rank ASC
          LIMIT ?
        """, (limit,)).fetchall()

        for row in rows:
            quality = _buzzing_now_quality(c, row["trend_id"])
            if not quality["qualified"]:
                continue

            keyword = row["keyword"]
            detail_url = f"{SITE_URL}/trend/{row['slug']}"
            source = _fetch_yahoo_quote_candidates(keyword)
            eligible = [
                x for x in (source.get("items") or [])
                if x.get("detail_verified")
                and x.get("detail_keyword_relevant")
                and x.get("metric_verified")
                and x.get("strong_enough")
                and not x.get("detail_is_reply")
            ]
            best = eligible[0] if eligible else None

            comment = _sns_detective_comment(keyword, detail_url, best)

            previews.append({
                "keyword": keyword,
                "rank": int(row["rank"]),
                "detail_url": detail_url,
                "quality": quality,
                "source_search": source,
                "best_quote_target": best,
                "comment_template": comment["template"],
                "comment_preview": comment["text"],
                "ready_for_quote_test": bool(best and best.get("tweet_id") and best.get("detail_verified") and best.get("strong_enough") and not best.get("detail_is_reply")),
            })

    return {
        "ok": True,
        "version": APP_VERSION,
        "posted_to_x": False,
        "x_api_used": False,
        "buffer_used": False,
        "items": previews,
    }


def _send_to_buffer_quote_post(tweet_id: str, comment: str, mode: str = "shareNow") -> dict:
    """Buffer supports X retweet metadata with an optional comment.
    Kept unused by automatic jobs in V35.16; a later manual test endpoint can
    call this once quote-target quality is confirmed.
    """
    if not BUFFER_API_KEY:
        return {"ok": False, "reason": "BUFFER_API_KEY is not configured"}
    if not BUFFER_CHANNEL_ID:
        return {"ok": False, "reason": "BUFFER_CHANNEL_ID is not configured"}
    tweet_id = re.sub(r"\D", "", str(tweet_id or ""))
    if not tweet_id:
        return {"ok": False, "reason": "tweet_id missing"}

    query = (
        "mutation CreateBuzzNowQuotePost { createPost(input: { "
        + "text: " + _graphql_string(comment) + " "
        + "channelId: " + _graphql_string(BUFFER_CHANNEL_ID) + " "
        + "schedulingType: automatic "
        + "mode: " + mode + " "
        + "metadata: { twitter: { retweet: { "
        + "id: " + _graphql_string(tweet_id) + " "
        + "comment: " + _graphql_string(comment) + " "
        + "} } } "
        + "}) { "
        + "... on PostActionSuccess { post { id text status } } "
        + "... on MutationError { message } "
        + "} }"
    )

    try:
        response = httpx.post(
            BUFFER_API_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {BUFFER_API_KEY}",
            },
            json={"query": query},
            timeout=45.0,
        )
        try:
            body = response.json()
        except Exception:
            body = {"raw": response.text[:1000]}

        if response.status_code != 200:
            return {
                "ok": False,
                "status_code": response.status_code,
                "reason": "Buffer HTTP error",
                "response": body,
            }
        if isinstance(body, dict) and body.get("errors"):
            return {
                "ok": False,
                "status_code": 200,
                "reason": "Buffer GraphQL error",
                "response": body,
            }

        result = ((body or {}).get("data") or {}).get("createPost")
        if not result:
            return {"ok": False, "reason": "Buffer returned no createPost result", "response": body}
        if result.get("message"):
            return {"ok": False, "reason": result["message"], "response": body}
        post = result.get("post")
        if post and post.get("id"):
            return {"ok": True, "post_id": post["id"], "post": post}
        return {"ok": False, "reason": "Buffer did not return a post id", "response": body}
    except Exception as exc:
        logger.exception("Direct Buffer quote post failed")
        return {"ok": False, "reason": str(exc)[:500]}


def _ensure_quote_post_log(c):
    c.execute("""
      CREATE TABLE IF NOT EXISTS buzzing_quote_posts(
        id BIGSERIAL PRIMARY KEY,
        keyword TEXT NOT NULL,
        tweet_id TEXT NOT NULL UNIQUE,
        tweet_url TEXT NOT NULL,
        comment_text TEXT NOT NULL,
        comment_template TEXT NOT NULL,
        buffer_post_id TEXT,
        status TEXT NOT NULL DEFAULT 'sent',
        sent_at TEXT NOT NULL
      )
    """)


@app.get("/api/buzzing-now/quote-test-one")
def yahoo_buzz_quote_test_one(keyword: str = "", confirm: str = ""):
    """MANUAL REAL POST TEST.

    Safety:
    - does nothing unless confirm=POST_ONE
    - posts only one qualified/strong quote target
    - blocks duplicate tweet_id
    - does not enable any scheduler/automatic quote-posting
    """
    if confirm != "POST_ONE":
        return {
            "ok": False,
            "version": APP_VERSION,
            "posted_to_x": False,
            "reason": "confirmation_required",
            "how_to_confirm": "Add ?keyword=<keyword>&confirm=POST_ONE",
        }

    keyword = _clean_keyword(keyword)
    if not keyword:
        return {
            "ok": False,
            "version": APP_VERSION,
            "posted_to_x": False,
            "reason": "keyword_required",
        }

    with db() as c:
        _ensure_yahoo_buzz_tables(c)
        _ensure_quote_post_log(c)

        row = c.execute("""
          SELECT
            y.keyword,
            y.rank,
            t.id AS trend_id,
            t.slug,
            t.status
          FROM yahoo_buzz_candidates y
          JOIN trends t ON t.keyword=y.keyword
          WHERE y.active=1
            AND t.status='🔥 バズり中'
            AND y.keyword=?
          ORDER BY y.rank ASC
          LIMIT 1
        """, (keyword,)).fetchone()

        if not row:
            return {
                "ok": False,
                "version": APP_VERSION,
                "posted_to_x": False,
                "reason": "qualified_buzzing_topic_not_found",
                "keyword": keyword,
            }

        quality = _buzzing_now_quality(c, row["trend_id"])
        if not quality["qualified"]:
            return {
                "ok": False,
                "version": APP_VERSION,
                "posted_to_x": False,
                "reason": "fresh_source_quality_gate_failed",
                "keyword": keyword,
                "quality": quality,
            }

        source = _fetch_yahoo_quote_candidates(keyword)
        eligible = [
            x for x in (source.get("items") or [])
            if x.get("detail_verified")
            and x.get("detail_keyword_relevant")
            and x.get("metric_verified")
            and x.get("strong_enough")
            and not x.get("detail_is_reply")
        ]

        if not eligible:
            return {
                "ok": False,
                "version": APP_VERSION,
                "posted_to_x": False,
                "reason": "strong_quote_target_not_found",
                "keyword": keyword,
                "source_mode": source.get("source_mode"),
            }

        target = eligible[0]

        duplicate = c.execute(
            "SELECT id,buffer_post_id,sent_at FROM buzzing_quote_posts WHERE tweet_id=? LIMIT 1",
            (target["tweet_id"],),
        ).fetchone()
        if duplicate:
            return {
                "ok": False,
                "version": APP_VERSION,
                "posted_to_x": False,
                "reason": "duplicate_quote_blocked",
                "keyword": keyword,
                "tweet_id": target["tweet_id"],
                "previous": dict(duplicate),
            }

        detail_url = f"{SITE_URL}/trend/{row['slug']}"
        comment = _sns_detective_comment(keyword, detail_url, target)

        send_result = _send_to_buffer_quote_post(
            target["tweet_id"],
            comment["text"],
            mode="shareNow",
        )

        if not send_result.get("ok"):
            return {
                "ok": False,
                "version": APP_VERSION,
                "posted_to_x": False,
                "keyword": keyword,
                "tweet_id": target["tweet_id"],
                "tweet_url": target["tweet_url"],
                "comment_template": comment["template"],
                "comment_text": comment["text"],
                "buffer_result": send_result,
            }

        ts = now_iso()
        c.execute("""
          INSERT INTO buzzing_quote_posts(
            keyword,tweet_id,tweet_url,comment_text,comment_template,
            buffer_post_id,status,sent_at
          ) VALUES(?,?,?,?,?,?,?,?)
        """, (
            keyword,
            target["tweet_id"],
            target["tweet_url"],
            comment["text"],
            comment["template"],
            str(send_result.get("post_id") or ""),
            "sent",
            ts,
        ))
        c.commit()

        return {
            "ok": True,
            "version": APP_VERSION,
            "posted_to_x": True,
            "automatic_quote_posting": False,
            "keyword": keyword,
            "tweet_id": target["tweet_id"],
            "tweet_url": target["tweet_url"],
            "likes": target.get("likes", 0),
            "reposts": target.get("reposts", 0),
            "comment_template": comment["template"],
            "comment_text": comment["text"],
            "buffer_result": send_result,
        }


def _parse_iso_dt(value: str):
    value = str(value or "").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def auto_quote_yahoo_buzzing_now():
    """Production Buzzing Now -> X quote-post automation.

    Flow:
      Yahoo realtime candidate
      -> BUZZ NOW article / fresh-source gate
      -> exact Yahoo tweet-detail verification
      -> strong non-reply X post
      -> SNS detective comment
      -> Buffer quote-post to X

    Safety:
      - max N per run (default 1)
      - daily cap (default 6)
      - global cooldown (default 60 min)
      - keyword cooldown (default 24 h)
      - exact tweet-detail verification required
      - no replies
      - duplicate tweet_id blocked forever
      - stronger engagement threshold for automatic posting
    """
    result = {
        "ok": True,
        "version": APP_VERSION,
        "enabled": YAHOO_QUOTE_AUTO_ENABLED,
        "posted_count": 0,
        "posts": [],
        "skipped": [],
        "errors": [],
    }

    if not YAHOO_QUOTE_AUTO_ENABLED:
        result["reason"] = "YAHOO_QUOTE_AUTO_ENABLED=false"
        return result

    if not BUFFER_API_KEY or not BUFFER_CHANNEL_ID:
        result["ok"] = False
        result["reason"] = "Buffer is not configured"
        return result

    now_dt = datetime.now(timezone.utc)
    today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    global_cutoff = now_dt - timedelta(minutes=YAHOO_QUOTE_GLOBAL_COOLDOWN_MINUTES)
    keyword_cutoff = now_dt - timedelta(hours=YAHOO_QUOTE_KEYWORD_COOLDOWN_HOURS)

    with db() as c:
        _ensure_yahoo_buzz_tables(c)
        _ensure_quote_post_log(c)

        # Daily cap.
        sent_today = c.execute(
            "SELECT COUNT(*) AS n FROM buzzing_quote_posts WHERE sent_at >= ? AND status='sent'",
            (today_start.isoformat(),),
        ).fetchone()
        daily_count = int(sent_today["n"] if sent_today else 0)
        if daily_count >= YAHOO_QUOTE_DAILY_CAP:
            result["reason"] = "daily_cap_reached"
            result["daily_count"] = daily_count
            return result

        # Global quote-post cooldown.
        latest = c.execute("""
          SELECT sent_at
          FROM buzzing_quote_posts
          WHERE status='sent'
          ORDER BY sent_at DESC
          LIMIT 1
        """).fetchone()
        if latest:
            latest_dt = _parse_iso_dt(latest["sent_at"])
            if latest_dt and latest_dt > global_cutoff:
                result["reason"] = "global_cooldown"
                result["latest_sent_at"] = latest["sent_at"]
                return result

        rows = c.execute("""
          SELECT
            y.keyword,
            y.rank,
            t.id AS trend_id,
            t.slug,
            t.status
          FROM yahoo_buzz_candidates y
          JOIN trends t ON t.keyword=y.keyword
          WHERE y.active=1
            AND t.status='🔥 バズり中'
          ORDER BY y.rank ASC
          LIMIT 10
        """).fetchall()

        for row in rows:
            if result["posted_count"] >= YAHOO_QUOTE_MAX_PER_RUN:
                break

            keyword = str(row["keyword"] or "").strip()
            if not keyword:
                continue

            # Same keyword cannot be pushed repeatedly every ranking refresh.
            recent_keyword = c.execute("""
              SELECT sent_at,tweet_id
              FROM buzzing_quote_posts
              WHERE keyword=? AND status='sent' AND sent_at >= ?
              ORDER BY sent_at DESC
              LIMIT 1
            """, (keyword, keyword_cutoff.isoformat())).fetchone()
            if recent_keyword:
                result["skipped"].append({
                    "keyword": keyword,
                    "reason": "keyword_cooldown",
                    "previous_tweet_id": recent_keyword["tweet_id"],
                    "previous_sent_at": recent_keyword["sent_at"],
                })
                continue

            quality = _buzzing_now_quality(c, row["trend_id"])
            if not quality["qualified"]:
                result["skipped"].append({
                    "keyword": keyword,
                    "reason": "fresh_source_quality_gate_failed",
                })
                continue

            source = _fetch_yahoo_quote_candidates(keyword)
            eligible = [
                x for x in (source.get("items") or [])
                if x.get("detail_verified")
                and x.get("detail_keyword_relevant")
                and x.get("metric_verified")
                and x.get("strong_enough")
                and not x.get("detail_is_reply")
                and (
                    int(x.get("likes") or 0) >= YAHOO_QUOTE_AUTO_MIN_LIKES
                    or int(x.get("reposts") or 0) >= YAHOO_QUOTE_AUTO_MIN_REPOSTS
                )
            ]

            if not eligible:
                result["skipped"].append({
                    "keyword": keyword,
                    "reason": "no_auto_grade_quote_target",
                    "source_mode": source.get("source_mode"),
                })
                continue

            target = eligible[0]

            duplicate = c.execute(
                "SELECT id,buffer_post_id,sent_at FROM buzzing_quote_posts WHERE tweet_id=? LIMIT 1",
                (target["tweet_id"],),
            ).fetchone()
            if duplicate:
                result["skipped"].append({
                    "keyword": keyword,
                    "reason": "duplicate_tweet_blocked",
                    "tweet_id": target["tweet_id"],
                })
                continue

            detail_url = f"{SITE_URL}/trend/{row['slug']}"
            comment = _sns_detective_comment(keyword, detail_url, target)

            send_result = _send_to_buffer_quote_post(
                target["tweet_id"],
                comment["text"],
                mode="shareNow",
            )

            if not send_result.get("ok"):
                result["errors"].append({
                    "keyword": keyword,
                    "tweet_id": target["tweet_id"],
                    "reason": "buffer_quote_post_failed",
                    "buffer_result": send_result,
                })
                continue

            ts = now_iso()
            c.execute("""
              INSERT INTO buzzing_quote_posts(
                keyword,tweet_id,tweet_url,comment_text,comment_template,
                buffer_post_id,status,sent_at
              ) VALUES(?,?,?,?,?,?,?,?)
            """, (
                keyword,
                target["tweet_id"],
                target["tweet_url"],
                comment["text"],
                comment["template"],
                str(send_result.get("post_id") or ""),
                "sent",
                ts,
            ))
            c.commit()

            result["posted_count"] += 1
            result["posts"].append({
                "keyword": keyword,
                "rank": int(row["rank"]),
                "tweet_id": target["tweet_id"],
                "tweet_url": target["tweet_url"],
                "likes": int(target.get("likes") or 0),
                "reposts": int(target.get("reposts") or 0),
                "comment_template": comment["template"],
                "comment_text": comment["text"],
                "detail_url": detail_url,
                "buffer_post_id": str(send_result.get("post_id") or ""),
            })

    if result["posted_count"] == 0 and not result.get("reason"):
        result["reason"] = "no_qualified_post_this_run"

    return result


@app.get("/api/buzzing-now/auto-status")
def yahoo_buzz_auto_status():
    with db() as c:
        _ensure_quote_post_log(c)
        rows = c.execute("""
          SELECT keyword,tweet_id,tweet_url,comment_template,buffer_post_id,status,sent_at
          FROM buzzing_quote_posts
          ORDER BY sent_at DESC
          LIMIT 20
        """).fetchall()

    return {
        "ok": True,
        "version": APP_VERSION,
        "auto_enabled": YAHOO_QUOTE_AUTO_ENABLED,
        "daily_cap": YAHOO_QUOTE_DAILY_CAP,
        "global_cooldown_minutes": YAHOO_QUOTE_GLOBAL_COOLDOWN_MINUTES,
        "keyword_cooldown_hours": YAHOO_QUOTE_KEYWORD_COOLDOWN_HOURS,
        "auto_min_likes": YAHOO_QUOTE_AUTO_MIN_LIKES,
        "auto_min_reposts": YAHOO_QUOTE_AUTO_MIN_REPOSTS,
        "recent_posts": [dict(r) for r in rows],
    }


@app.get("/api/buzzing-now/run-now")
def yahoo_buzz_run_now():
    """Manual production run using the same flow as the scheduled collector.
    This CAN post one qualifying quote to X.
    """
    promote = promote_yahoo_buzz_candidates(limit=YAHOO_BUZZ_PROMOTE_LIMIT)
    quote_result = auto_quote_yahoo_buzzing_now()
    return {
        "ok": True,
        "version": APP_VERSION,
        "promote": promote,
        "quote": quote_result,
    }


@app.get("/api/buzzing-now/yahoo-status")
def yahoo_buzz_status():
    with db() as c:
        _ensure_yahoo_buzz_tables(c)
        rows = c.execute("""
          SELECT keyword,rank,source_url,detected_at,last_seen_at,active
          FROM yahoo_buzz_candidates
          WHERE active=1
          ORDER BY rank ASC
          LIMIT 50
        """).fetchall()
    return {
        "ok": True,
        "version": APP_VERSION,
        "source": "Yahoo!リアルタイム検索 public HTML",
        "x_api_used": False,
        "items": [dict(r) for r in rows],
    }


@app.get("/api/buzzing-now/yahoo-collect-preview")
def yahoo_buzz_collect_preview():
    """Manual test. Reads Yahoo public page and stores candidates; never posts to X."""
    ts = now_iso()
    with db() as c:
        result = collect_yahoo_realtime_buzz(c, ts)
        c.commit()
    result["posted_to_x"] = False
    result["x_api_used"] = False
    return result


def collect_real_sources():
    ts = now_iso()

    # Phase 1: normal data refresh. Commit first so Yahoo candidates are visible
    # to the promotion/quote phase even when db() uses a separate connection.
    with db() as c:
        g = collect_google_trends(c, ts)
        w = collect_wikimedia(c, ts)
        yahoo_buzz = collect_yahoo_realtime_buzz(c, ts)
        news_count = collect_fast_news(c, ts, limit=6)
        refresh_confidence(c, ts)
        refresh_propagation(c, ts)
        refresh_monetization(c, ts)
        snapshot_v9_sources(c, ts)
        refresh_v9_velocity(c, ts)
        refresh_real_traffic_forecast(c, ts)
        evaluate_predictions(c, ts)
        create_predictions(c, ts)
        cautiously_tune_model(c)

        # Existing Pre-Buzz own-post automation remains unchanged.
        social_result = auto_post_social(c, ts)
        c.commit()

    # Phase 2: Yahoo Buzzing Now automation.
    # All active candidates are article-checked first, then at most one strong
    # exact-verified X post is quote-posted through Buffer.
    yahoo_promote = promote_yahoo_buzz_candidates(limit=YAHOO_BUZZ_PROMOTE_LIMIT)
    yahoo_quote = auto_quote_yahoo_buzzing_now()

    return {
        "google_trends": g,
        "wikimedia": w,
        "yahoo_buzzing_now": yahoo_buzz,
        "yahoo_promote": yahoo_promote,
        "yahoo_quote": yahoo_quote,
        "news": news_count,
        "total": g + w,
        "social": social_result,
    }


def demo_tick():
    if not DEMO_MODE:
        return
    ensure_demo_keywords()
    ts = now_iso()
    with db() as c:
        rows = c.execute("SELECT * FROM trends").fetchall()
        for r in rows:
            pre = float(r["pre_buzz_score"])
            buzz = float(r["buzz_score"])
            acc = float(r["acceleration"])

            # random walk with mild momentum so some trends rise/fall
            acc += random.uniform(-0.05, 0.07)
            acc = max(-0.2, min(0.6, acc))

            pre += acc * random.uniform(4.0, 9.0) + random.uniform(-2.2, 2.2)
            buzz += acc * random.uniform(2.5, 6.0) + random.uniform(-1.6, 1.6)

            pre = max(0, min(100, pre))
            buzz = max(0, min(100, buzz))

            # occasional simulated spike
            if random.random() < 0.06:
                pre = min(100, pre + random.uniform(8, 16))
                buzz = min(100, buzz + random.uniform(5, 12))
                acc = min(0.8, acc + random.uniform(0.10, 0.22))

            status = classify(pre,buzz,acc)

            c.execute("""
                UPDATE trends
                SET pre_buzz_score=?, buzz_score=?, acceleration=?, status=?, updated_at=?
                WHERE id=?
            """, (round(pre,1), round(buzz,1), round(acc,2), status, ts, r["id"]))

            c.execute("""
                INSERT INTO trend_history(
                    trend_id,pre_buzz_score,buzz_score,acceleration,recorded_at
                ) VALUES(?,?,?,?,?)
            """, (r["id"], round(pre,1), round(buzz,1), round(acc,2), ts))

            fresh = dict(r)
            fresh["pre_buzz_score"] = round(pre,1)
            fresh["buzz_score"] = round(buzz,1)
            fresh["acceleration"] = round(acc,2)
            simulate_traffic(c, fresh, ts)

        auto_grow_pages(c, ts)
        evaluate_predictions(c, ts)
        create_predictions(c, ts)
        cautiously_tune_model(c)

        c.execute("""
            INSERT INTO system_state(key,value) VALUES('last_demo_tick',?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,(ts,))

        # keep demo DB small
        c.execute("""
            DELETE FROM trend_history
            WHERE id NOT IN (
                SELECT id FROM trend_history ORDER BY id DESC LIMIT 3000
            )
        """)



scheduler = BackgroundScheduler()

@app.on_event("startup")
def startup():
    # V35: PostgreSQL is already initialized. Do not block Render startup on DB DDL.
    # init_db() remains available for explicit maintenance, but is not run here.

    if DEMO_MODE:
        ensure_demo_keywords()
        demo_tick()
        scheduler.add_job(
            demo_tick,
            "interval",
            seconds=DEMO_INTERVAL_SECONDS,
            id="demo_tick",
            replace_existing=True,
            max_instances=1
        )

    if REAL_DATA_MODE:
        # Run once at startup, then continue periodically.
    # V34.9 disabled: collect_real_sources()
        scheduler.add_job(
            collect_real_sources,
            "interval",
            minutes=REAL_DATA_INTERVAL_MINUTES,
            id="real_source_collector",
            replace_existing=True,
            max_instances=1
        )

    if not scheduler.running:
        scheduler.start()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    with db() as c:
        rows = c.execute("""
            SELECT * FROM trends
            ORDER BY pre_buzz_score DESC, acceleration DESC, buzz_score DESC
            LIMIT 50
        """).fetchall()
    return templates.TemplateResponse(request, "index.html", {
        "request": request,
        "trends": rows,
        "site_name": SITE_NAME,
    })


@app.get("/trend/{slug}", response_class=HTMLResponse)
def trend_detail(slug: str, request: Request):
    with db() as c:
        trend = c.execute("SELECT * FROM trends WHERE slug=?", (slug,)).fetchone()
        if not trend:
            raise HTTPException(404, "Trend not found")

        related = c.execute(
            "SELECT keyword FROM related_keywords WHERE trend_id=? ORDER BY id",
            (trend["id"],)
        ).fetchall()

        sources = c.execute(
            "SELECT * FROM sources WHERE trend_id=? ORDER BY CASE WHEN published_at='' THEN 1 ELSE 0 END, published_at DESC, id DESC LIMIT 10",
            (trend["id"],)
        ).fetchall()

        # V20: if this SEO detail page still has no supporting articles, perform a
        # throttled targeted refresh for this exact keyword. This avoids waiting for
        # the keyword to appear in the small scheduled GDELT batch.
        if NEWS_ENRICHMENT_ENABLED and not sources and _news_check_is_due(c, trend["id"], hours=1/6):
            try:
                _enrich_keyword_news(c, trend["keyword"], now_iso(), force=True)
                c.commit()
                trend = c.execute("SELECT * FROM trends WHERE id=?", (trend["id"],)).fetchone()
                sources = c.execute(
                    "SELECT * FROM sources WHERE trend_id=? ORDER BY CASE WHEN published_at='' THEN 1 ELSE 0 END, published_at DESC, id DESC LIMIT 10",
                    (trend["id"],)
                ).fetchall()
                related = c.execute(
                    "SELECT keyword FROM related_keywords WHERE trend_id=? ORDER BY id LIMIT 12",
                    (trend["id"],)
                ).fetchall()
            except Exception as e:
                logger.warning("detail news enrichment failed slug=%s: %s", slug, e)

    title = f"{trend['keyword']}とは？なぜ今話題？｜{SITE_NAME}"
    why_text = " ".join(str(trend["why_now"] or "").split())
    description = (why_text[:145] + "…") if len(why_text) > 145 else why_text
    if not description:
        description = (
            f"{trend['keyword']}がなぜ注目されているのかを、"
            f"Pre-Buzz Score・Buzz Score・関連キーワード・情報源から整理。"
        )
    canonical = f"{SITE_URL}/trend/{trend['slug']}"

    return templates.TemplateResponse(request, "trend.html", {
        "request": request,
        "trend": trend,
        "related": related,
        "sources": sources,
        "site_name": SITE_NAME,
        "title": title,
        "description": description,
        "canonical": canonical,
    })



@app.get("/api/trends/{slug}/news-diagnostic")
def trend_news_diagnostic(slug: str, force: int = 0):
    with db() as c:
        trend=c.execute("SELECT id,keyword,why_now FROM trends WHERE slug=?",(slug,)).fetchone()
        if not trend:
            raise HTTPException(404,"Trend not found")

        # V22: force=1 performs an immediate provider check so diagnostics never
        # stay blank merely because an older version wrote news_checked first.
        if force:
            try:
                _enrich_keyword_news(c, trend["keyword"], now_iso(), force=True)
                c.commit()
            except Exception as e:
                _record_news_diagnostic(c, trend["id"], "force", f"error {type(e).__name__}: {e}", now_iso())
                c.commit()

        trend=c.execute("SELECT id,keyword,why_now FROM trends WHERE id=?",(trend["id"],)).fetchone()
        sources=c.execute("SELECT publisher,title,url,published_at,source_label FROM sources WHERE trend_id=? ORDER BY id DESC LIMIT 10",(trend["id"],)).fetchall()
        related=c.execute("SELECT keyword FROM related_keywords WHERE trend_id=? ORDER BY id DESC LIMIT 12",(trend["id"],)).fetchall()
        states=c.execute("SELECT key,value FROM system_state WHERE key LIKE ? ORDER BY key",(f"news_diag:{trend['id']}:%",)).fetchall()
        checked=c.execute("SELECT value FROM system_state WHERE key=?",(f"news_checked:{trend['id']}",)).fetchone()
    return {
        "keyword":trend["keyword"],
        "why_now":trend["why_now"],
        "source_count":len(sources),
        "sources":[dict(x) for x in sources],
        "related_keywords":[x["keyword"] for x in related],
        "last_checked": checked["value"] if checked else None,
        "diagnostics":{x["key"].split(":")[-1]:x["value"] for x in states},
    }

@app.get("/api/system-status")
def system_status():
    with db() as c:
        row = c.execute("SELECT value FROM system_state WHERE key='last_demo_tick'").fetchone()
    return {
        "demo_mode": DEMO_MODE,
        "interval_seconds": DEMO_INTERVAL_SECONDS if DEMO_MODE else None,
        "last_update": row["value"] if row else None
    }


@app.get("/api/trends/{slug}/history")
def trend_history(slug: str, limit: int = 30):
    limit = max(1, min(limit, 100))
    with db() as c:
        trend = c.execute("SELECT id,keyword FROM trends WHERE slug=?", (slug,)).fetchone()
        if not trend:
            raise HTTPException(404, "Trend not found")
        rows = c.execute("""
            SELECT pre_buzz_score,buzz_score,acceleration,recorded_at
            FROM trend_history
            WHERE trend_id=?
            ORDER BY id DESC
            LIMIT ?
        """,(trend["id"],limit)).fetchall()
    items = [dict(r) for r in reversed(rows)]
    return {"keyword": trend["keyword"], "items": items}



@app.get("/api/traffic-ranking")
def traffic_ranking(limit: int = 50):
    limit=max(1,min(limit,100))
    with db() as c:
        rows=c.execute("""
            SELECT
              t.id,
              t.keyword,t.slug,t.category,t.status,
              t.pre_buzz_score,t.buzz_score,t.acceleration,
              COALESCE(x.impressions,0) AS impressions,
              COALESCE(x.clicks,0) AS clicks,
              COALESCE(x.pageviews,0) AS pageviews,
              COALESCE(x.last_ctr,0) AS ctr,
              COALESCE(x.traffic_potential,0) AS traffic_potential,
              COALESCE(cs.source_count,0) AS source_count,
              COALESCE(cs.confidence_score,0) AS confidence_score,
              COALESCE(cs.confidence_label,'デモ/未確認') AS confidence_label,
              COALESCE(cs.corroborated,0) AS corroborated,
              COALESCE(ps.first_source,'') AS first_source,
              COALESCE(ps.source_sequence,'') AS source_sequence,
              ps.propagation_minutes AS propagation_minutes,
              COALESCE(ps.velocity_30m,0) AS velocity_30m,
              COALESCE(ps.velocity_1h,0) AS velocity_1h,
              COALESCE(ps.velocity_3h,0) AS velocity_3h,
              COALESCE((
                SELECT STRING_AGG(s.title, ' ')
                FROM sources s
                WHERE s.trend_id=t.id
                  AND COALESCE(TRIM(s.title),'')<>''
              ), '') AS category_context
            FROM trends t
            LEFT JOIN traffic_totals x ON x.trend_id=t.id
            LEFT JOIN confidence_state cs ON cs.trend_id=t.id
            LEFT JOIN propagation_state ps ON ps.trend_id=t.id
            ORDER BY traffic_potential DESC, confidence_score DESC, pageviews DESC
            LIMIT ?
        """,(limit,)).fetchall()

    items=[]
    for r in rows:
        item=dict(r)
        stored=str(item.get("category") or "").strip()
        inferred=infer_category(item.get("keyword",""), item.pop("category_context",""))

        # Keep a deliberate/manual category, but repair legacy generic rows live.
        if stored in ("", "総合", "ニュース・時事"):
            item["category"]=inferred
        else:
            item["category"]=stored

        item.pop("id", None)
        items.append(item)

    return {"items":items}


@app.get("/api/trends/{slug}/traffic")
def trend_traffic(slug: str, limit: int = 24):
    limit=max(1,min(limit,100))
    with db() as c:
        trend=c.execute("SELECT id,keyword FROM trends WHERE slug=?",(slug,)).fetchone()
        if not trend:
            raise HTTPException(404,"Trend not found")
        total=c.execute("SELECT * FROM traffic_totals WHERE trend_id=?",(trend["id"],)).fetchone()
        hist=c.execute("""
            SELECT impressions,clicks,pageviews,ctr,traffic_potential,recorded_at
            FROM traffic_history
            WHERE trend_id=?
            ORDER BY id DESC LIMIT ?
        """,(trend["id"],limit)).fetchall()
    return {
        "keyword":trend["keyword"],
        "total":dict(total) if total else None,
        "history":[dict(r) for r in reversed(hist)]
    }



@app.get("/api/growth-ranking")
def growth_ranking(limit: int = 50):
    limit=max(1,min(limit,100))
    with db() as c:
        rows=c.execute("""
          SELECT t.keyword,t.slug,t.category,t.status,
                 COALESCE(g.level,0) AS growth_level,
                 COALESCE(g.quality_score,0) AS quality_score,
                 COALESCE(g.decision,'観察中') AS decision,
                 COALESCE(g.last_reason,'') AS reason,
                 COALESCE(x.pageviews,0) AS pageviews,
                 COALESCE(x.traffic_potential,0) AS traffic_potential
          FROM trends t
          LEFT JOIN growth_state g ON g.trend_id=t.id
          LEFT JOIN traffic_totals x ON x.trend_id=t.id
          ORDER BY g.quality_score DESC, x.traffic_potential DESC
          LIMIT ?
        """,(limit,)).fetchall()
    return {"items":[dict(r) for r in rows]}


@app.get("/api/trends/{slug}/growth")
def trend_growth(slug: str):
    with db() as c:
        t=c.execute("SELECT id,keyword FROM trends WHERE slug=?",(slug,)).fetchone()
        if not t:
            raise HTTPException(404,"Trend not found")
        state=c.execute("SELECT * FROM growth_state WHERE trend_id=?",(t["id"],)).fetchone()
        logs=c.execute("""
          SELECT old_level,new_level,decision,reason,recorded_at
          FROM growth_log WHERE trend_id=?
          ORDER BY id DESC LIMIT 10
        """,(t["id"],)).fetchall()
    return {
      "keyword":t["keyword"],
      "state":dict(state) if state else None,
      "logs":[dict(r) for r in logs]
    }



@app.get("/api/learning-status")
def learning_status():
    with db() as c:
        ensure_model_state(c)
        state={r["key"]:r["value"] for r in c.execute("SELECT key,value FROM model_state").fetchall()}
        pending=c.execute("SELECT COUNT(*) AS n FROM predictions WHERE status='pending'").fetchone()["n"]
        recent=c.execute("""
          SELECT t.keyword,r.hit,r.score,r.buzz_gain,r.traffic_gain,r.pv_gain,r.evaluated_at
          FROM prediction_results r
          JOIN trends t ON t.id=r.trend_id
          ORDER BY r.id DESC LIMIT 12
        """).fetchall()
    return {"model":state,"pending":pending,"recent":[dict(r) for r in recent]}


@app.get("/api/trends/{slug}/predictions")
def trend_predictions(slug: str):
    with db() as c:
        t=c.execute("SELECT id,keyword FROM trends WHERE slug=?",(slug,)).fetchone()
        if not t:
            raise HTTPException(404,"Trend not found")
        rows=c.execute("""
          SELECT p.id,p.predicted_pre_buzz,p.predicted_buzz,p.predicted_acceleration,
                 p.predicted_traffic_potential,p.predicted_pageviews,p.status,
                 p.created_at,p.evaluated_at,
                 r.actual_buzz,r.actual_traffic_potential,r.actual_pageviews,
                 r.buzz_gain,r.traffic_gain,r.pv_gain,r.hit,r.score
          FROM predictions p
          LEFT JOIN prediction_results r ON r.prediction_id=p.id
          WHERE p.trend_id=?
          ORDER BY p.id DESC LIMIT 10
        """,(t["id"],)).fetchall()
    return {"keyword":t["keyword"],"items":[dict(r) for r in rows]}



def _graphql_string(value: str) -> str:
    return json.dumps(value or "", ensure_ascii=False)


def _send_to_buffer_direct(post_text: str, image_url: str = "", mode: str = "shareNow") -> dict:
    if not BUFFER_API_KEY:
        return {"ok": False, "reason": "BUFFER_API_KEY is not configured"}
    if not BUFFER_CHANNEL_ID:
        return {"ok": False, "reason": "BUFFER_CHANNEL_ID is not configured"}

    assets = ""
    if image_url:
        assets = "assets: [{ image: { url: " + _graphql_string(image_url) + " } }] "

    query = (
        "mutation CreateBuzzNowPost { createPost(input: { "
        + "text: " + _graphql_string(post_text) + " "
        + "channelId: " + _graphql_string(BUFFER_CHANNEL_ID) + " "
        + "schedulingType: automatic "
        + "mode: " + mode + " "
        + assets
        + "}) { "
        + "... on PostActionSuccess { post { id text status assets { id mimeType } } } "
        + "... on MutationError { message } "
        + "} }"
    )
    try:
        response = httpx.post(
            BUFFER_API_URL,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {BUFFER_API_KEY}"},
            json={"query": query},
            timeout=45.0,
        )
        try:
            body = response.json()
        except Exception:
            body = {"raw": response.text[:1000]}
        if response.status_code != 200:
            return {"ok": False, "status_code": response.status_code, "reason": "Buffer HTTP error", "response": body}
        if isinstance(body, dict) and body.get("errors"):
            return {"ok": False, "status_code": 200, "reason": "Buffer GraphQL error", "response": body}
        result = ((body or {}).get("data") or {}).get("createPost") if isinstance(body, dict) else None
        if not result:
            return {"ok": False, "reason": "Buffer returned no createPost result", "response": body}
        if result.get("message"):
            return {"ok": False, "reason": result["message"], "response": body}
        post = result.get("post")
        if post and post.get("id"):
            return {"ok": True, "post_id": post["id"], "post": post}
        return {"ok": False, "reason": "Buffer did not return a post id", "response": body}
    except Exception as exc:
        logger.exception("Direct Buffer post failed")
        return {"ok": False, "reason": str(exc)[:500]}


def _send_to_make(payload: dict) -> dict:
    """Send one JSON payload to the configured Make.com Custom Webhook.

    The webhook URL is intentionally read only from Render environment variables
    so it never has to be committed to GitHub.
    """
    if not MAKE_WEBHOOK_URL:
        raise HTTPException(503, "MAKE_WEBHOOK_URL is not configured")

    try:
        response = httpx.post(
            MAKE_WEBHOOK_URL,
            json=payload,
            timeout=15,
            follow_redirects=True,
            headers={
                "User-Agent": "BUZZ-NOW/30 Make-Webhook",
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        return {
            "ok": True,
            "status_code": response.status_code,
            "response": (response.text or "")[:500],
        }
    except httpx.HTTPError as exc:
        logger.exception("Make webhook send failed")
        raise HTTPException(502, f"Make webhook send failed: {exc}")


@app.get("/api/social/test-send")
def social_test_send():
    """Browser-friendly one-time connection test for Make.com.

    Keep SOCIAL_TEST_ENABLED=false in normal production. During setup, enable it
    temporarily in Render, open this endpoint once, then disable it again.
    """
    if not SOCIAL_TEST_ENABLED:
        raise HTTPException(403, "SOCIAL_TEST_ENABLED is false")

    payload = {
        "keyword": "BUZZ NOW テスト",
        "pre_buzz_score": 92,
        "traffic_potential": 81,
        "status": "急上昇",
        "why_now": "検索量と情報源の増加を検知",
        "detail_url": f"{SITE_URL}/",
        "image_url": "",
        "image_ready": False,
        "post_text": (
            "🚀 BUZZ NOW｜急上昇を検知\n"
            "BUZZ NOW テスト\n"
            "Pre-Buzz Score：92\n"
            "Traffic Potential：81"
        ),
        "source": "buzz-now-v30-test",
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }
    result = _send_to_make(payload)
    return {
        "ok": True,
        "message": "Test payload sent to Make.com",
        "payload": payload,
        "make": result,
    }


@app.post("/api/social/send-test")
def social_send_test_post():
    """POST alias for the same temporary Make.com connection test."""
    return social_test_send()


@app.get("/api/social/why-not")
def social_why_not(limit: int = 10):
    """Show the actual visible TOP order and posting eligibility without rank compression."""
    limit = max(1, min(int(limit), 10))
    now_dt = datetime.now(timezone.utc)

    with db() as c:
        rows = _visible_top_rows(c, limit=limit)
        items = []

        for visible_rank, row in enumerate(rows, start=1):
            pre_ok = float(row["pre_buzz_score"] or 0) >= SOCIAL_MIN_PREBUZZ
            traffic_ok = float(row["traffic_potential"] or 0) >= SOCIAL_MIN_TRAFFIC
            status_ok = "下降" not in str(row["status"] or "")
            signal_ok = pre_ok and traffic_ok and status_ok

            allowed, cooldown_reason = _social_post_allowed(c, row, now_dt)
            confidence = float(row["confidence_score"] or 0)
            mode = (
                "confirmed_reason"
                if confidence >= SOCIAL_MIN_CONFIDENCE
                else "cautious_signal_only"
            )

            blocked = []
            if not pre_ok:
                blocked.append(f"Pre-Buzz<{SOCIAL_MIN_PREBUZZ:g}")
            if not traffic_ok:
                blocked.append(f"Traffic<{SOCIAL_MIN_TRAFFIC:g}")
            if not status_ok:
                blocked.append("下降中")
            if signal_ok and not allowed:
                blocked.append(cooldown_reason)

            items.append({
                "visible_rank": visible_rank,
                "keyword": row["keyword"],
                "pre_buzz_score": round(float(row["pre_buzz_score"] or 0), 1),
                "traffic_potential": round(float(row["traffic_potential"] or 0), 1),
                "confidence_score": round(confidence, 1),
                "confidence_label": row["confidence_label"],
                "pageviews": int(row["pageviews"] or 0),
                "first_source": row["first_source"],
                "post_mode": mode,
                "inside_visible_top10": True,
                "signal_gate_ok": signal_ok,
                "post_now_ok": bool(signal_ok and allowed),
                "blocked_by": blocked,
            })

    return {
        "ok": True,
        "version": APP_VERSION,
        "ranking_alignment": "exact visible TOP first; posting filters applied only after TOP10 is frozen",
        "social_candidate_scope": "visible TOP10 only; rank 11+ can never auto-post",
        "low_confidence_policy": "post signal only; do not assert the event as fact",
        "daily_cap": SOCIAL_DAILY_CAP,
        "global_cooldown_minutes": SOCIAL_GLOBAL_COOLDOWN_MINUTES,
        "items": items,
    }


@app.get("/api/social/run-now")
def social_run_now():
    """REAL normal BUZZ NOW X run. Existing caps/cooldowns still apply."""
    ts = now_iso()
    with db() as c:
        result = auto_post_social(c, ts)
        c.commit()
    return {
        "ok": True,
        "version": APP_VERSION,
        "result": result,
    }


@app.get("/api/social/status")
def social_status():
    with db() as c:
        last = c.execute("""
            SELECT keyword,pre_buzz_score,traffic_potential,make_status,posted_at
            FROM social_posts ORDER BY id DESC LIMIT 1
        """).fetchone()
        sent_24h = c.execute(
            "SELECT COUNT(*) AS n FROM social_posts WHERE make_status=1 AND posted_at>=?",
            ((datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),),
        ).fetchone()["n"]
    return {
        "version": APP_VERSION,
        "production_social_route": "buffer-direct",
        "buffer_api_key_configured": bool(BUFFER_API_KEY),
        "buffer_channel_id_configured": bool(BUFFER_CHANNEL_ID),
        "make_webhook_configured": bool(MAKE_WEBHOOK_URL),
        "social_image_mode": "ai_context_visual",
        "social_ai_image_enabled": SOCIAL_AI_IMAGE_ENABLED,
        "openai_api_key_configured": bool(OPENAI_API_KEY),
        "openai_image_model": OPENAI_IMAGE_MODEL,
        "social_test_enabled": SOCIAL_TEST_ENABLED,
        "social_auto_enabled": SOCIAL_AUTO_ENABLED,
        "min_pre_buzz": SOCIAL_MIN_PREBUZZ,
        "min_traffic_potential": SOCIAL_MIN_TRAFFIC,
        "min_confidence_score": SOCIAL_MIN_CONFIDENCE,
        "keyword_cooldown_hours": SOCIAL_KEYWORD_COOLDOWN_HOURS,
        "global_cooldown_minutes": SOCIAL_GLOBAL_COOLDOWN_MINUTES,
        "daily_cap": SOCIAL_DAILY_CAP,
        "max_posts_per_run": SOCIAL_MAX_POSTS_PER_RUN,
        "sent_last_24h": int(sent_24h or 0),
        "last_post": dict(last) if last else None,
        "version": APP_VERSION,
    }


def _social_image_payload(trend_id: int) -> bytes:
    """Load a cached social image and normalize it to a real PNG byte stream.

    Buffer fetches remote media from its own servers.  Serving a predictable
    PNG with explicit response headers avoids scraper/CDN ambiguity around a
    dynamic database-backed endpoint.
    """
    with db() as c:
        row = c.execute(
            "SELECT image_b64,mime_type FROM social_images WHERE trend_id=?",
            (trend_id,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "Social image not generated yet")

    try:
        raw = base64.b64decode(row["image_b64"], validate=True)
    except Exception:
        raise HTTPException(500, "Stored social image is invalid")

    # Re-encode through Pillow so the URL extension, MIME type and actual file
    # format are guaranteed to agree. This also strips metadata that some
    # third-party media fetchers can reject.
    try:
        with Image.open(BytesIO(raw)) as im:
            if im.mode not in ("RGB", "RGBA"):
                im = im.convert("RGB")
            out = BytesIO()
            im.save(out, format="PNG", optimize=False)
            png = out.getvalue()
    except Exception:
        raise HTTPException(500, "Stored social image could not be decoded")

    if not png:
        raise HTTPException(500, "Stored social image is empty")
    return png


def _social_image_headers(trend_id: int, content_length: int) -> dict:
    return {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Content-Disposition": f'inline; filename="buzz-now-{int(trend_id)}.png"',
        "Content-Length": str(int(content_length)),
        "Accept-Ranges": "bytes",
        "X-Content-Type-Options": "nosniff",
    }


@app.get("/social-image/{trend_id}.png")
def social_ai_image_png(trend_id: int):
    """Direct public image response for Make/Buffer/X. No auth, no redirect."""
    png = _social_image_payload(trend_id)
    return Response(
        content=png,
        media_type="image/png",
        headers=_social_image_headers(trend_id, len(png)),
        status_code=200,
    )


@app.head("/social-image/{trend_id}.png")
def social_ai_image_png_head(trend_id: int):
    """Explicit HEAD support for third-party media fetchers such as Buffer."""
    png = _social_image_payload(trend_id)
    return Response(
        content=b"",
        media_type="image/png",
        headers=_social_image_headers(trend_id, len(png)),
        status_code=200,
    )


_BUZZ_NOW_APPROVED_OVERLAY_B64 = "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9yMeb7BbWpKN36QBvySIFIXAq76+XuF7Dq9qa/PDTutzqUnPn79E/eeDFqC29t957LK2tlb07rkW36Ktviy0+bK+/88GwV/13kqfzxfo37+/AlJob0+JTEdHdhrQWvUVx1//5UExvmA/Mv/L+ZbH9l34pkMGv0zA50NFOg09pE8fWd4/0Dlv3qp/ebxdCKGKqr5VAX+ZXwel1HM++6ynOnDmjBlm3axZ7nfYXwv9ySot9fUt85fptmS77OxMtwPoAkNpIiIiIiKi7+TBS36DX+JbbvM/eS5fFTB82a/vYt/f1He9b/qaazMz39Yzv1lf/qb3xH/yOn7ZvsW/6di+i8+Fb7u/7/x+nDlz5r/zmMV/2echERERERF9DyEE/fCupf439wP9Ne/X/8a+x8oK+rZ9i33mu/us+E9s979tn0RERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERE3zPB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"

def _social_image_jpeg_payload(trend_id: int) -> bytes:
    """V35.8: AI news background + approved fixed graphic overlay + variable buzzword only."""
    with db() as c:
        row = c.execute(
            """SELECT si.image_b64, t.keyword
               FROM social_images si
               JOIN trends t ON t.id=si.trend_id
               WHERE si.trend_id=?""",
            (trend_id,),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Social image not found")

        try:
            original = base64.b64decode(row["image_b64"])
            image = Image.open(BytesIO(original)).convert("RGB")
            target_ratio = 1200 / 675
            ratio = image.width / image.height
            if ratio > target_ratio:
                nw = int(image.height * target_ratio)
                left = max(0, (image.width - nw)//2)
                image = image.crop((left,0,left+nw,image.height))
            elif ratio < target_ratio:
                nh = int(image.width / target_ratio)
                top = max(0,(image.height-nh)//2)
                image = image.crop((0,top,image.width,top+nh))
            image = image.resize((1200,675), Image.Resampling.LANCZOS).convert("RGBA")

            # Readability gradient on left; background remains the AI-generated news image.
            shade = Image.new("RGBA", image.size, (0,0,0,0))
            sd = ImageDraw.Draw(shade)
            for x in range(800):
                alpha = int(150 * (1-(x/800)**1.7))
                sd.rectangle((x,0,x+1,675), fill=(0,0,0,max(0,alpha)))
            image = Image.alpha_composite(image, shade)

            # One transparent approved overlay: logo, WHY NOW, brush, tags, signature.
            fixed = Image.open(BytesIO(base64.b64decode(_BUZZ_NOW_APPROVED_OVERLAY_B64))).convert("RGBA")
            image = Image.alpha_composite(image, fixed)
            d = ImageDraw.Draw(image)

            def buzzword_font(size):
                # Bold Gothic first. IPAex Gothic is the guaranteed Japanese fallback.
                paths = [
                    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
                    "/usr/share/fonts/opentype/noto/NotoSansCJKJP-Bold.otf",
                    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                    "/opt/render/project/src/.venv/lib/python3.13/site-packages/japanize_matplotlib/fonts/ipaexg.ttf",
                ]
                for p in paths:
                    try:
                        return ImageFont.truetype(p, size)
                    except Exception:
                        pass
                raise RuntimeError("Japanese Gothic font not found")

            # ONLY variable design element: current buzzword.
            keyword = (row["keyword"] or "").strip()
            size = 126
            while size > 56:
                f = buzzword_font(size)
                bb = d.textbbox((0,0), keyword, font=f, stroke_width=5)
                if bb[2]-bb[0] <= 730:
                    break
                size -= 2

            x, y = 48, 160

            # Strong black outer edge/shadow.
            d.text((x+4,y+5), keyword, font=f, fill=(0,0,0,225),
                   stroke_width=7, stroke_fill=(0,0,0,235))

            # Thicken the actual white glyph body even when IPAex Gothic is used.
            d.text((x,y), keyword, font=f, fill="white",
                   stroke_width=4, stroke_fill="white")

            # Final crisp thin black contour.
            d.text((x,y), keyword, font=f, fill="white",
                   stroke_width=2, stroke_fill=(10,10,10,255))

            out = BytesIO()
            image.convert("RGB").save(out, "JPEG", quality=88, optimize=True)
            data = out.getvalue()

            encoded = base64.b64encode(data).decode("ascii")
            ts = now_iso()
            c.execute("""
                INSERT INTO social_image_derivatives(trend_id,jpeg_b64,byte_length,created_at)
                VALUES(?,?,?,?)
                ON CONFLICT(trend_id) DO UPDATE SET
                    jpeg_b64=excluded.jpeg_b64,
                    byte_length=excluded.byte_length,
                    created_at=excluded.created_at
            """, (trend_id, encoded, len(data), ts))
            return data
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("V35.8 approved-overlay JPEG failed trend_id=%s", trend_id)
            raise HTTPException(500, f"Approved-overlay JPEG failed: {str(exc)[:200]}")

def _prewarm_social_jpeg(trend_id: int) -> None:
    """Create the derivative before Make/Buffer is called."""
    _social_image_jpeg_payload(trend_id)

def _social_jpeg_headers(trend_id: int, content_length: int) -> dict:
    return {
        "Cache-Control": "public, max-age=31536000, immutable",
        "Content-Disposition": f'inline; filename="buzz-now-{int(trend_id)}.jpg"',
        "Content-Length": str(int(content_length)),
        "X-Content-Type-Options": "nosniff",
    }


@app.get("/social-image/{trend_id}.jpg")
def social_ai_image_jpg(trend_id: int):
    data = _social_image_jpeg_payload(trend_id)
    return Response(content=data, media_type="image/jpeg", headers=_social_jpeg_headers(trend_id, len(data)), status_code=200)


@app.head("/social-image/{trend_id}.jpg")
def social_ai_image_jpg_head(trend_id: int):
    data = _social_image_jpeg_payload(trend_id)
    return Response(content=b"", media_type="image/jpeg", headers=_social_jpeg_headers(trend_id, len(data)), status_code=200)


@app.get("/api/social/generate-image/{trend_id}")
def generate_social_image_only(trend_id: int):
    """Generate/cache one AI visual only. Does NOT call Make and does NOT post to X."""
    ts = now_iso()
    with db() as c:
        row = c.execute("""
            SELECT
                t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.status,t.why_now,
                COALESCE(tt.traffic_potential,0) AS traffic_potential,
                COALESCE(cf.confidence_score,0) AS confidence_score
            FROM trends t
            LEFT JOIN traffic_totals tt ON tt.trend_id=t.id
            LEFT JOIN confidence_state cf ON cf.trend_id=t.id
            WHERE t.id=?
            LIMIT 1
        """, (trend_id,)).fetchone()

        if not row:
            raise HTTPException(404, "Trend not found")

        try:
            result = _ensure_social_ai_image(c, row, ts)
            c.commit()
        except Exception as exc:
            logger.exception("Manual AI image generation failed for trend_id=%s", trend_id)
            raise HTTPException(500, f"AI image generation failed: {str(exc)[:300]}")

    return {
        "ok": bool(result.get("ok")),
        "trend_id": trend_id,
        "keyword": row["keyword"],
        "posted_to_x": False,
        "sent_to_make": False,
        "image_url": result.get("image_url", ""),
        "cached": bool(result.get("cached")),
        "reason": result.get("reason", ""),
    }


@app.get("/api/social/buffer-status")
def social_buffer_status():
    return {
        "ok": True,
        "version": APP_VERSION,
        "buffer_api_key_configured": bool(BUFFER_API_KEY),
        "buffer_channel_id_configured": bool(BUFFER_CHANNEL_ID),
        "buffer_channel_id_suffix": BUFFER_CHANNEL_ID[-6:] if BUFFER_CHANNEL_ID else "",
        "mode": "direct-buffer-graphql",
    }



@app.get("/api/social/preview-image-post/{trend_id}")
def social_preview_image_post(trend_id: int):
    """Preview the exact X text/image without posting anything."""
    with db() as c:
        row = c.execute("""
            SELECT
                t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.status,t.why_now,
                COALESCE(tt.traffic_potential,0) AS traffic_potential,
                COALESCE(cf.confidence_score,0) AS confidence_score,
                si.trend_id AS image_exists,
                (
                    SELECT s.title
                    FROM sources s
                    WHERE s.trend_id=t.id
                      AND COALESCE(TRIM(s.title),'')<>''
                    ORDER BY
                      CASE WHEN COALESCE(TRIM(s.published_at),'')='' THEN 1 ELSE 0 END,
                      s.published_at DESC,
                      s.id DESC
                    LIMIT 1
                ) AS reason_title
            FROM trends t
            LEFT JOIN traffic_totals tt ON tt.trend_id=t.id
            LEFT JOIN confidence_state cf ON cf.trend_id=t.id
            LEFT JOIN social_images si ON si.trend_id=t.id
            WHERE t.id=?
            LIMIT 1
        """, (trend_id,)).fetchone()

    if not row:
        raise HTTPException(404, "Trend not found")

    image_url = _social_image_url(row["id"]) if row["image_exists"] else ""
    return {
        "ok": True,
        "version": APP_VERSION,
        "posted_to_x": False,
        "keyword": row["keyword"],
        "reason_title": row["reason_title"] or "",
        "post_text": _build_social_post_text(row),
        "image_url": image_url,
        "detail_url": _social_short_url(row["id"]),
    }


@app.get("/api/social/test-image-post/{trend_id}")
def social_test_image_post(trend_id: int):
    """V32.2 direct Buffer/X test using an already-generated cached image."""
    with db() as c:
        row = c.execute("""
            SELECT
                t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.status,t.why_now,
                COALESCE(tt.traffic_potential,0) AS traffic_potential,
                COALESCE(cf.confidence_score,0) AS confidence_score,
                si.trend_id AS image_exists,
                (
                    SELECT s.title
                    FROM sources s
                    WHERE s.trend_id=t.id
                      AND COALESCE(TRIM(s.title),'')<>''
                    ORDER BY
                      CASE WHEN COALESCE(TRIM(s.published_at),'')='' THEN 1 ELSE 0 END,
                      s.published_at DESC,
                      s.id DESC
                    LIMIT 1
                ) AS reason_title
            FROM trends t
            LEFT JOIN traffic_totals tt ON tt.trend_id=t.id
            LEFT JOIN confidence_state cf ON cf.trend_id=t.id
            LEFT JOIN social_images si ON si.trend_id=t.id
            WHERE t.id=?
            LIMIT 1
        """, (trend_id,)).fetchone()

    if not row:
        raise HTTPException(404, "Trend not found")

    if not row["image_exists"]:
        return {
            "ok": False,
            "version": APP_VERSION,
            "posted_to_x": False,
            "message": "No generated image exists for this trend.",
        }

    # The image already exists. Prebuild the compact JPEG before Buffer fetches it.
    _prewarm_social_jpeg(row["id"])

    image_url = _social_image_url(row["id"])
    detail_url = _social_short_url(row["id"])
    post_text = _build_social_post_text(row)

    result = _send_to_buffer_direct(post_text, image_url, "shareNow")
    return {
        "ok": bool(result.get("ok")),
        "version": APP_VERSION,
        "posted_to_x": bool(result.get("ok")),
        "make_used": False,
        "payload": {
            "keyword": row["keyword"],
            "detail_url": detail_url,
            "image_url": image_url,
            "post_text": post_text,
            "source": "buzz-now-v32.2-buffer-direct"
        },
        "buffer": result,
    }

@app.get("/api/social/image-status/{trend_id}")
def social_ai_image_status(trend_id: int):
    with db() as c:
        row = c.execute("""
            SELECT trend_id,mime_type,model,created_at
            FROM social_images WHERE trend_id=?
        """, (trend_id,)).fetchone()
    return {
        "trend_id": trend_id,
        "ready": bool(row),
        "image_url": _social_image_url(trend_id) if row else "",
        "model": row["model"] if row else "",
        "created_at": row["created_at"] if row else "",
    }


@app.get("/t/{trend_id}")
def social_short_link(trend_id: int):
    """Short mobile-safe URL for social posts; redirects to the canonical trend page."""
    with db() as c:
        row = c.execute("SELECT slug FROM trends WHERE id=?", (trend_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Trend not found")
    return RedirectResponse(url=_social_detail_url(row["slug"]), status_code=307)


@app.get("/api/social/candidates")
def social_candidates(limit: int = 10):
    """Read-only preview. This endpoint never posts to X."""
    with db() as c:
        rows = _social_candidate_rows(c, limit=limit)
        now_dt = datetime.now(timezone.utc)
        items = []
        for row in rows:
            allowed, reason = _social_post_allowed(c, row, now_dt)
            item = dict(row)
            item["eligible_now"] = allowed
            item["blocked_reason"] = None if allowed else reason
            item["post_text_preview"] = _build_social_post_text(row)
            items.append(item)
    return {"ok": True, "auto_enabled": SOCIAL_AUTO_ENABLED, "items": items}


@app.get("/api/social/history")
def social_history(limit: int = 20):
    limit = max(1, min(int(limit), 100))
    with db() as c:
        rows = c.execute("""
            SELECT keyword,pre_buzz_score,traffic_potential,make_status,posted_at
            FROM social_posts ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


@app.post("/api/collect-now")
def collect_now():
    result = collect_real_sources()
    with db() as c:
        states = c.execute("""
          SELECT source,last_status,last_message,last_count,last_run_at
          FROM collector_state ORDER BY source
        """).fetchall()
    return {
      "ok": result.get("total", 0) > 0,
      "result": result,
      "collectors": [dict(x) for x in states]
    }


@app.get("/api/collect-now-browser")
def collect_now_browser(background_tasks: BackgroundTasks):
    """Browser-friendly collection trigger. Returns immediately and runs collection in background."""
    background_tasks.add_task(collect_real_sources)
    return {
      "ok": True,
      "message": "Collection started in background. Wait about 30-60 seconds, then open /api/collector-debug to confirm.",
      "check_url": f"{SITE_URL}/api/collector-debug"
    }

@app.get("/api/collector-debug")
def collector_debug():
    with db() as c:
        states = c.execute("""
          SELECT source,last_status,last_message,last_count,last_run_at
          FROM collector_state ORDER BY source
        """).fetchall()
    return {
      "real_data_mode": REAL_DATA_MODE,
      "states": [dict(x) for x in states]
    }



@app.get("/api/v9/dashboard")
def api_v9_dashboard(limit: int = 50):
    limit = max(1, min(int(limit), 100))
    with db() as c:
        rows = c.execute("""
            SELECT
                t.keyword, t.slug, t.category, t.pre_buzz_score, t.buzz_score,
                t.acceleration, t.status,
                COALESCE(v.velocity_30m,0) AS velocity_30m,
                COALESCE(v.velocity_1h,0) AS velocity_1h,
                COALESCE(v.velocity_3h,0) AS velocity_3h,
                COALESCE(v.velocity_score,0) AS velocity_score,
                COALESCE(v.velocity_label,'観測開始') AS velocity_label,
                COALESCE(v.first_source,'') AS first_source,
                COALESCE(v.source_sequence,'') AS source_sequence,
                COALESCE(cf.confidence_score,0) AS confidence_score,
                COALESCE(cf.source_count,0) AS source_count
            FROM trends t
            LEFT JOIN v9_velocity_state v ON v.trend_id=t.id
            LEFT JOIN confidence_state cf ON cf.trend_id=t.id
            ORDER BY
                CASE COALESCE(v.velocity_label,'')
                    WHEN '急加速' THEN 4
                    WHEN '加速中' THEN 3
                    WHEN '上昇中' THEN 2
                    WHEN '観測中' THEN 1
                    ELSE 0
                END DESC,
                COALESCE(v.velocity_score,0) DESC,
                t.pre_buzz_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/v9/velocity-ranking")
def api_v9_velocity_ranking(limit: int = 50):
    limit = max(1, min(int(limit), 100))
    with db() as c:
        rows = c.execute("""
            SELECT
                t.keyword, t.slug, t.category, t.pre_buzz_score, t.buzz_score,
                t.acceleration, t.status,
                COALESCE(v.velocity_30m,0) AS velocity_30m,
                COALESCE(v.velocity_1h,0) AS velocity_1h,
                COALESCE(v.velocity_3h,0) AS velocity_3h,
                COALESCE(v.velocity_score,0) AS velocity_score,
                COALESCE(v.velocity_label,'観測開始') AS velocity_label,
                COALESCE(v.first_source,'') AS first_source,
                COALESCE(v.first_seen_at,'') AS first_seen_at,
                COALESCE(v.source_sequence,'') AS source_sequence
            FROM trends t
            LEFT JOIN v9_velocity_state v ON v.trend_id=t.id
            ORDER BY
                CASE WHEN v.trend_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                COALESCE(v.velocity_score,0) DESC,
                t.pre_buzz_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/trends/{slug}/v9-history")
def api_trend_v9_history(slug: str, limit: int = 200):
    limit = max(1, min(int(limit), 500))
    with db() as c:
        trend = c.execute("SELECT id, keyword, slug FROM trends WHERE slug=?", (slug,)).fetchone()
        if not trend:
            raise HTTPException(status_code=404, detail="trend not found")
        key = normalize_match_key(trend["keyword"])
        history = c.execute("""
            SELECT source, keyword, source_score, raw_metric, captured_at
            FROM v9_signal_history
            WHERE match_key=?
            ORDER BY captured_at DESC
            LIMIT ?
        """, (key, limit)).fetchall()
        velocity = c.execute("""
            SELECT * FROM v9_velocity_state WHERE trend_id=?
        """, (trend["id"],)).fetchone()
    return {
        "keyword": trend["keyword"],
        "velocity": dict(velocity) if velocity else None,
        "history": [dict(r) for r in history]
    }


@app.get("/api/collectors")
def collectors():
    with db() as c:
        rows=c.execute("""
          SELECT source,last_status,last_message,last_count,last_run_at
          FROM collector_state ORDER BY source
        """).fetchall()
        recent=c.execute("""
          SELECT source,keyword,source_score,raw_metric,source_url,collected_at
          FROM source_items ORDER BY id DESC LIMIT 30
        """).fetchall()
    return {
      "real_data_mode":REAL_DATA_MODE,
      "interval_minutes":REAL_DATA_INTERVAL_MINUTES,
      "collectors":[dict(r) for r in rows],
      "recent":[dict(r) for r in recent]
    }



@app.get("/api/trends/{slug}/confidence")
def trend_confidence(slug: str):
    with db() as c:
        t=c.execute("SELECT id,keyword FROM trends WHERE slug=?",(slug,)).fetchone()
        if not t:
            raise HTTPException(404,"Trend not found")
        state=c.execute("SELECT * FROM confidence_state WHERE trend_id=?",(t["id"],)).fetchone()
        key=normalize_match_key(t["keyword"])
        items=c.execute("""
          SELECT source,keyword,source_score,raw_metric,source_url,collected_at
          FROM source_items ORDER BY id DESC
        """).fetchall()
        matched=[dict(x) for x in items if normalize_match_key(x["keyword"])==key]
    return {
      "keyword":t["keyword"],
      "state":dict(state) if state else None,
      "sources":matched[:20]
    }



@app.get("/api/trends/{slug}/propagation")
def trend_propagation(slug: str):
    with db() as c:
        t=c.execute("SELECT id,keyword FROM trends WHERE slug=?",(slug,)).fetchone()
        if not t:
            raise HTTPException(404,"Trend not found")
        v=c.execute("SELECT * FROM v9_velocity_state WHERE trend_id=?",(t["id"],)).fetchone()
        p=c.execute("SELECT propagation_minutes FROM propagation_state WHERE trend_id=?",(t["id"],)).fetchone()
        key=normalize_match_key(t["keyword"])
        rows=c.execute("""
          SELECT source,source_score,raw_metric,captured_at
          FROM source_snapshots
          WHERE match_key=?
          ORDER BY captured_at ASC,id ASC
          LIMIT 300
        """,(key,)).fetchall()

        state=dict(v) if v else None
        if state is not None:
            state["propagation_minutes"] = p["propagation_minutes"] if p else None
    return {
      "keyword":t["keyword"],
      "state":state,
      "timeline":[dict(x) for x in rows]
    }


@app.get("/api/velocity-ranking")
def velocity_ranking(limit: int = 50):
    """V14: dashboard velocity endpoint backed by REAL V9/V11 velocity state.

    The old endpoint read propagation_state, while the active composite engine writes
    30m/1h/3h movement into v9_velocity_state. That mismatch made the BUZZ VELOCITY
    panel look frozen at +0.0/h even when the real velocity engine had non-zero data.
    """
    limit=max(1,min(limit,100))
    with db() as c:
        rows=c.execute("""
          SELECT
            t.keyword,t.slug,t.category,t.status,
            t.pre_buzz_score,t.buzz_score,t.acceleration,
            COALESCE(cs.confidence_score,0) AS confidence_score,
            COALESCE(cs.confidence_label,'デモ/未確認') AS confidence_label,
            COALESCE(cs.source_count,0) AS source_count,
            COALESCE(v.first_source,'') AS first_source,
            COALESCE(v.source_sequence,'') AS source_sequence,
            p.propagation_minutes AS propagation_minutes,
            COALESCE(v.velocity_30m,0) AS velocity_30m,
            COALESCE(v.velocity_1h,0) AS velocity_1h,
            COALESCE(v.velocity_3h,0) AS velocity_3h,
            COALESCE(v.velocity_score,0) AS velocity_score,
            COALESCE(v.velocity_label,'観測開始') AS velocity_label
          FROM trends t
          LEFT JOIN confidence_state cs ON cs.trend_id=t.id
          LEFT JOIN v9_velocity_state v ON v.trend_id=t.id
          LEFT JOIN propagation_state p ON p.trend_id=t.id
          ORDER BY
            COALESCE(v.velocity_30m,0) DESC,
            COALESCE(v.velocity_1h,0) DESC,
            COALESCE(v.velocity_3h,0) DESC,
            COALESCE(v.velocity_score,0) DESC,
            confidence_score DESC
          LIMIT ?
        """,(limit,)).fetchall()
    return {"items":[dict(x) for x in rows]}



@app.get("/api/monetize-ranking")
def monetize_ranking(limit: int = 50):
    limit=max(1,min(limit,100))
    with db() as c:
        rows=c.execute("""
          SELECT t.keyword,t.slug,t.category,t.status,
                 COALESCE(ms.monetize_score,0) AS monetize_score,
                 COALESCE(ms.monetize_grade,'C') AS monetize_grade,
                 COALESCE(ms.intent_category,'general') AS intent_category,
                 COALESCE(ms.recommended_mode,'adsense') AS recommended_mode,
                 COALESCE(tt.traffic_potential,0) AS traffic_potential,
                 COALESCE(cs.confidence_score,0) AS confidence_score
          FROM trends t
          LEFT JOIN monetization_state ms ON ms.trend_id=t.id
          LEFT JOIN traffic_totals tt ON tt.trend_id=t.id
          LEFT JOIN confidence_state cs ON cs.trend_id=t.id
          ORDER BY monetize_score DESC,traffic_potential DESC
          LIMIT ?
        """,(limit,)).fetchall()
    return {"items":[dict(x) for x in rows]}

@app.get("/api/trends/{slug}/monetization")
def trend_monetization(slug: str):
    with db() as c:
        t=c.execute("SELECT id,keyword FROM trends WHERE slug=?",(slug,)).fetchone()
        if not t:
            raise HTTPException(404,"Trend not found")
        s=c.execute("SELECT * FROM monetization_state WHERE trend_id=?",(t["id"],)).fetchone()
    return {
      "keyword":t["keyword"],
      "state":dict(s) if s else None,
      "adsense_enabled":ADSENSE_ENABLED,
      "affiliate_enabled":AFFILIATE_ENABLED,
      "affiliate_provider":AFFILIATE_PROVIDER,
      "pr_label":PR_LABEL
    }



@app.get("/health")
def health():
    return {"ok": True, "service": SITE_NAME, "version": APP_VERSION, "environment": ENVIRONMENT}

@app.get("/ready")
def ready():
    try:
        with db() as c:
            c.execute("SELECT 1").fetchone()
        return {"ready": True, "database": "ok", "real_data_mode": REAL_DATA_MODE, "demo_mode": DEMO_MODE}
    except Exception:
        logger.exception("Readiness check failed")
        raise HTTPException(status_code=503, detail="database unavailable")

@app.get("/api/runtime")
def runtime_info():
    return {
        "site": SITE_NAME, "site_url": SITE_URL, "version": APP_VERSION,
        "environment": ENVIRONMENT, "demo_mode": DEMO_MODE,
        "real_data_mode": REAL_DATA_MODE,
        "real_data_interval_minutes": REAL_DATA_INTERVAL_MINUTES,
        "adsense_enabled": ADSENSE_ENABLED, "affiliate_enabled": AFFILIATE_ENABLED
    }


@app.get("/sitemap.xml", response_class=PlainTextResponse)
def sitemap():
    with db() as c:
        rows = c.execute("""
            SELECT slug,updated_at FROM trends
            WHERE is_indexable=1
            ORDER BY updated_at DESC
        """).fetchall()

    # Google向けに sitemap 内のURLをASCII形式へ正規化。
    # 日本語スラッグはUTF-8でpercent-encodeし、XML特殊文字もescapeする。
    home_url = xml_escape(f"{SITE_URL}/")
    urls = [f"""  <url>
    <loc>{home_url}</loc>
  </url>"""]

    for r in rows:
        raw_slug = str(r["slug"] or "").strip()
        if not raw_slug:
            continue

        encoded_slug = quote(raw_slug, safe="-._~")
        loc = xml_escape(f"{SITE_URL}/trend/{encoded_slug}")

        updated_at = str(r["updated_at"] or "")
        lastmod = updated_at[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", updated_at) else ""

        if lastmod:
            urls.append(f"""  <url>
    <loc>{loc}</loc>
    <lastmod>{lastmod}</lastmod>
  </url>""")
        else:
            urls.append(f"""  <url>
    <loc>{loc}</loc>
  </url>""")

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
%s
</urlset>
""" % "\n".join(urls)

    return PlainTextResponse(
        content=xml,
        media_type="application/xml",
        headers={
            "Cache-Control": "public, max-age=300",
            "X-Robots-Tag": "noindex",
        },
    )


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots():
    return f"""User-agent: *
Allow: /
Sitemap: {SITE_URL}/sitemap.xml
"""


@app.get("/api/trends")
def api_trends(limit: int = 50):
    limit = max(1, min(limit, 100))
    with db() as c:
        rows = c.execute("""
            SELECT keyword,slug,category,pre_buzz_score,buzz_score,acceleration,status,updated_at
            FROM trends
            ORDER BY pre_buzz_score DESC, acceleration DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return {"items":[dict(r) for r in rows]}


@app.post("/api/trends")
def create_or_update_trend(
    keyword: str = Form(...),
    summary: str = Form(""),
    why_now: str = Form(""),
    category: str = Form("総合"),
    pre_buzz_score: float = Form(0),
    buzz_score: float = Form(0),
    acceleration: float = Form(0),
    status: str = Form("🌱 前兆"),
):
    """
    V1の入口。
    将来はここをトレンド収集エンジンから自動で呼ぶ。
    """
    slug = slugify(keyword)
    ts = now_iso()
    with db() as c:
        existing = c.execute(
            "SELECT id,keyword,slug FROM trends WHERE keyword=? OR slug=? ORDER BY CASE WHEN keyword=? THEN 0 ELSE 1 END LIMIT 1",
            (keyword, slug, keyword)
        ).fetchone()
        if existing:
            # Keep the canonical slug already stored when another spelling
            # normalizes to the same slug. This avoids UNIQUE(slug) failures.
            canonical_slug = existing["slug"]
            c.execute("""
                UPDATE trends SET
                    summary=?,why_now=?,category=?,
                    pre_buzz_score=?,buzz_score=?,acceleration=?,
                    status=?,updated_at=?
                WHERE id=?
            """, (
                summary, why_now, category,
                pre_buzz_score, buzz_score, acceleration,
                status, ts, existing["id"]
            ))
            slug = canonical_slug
        else:
            c.execute("""
                INSERT INTO trends(
                    keyword,slug,summary,why_now,category,
                    pre_buzz_score,buzz_score,acceleration,status,
                    first_detected_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """, (
                keyword, slug, summary, why_now, category,
                pre_buzz_score, buzz_score, acceleration,
                status, ts, ts
            ))
    return RedirectResponse(url=f"/trend/{slug}", status_code=303)


@app.get("/google9854439bbecd0905.html", response_class=PlainTextResponse)
def google_site_verification():
    return "google-site-verification: google9854439bbecd0905.html"
