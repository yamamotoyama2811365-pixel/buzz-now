
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
SITE_NAME = os.getenv("SITE_NAME", "BUZZ NOW")

# Production runtime settings
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
APP_VERSION = os.getenv("APP_VERSION", "35.10.1")
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
        c.execute("""
          UPDATE trends
          SET pre_buzz_score=?,buzz_score=?,acceleration=?,status=?,updated_at=?
          WHERE id=?
        """,(round(new_pre,1),round(new_buzz,1),round(new_acc,2),
             classify(new_pre,new_buzz,new_acc),ts,row["id"]))
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
        "総合",round(pre,1),round(buzz,1),round(acceleration,2),status,ts,ts
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
    if title:
        # Remove common publisher suffixes and the keyword itself to avoid a clumsy repeat.
        title = re.sub(r"\s*[|｜]\s*[^|｜]{1,40}$", "", title).strip()
        title = re.sub(r"\s*[-–—]\s*[^-–—]{1,35}$", "", title).strip()
        title = title.replace(keyword, "").strip(" 　「」『』:：-–—|｜")
        title = re.sub(r"\s+", " ", title)
        if len(title) > 42:
            title = title[:42].rstrip("、。・ ") + "…"
        if title:
            return f"{title}が要因か。"

    # No sufficiently relevant collected article: do not invent a cause.
    return "関連報道の増加が要因か。"


def _build_social_post_text(row) -> str:
    keyword = str(row["keyword"]).strip()
    pre = int(round(float(row["pre_buzz_score"] or 0)))
    traffic = int(round(float(row["traffic_potential"] or 0)))
    status = str(row["status"] or "急上昇")
    status_plain = re.sub(r"^[^ぁ-んァ-ヶ一-龠A-Za-z0-9]+\s*", "", status).strip() or "急上昇"
    detail_url = _social_short_url(row["id"])
    reason = _social_reason_from_row(row, keyword)
    return (
        f"🚨 BUZZNOW SNS捜査官｜{status_plain}を検知\n"
        f"いま「{keyword}」がバズり中。\n"
        f"{reason}\n"
        f"シグナル上昇 / Pre-Buzz：{pre} / Traffic：{traffic}\n"
        f"なぜ今話題？ → {detail_url}"
    )


def _social_candidate_rows(c, limit: int = 20):
    limit = max(1, min(int(limit), 100))
    return c.execute("""
        SELECT
            t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.buzz_score,t.acceleration,
            t.status,t.why_now,t.updated_at,
            COALESCE(tt.traffic_potential,0) AS traffic_potential,
            COALESCE(cf.confidence_score,0) AS confidence_score,
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
        WHERE t.is_indexable=1
          AND t.pre_buzz_score>=?
          AND COALESCE(tt.traffic_potential,0)>=?
          AND COALESCE(cf.confidence_score,0)>=?
          AND t.status NOT LIKE '%%下降%%'
        ORDER BY
          t.pre_buzz_score DESC,
          COALESCE(tt.traffic_potential,0) DESC,
          COALESCE(cf.confidence_score,0) DESC,
          t.updated_at DESC
        LIMIT ?
    """, (SOCIAL_MIN_PREBUZZ, SOCIAL_MIN_TRAFFIC, SOCIAL_MIN_CONFIDENCE, limit)).fetchall()


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
    if not SOCIAL_AUTO_ENABLED or not MAKE_WEBHOOK_URL:
        return result

    now_dt = _parse_iso_datetime(ts) or datetime.now(timezone.utc)
    candidates = _social_candidate_rows(c, limit=30)

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
            image_result = _ensure_social_ai_image(c, row, ts)
        except Exception as image_exc:
            logger.exception("V30.5 AI social image failed for %s", row["keyword"])
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


def collect_real_sources():
    ts=now_iso()
    with db() as c:
        g=collect_google_trends(c,ts)
        w=collect_wikimedia(c,ts)
        news_count=collect_fast_news(c,ts,limit=6)
        refresh_confidence(c,ts)
        refresh_propagation(c,ts)
        refresh_monetization(c,ts)
        snapshot_v9_sources(c, ts)
        refresh_v9_velocity(c, ts)
        # V13: refresh Traffic Potential and today's predicted PV from real signals.
        refresh_real_traffic_forecast(c, ts)
        # V15: answer-check forecasts against the newly collected state, then
        # create fresh +3h forecasts. This now runs in REAL_DATA_MODE too.
        evaluate_predictions(c, ts)
        create_predictions(c, ts)
        cautiously_tune_model(c)
        # V30: after all real-data scores are refreshed, publish at most the
        # configured number of qualifying topics to Make -> Buffer -> X.
        social_result = auto_post_social(c, ts)
        c.commit()
    return {
        "google_trends": g,
        "wikimedia": w,
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
              COALESCE(ps.velocity_3h,0) AS velocity_3h
            FROM trends t
            LEFT JOIN traffic_totals x ON x.trend_id=t.id
            LEFT JOIN confidence_state cs ON cs.trend_id=t.id
            LEFT JOIN propagation_state ps ON ps.trend_id=t.id
            ORDER BY traffic_potential DESC, confidence_score DESC, pageviews DESC
            LIMIT ?
        """,(limit,)).fetchall()
    return {"items":[dict(r) for r in rows]}


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


_BUZZ_NOW_APPROVED_OVERLAY_B64 = "iVBORw0KGgoAAAANSUhEUgAABLAAAAKjCAYAAAANs/bAAAEAAElEQVR42uy9d7xtR1n//35m1trltNuT3BQCJCBdIQFsgNJERRQwioCKPxQRpHdEIYgNEUXwawFREQEJHSl2Qg1VioSEFEi9ye33nrb3Xmvm+f0xs8o5SUgRNITnDYd7yt5rzZqZtV+v9eHzfB4wDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwDMMwjJstYlNgGHZvGt9RqE2BYRiGYRiGYRiGYRiGYRiGYRiGYRiGYXwTMZeHYdz82DGA7TOIN/0QQ2Ca/yV/f2Pf03/f8AYco//+6f/idF3feDeP69pe/78xvulN/Ps3awxTgABcAVR2mxmGYRiGYRiG8e2ECViGcfPBA8F7+V0V91QNYbUUfHSSblXNN6yAqOBEkKYaTAQBREFQFND8b2xv9vSaqNq8Jf1Wu2MAuN4ng9ekeKiAqrZ/EFViGkivIE2aQ+RzSz4rvdGktzTH765g43FoX90cr/nthpfkd0v7mv5rAVTT2NPre2drrlW1e7E085zHLtodpPdxKTRzkY4t1/aBKqB5blW7eWkWUPpX1j+8SnudzTqKbP6Y1na92x97MxDyVTpVXHM0VVUnTrwcXJvUDwa+tnlKDcMwDMMwDMMwbs4UNgWGcfMiwui2yPyv+sW5napSZ5FEnCCiOJLI5AV8aDQRQZzDRxBiEq5UCaJEQFU6ncQ33zicgENxWWhBFK+CuvQSr0IEArEVpCCm/0oWh0IrFyVFxKURJOEsCy2qoIprhKukweBUiK5RUfoqjxJUW1GtL1NF+hJQxMf0nuCaEQAx4kjiW5DeAVSTvCMOkSTQtfMiaY6SWNcoU1l4kzTHjm78qBLz75s1QGNzmiRgCURRXOzER5BWWGrFvUbYcg5FcY0QKc21CjiQmOfAp2OIklc7r4w4ohOISkkSsGIIDFHe7Sr+MdSyNBy6o9Op3WiGYRiGYRiGYXxbYQKWYdw8ECCeDKNL0GPvGR3P9HOx1uCKAKIRJ33DTKPIdKJPbI6iybnj28MqG4w2SQEiIq2ghCZRp3UbNQfT7nyx06nSy/P7QysCJaFH6/T9Rt9SZ9ZKmk/n3vJRiPRVpoS7xhRpfo/rhti+SyAKKrE9V+Nu6rxfaXwCSLzm+XpnofV2qbYuK8lvds18QZqzLKpl3Shdu262Nmnvn75Trfk5iV+NGLZxZMkRJv05qWXDcmp2b+EEL+0C5UF5cKKfD0GmSL1o95phGIZhGIZhGN+GmIBlGDcPBIh7Bv7BRH3UDvV6KNZuhSilCE5iVyLYCFkbqu5y0ZlmsSqXGXbiVd/DpIgKIjG/Jp9eA235XuzK43SDGJReq02Rm7aeLJpyRCES8zhV01+lV+wn0h2v9Vf1xbNeWaLrVbmlYeTjb5i2rmRRVLvrAyCiIllYIpdL9sQ/0bZSsDtVcpCJdjKcNFeQ56L1UGlMxqve9cReWWeuAaQtTZTm/QmVTp5KL09CpbalhFkUzGvYrWkjenXrgMTkyiLNkWgS9UpgHTgnqBRDNyBY6bhhGIZhGIZhGN9+mIBlGDcjPL5AK3e887EAKYFhqstr3TcqisP3RBxNxWJt6Zv2HD4xizlNppNr3TkirqdkSOdU0mQzirkcLhmPXJsNpVlw0sadpK4XIyVdQVtrEPOdENYPbsourq6u0LXaUiMIuVxCp70QLM2lf/koWchptKdGNIr5mn17vbG5tg2hVT1RT66ZZqXE9vjShHc149R+5pXkvC1NzjfpueNanaknPrXv01Z0FPFcI5LKdT602DufiuLyONoxSnZ2NXloWSgbAYdE9UqPxKCvG05nV2D5V4ZhGIZhGIZhfJthApZh3IwIBACOy44k1w8SF9cGi+umWHMhZpGll60kmhxHsjEzSlslrPkne32k+bepPkvlcdIrS4QuYr3NtmoEFLqYc5XOudSQD0erUolu+HtfEWr1n7ixZFIaQUs2pmZ14lDj5pJujLLJqdYLSlfthB7R2HqjOuGuG9eGEPp+on57pthme6W3xe7kjVuq5z5r1rXNzyJ0M5gFqWYw2ssPawamdCWHIr2srpTZnkcUEXUsEzniFEQ+fnkyZPl8QsMwDMMwDMMwjG8LTMC6Hl4Mjhe/+Ca888zmvzeUTUFF10s/AOnGEP+Xjn9T3//NZFO92M1iLN+QWUCcwrHiiJrL27QpB4y5lK0pcdMucJxeuZ1oW26IdqnpbSkg/d6APREsh6aLU1RdLpnbmLuVSvBcWzaXOuxpFmJkwwVK2ymwPwV5azRijsZ2LIJDsxDjmq6LAiFqFr+05yST7ADTLBrFXi1joB8ev8H81PyTOwPKRltYJ5ypbujsl/KwGpea9jaU9kobm7noSjwFRV1T2LehP+LGKdlUnIlIKz5uaL0oqUSxybdvrh9VmlgxVNuOkY1n7qqorHnR0sVhsO6zhmEYhmEYhmF8G2IC1vVwJkTOPPN/cz0iN0xkurGC17dEcPk/HN+361iuj+k259iFJ2pExeXBaw4Pb4w50nWok84H1Fxql5+UxaGs4qQ+eo3wlN8nWTbJ1iuJ0uuAB7FNS2/KApNAomjKa8pjyRnkyf3TlN31yxJJziLJr9/gDJO2USEiQtSYHGDicI58Pto6xKid40xks2DTS/xqlZ7mHK7zTGkW/5qJzTQ5WdpMSSv8ufZQrXcslw2mS+h1LtReV0M6Z1rjQFPpAuLbo+WDu0Ym043a6wbvXa/csdkHSuyJV0oUTQH7GvV8DdROZAR+YqWDhmEYhmEYhmF8G2IC1jfgRBiPTtr9g8vLh0chBPV+CIAvoPAe78GHNIUBCKGmrmfMAtRhytpaYHYDzrNtcU7vdo97nn322Wev0D7Ow3WILs3z+cnA3YH6hq710ri44uh6/ekb8uIxHL8Op3PDHVvFqCiuntT1OXl8u4F73oj3fzNRoBwMBl+ZzWbnA8dz467lWzGWL89mswu47uwhPQMGZ4medqx4tihUiGjjtmqypUSSM8s7xHlQwef2d0rIgpJ2alIMbTB5T3HpMrJEe5lVmkvvaNPW1XlEYutAasSXtlot51RpDO2x+gJSPw+rE6t6OVCSOgeqgniPcx5E8E2rQ1Ktm8Zc4uggRsWLQ2OkrisExTnphDM2ikgbbh1RYkzniggiLueBaVf+l7O/NCtqojG5nmJzTU3m1qY8rdY95RDvcwmo6/nOhKA9gTHWbakfm4UsdMMa0WRcIXhx+TjSlmWmYkWXcss0Qh5ziIoK+jXBVRKvmBe9+Bt8thiGYRiGYRiGYZiA9W2GALo8HB57wvatb3rwA+6/c+u27YS6RgScCEVZ4pzDieBwRJS6qphVFdNQU9U1q2trHF5eZmV9QtCkcqkq0/U1lldWWFtbY3n5KMuHDnP22We/Gbh828LCew6trHz0WgSrBpf1sh/9nrvd9c9vc9vbMq0qSu9yyLO0D7vpAVoofMHnP/8ZLrn0sg8AP3Y9ApkHwtS5H/6eu9zpjbc55VRClTutCenUMTlgNJ/PlQWf/8ynuWLPng9zv/s9gLPPrr3nB+58hzuddYc734XpdJpPFiEK4hsnSs4citnRkwULzZaf1nXTFz+ctNlMMSYXjro0aIcDJ8So+rnPfkau3rf/d4AXOcd9TzrxxDff4x6nt+6UkM/ZhIh3JXEbS8BiI0uEiGpM30cQl8QP71wWYZpOdb2OdCI4ET3/K+fKBRdfdCbwkt76XWO/fWQ02k2snny8OsYgIQsymp05UZouf8nuJFVAnEdjxJHCw2NTkucchAqRAs3KnUoSeVz+vtWI2nB06QWnJyEnhjo5rrJYFXNZm2p2RwkQA865XKyWRKamg1/bNVG6asauek7THJaDJPSEgE5XibGiasolnSO4dH9JrURqFMFTMihHFMNhsizW0y5aiyZ+SjY4ofrNAWOo0ryIS2PMDjdH6uRHTPeOaI2Kw7skeEVpSgl7mVjSdX90jbtM67RmMbRZWVHTWgqxLbbUXJK5Qe5rFURQl9cVIFYgeb0lgrrULZKI5JJTT0Rc3i8KhUDQqFeJsq767rWV+uP5hNE+5g3DMAzDMAzDMAHrFsKR6VQX5ufrX3vqM/S77nRnXV1ZEd90BXMOwSFe2rKoGCMxhvSvRuo6EjTivccXZXLHRGUyWWd1bYWV5VUOHNjPuV8+l0+c8/Gf+/jHP84ll1z2WBH+bdeuY16xd+/e8+A6TVzx5x/z2PiUpz89Hjp02JeDEucEcb4LqI7JOTMoy/i4x/2iXHjxJdfr1jrjjDM466yziDHqI37q4fG5L/wNXV1ddUVRIq4XXaSRUAcUWFpaio/62Z+RS9/xzvDil7yEM3/4hwmB+BMP+0k987d/Ox5dXhbnnCRBxyXXCKl8S7URq2IWsWIvd0l6QdpZHOoFa2t+0G9/l7vkFUURfuKhP+6uvOrs9Nwf0dNPv1d8w9+/UeuqkqgqvvBtKV7bpS7SZks1EkuMvUivqKjTNpS7Wfc2r8hJ69BRjRCV+fn58Bu/8SL3h3/48hsWmC0abqXCvKakbdcoL+21OrxWRK2od+7Ai6A1BA+xdElC0YjWgXpQMjh8lME0OXhcDhXv9Lm25V+vYV4qRhNVoguwdZHgXRbyfAqAd4KLDtFAVCVQMTg6wVdJ4GnL5LimaJWlnOSyGgzwGomrK0RmxNEScuptKE45AXfC8fjjT8Dt3IWORygRXV4jXnU19WWXEK64kvXzL0X2XckwCuVwnlAKdV0lIanffbDNrkpuK+opjArc4jylFknYKwvwPrnIskgdNTIde9zyOsXVh3Gi+MYR1zqlpM3sEoGoNREP4wHOFWk2Xeow6FRwIRJcJFQ1fnWGb8SrrJu2mWI56Sx9ytRMo6NeWsSrpvvapX3mUCQLmIVAPZuh0ylDV6ZxRaF2ymEHpSvcjNnNIQ/OMAzDMAzDMAzDBKxvNhGV0dw85XDEKEZ84WlyaZIYI7heq/v0pkBQRWN+EC1dflBt2Lrh5Q94wAPlV5/0xPjV8y+IZ731Tbtf+9rX//yePVc9YseWxT89cGT5N7n2bmHiCufKwSCO5kYMygHeuyyqpAd1DYHY9CJTdTf2wVWdc8PhMIQYGZRlEiVi51SqXUUdAkVRMByO0/E/9KH2/d6XeO8ZDkqc9xS+oCiKHKqU5ia0okYWpLQJ7N401NaGkl7taNQdIcb03hiVGOrcTU82XG9RlG5ubhxW1yKlOIaDQRIpshmljUHqnTZqbK0qCnikHUcEnFNidHksDmJsh+iAWiOFL3De36C5F1AncBvxDBTW6DrkNZKQF0eIU/z3n8bSH78C6hnUHgYFWiRXlGikrirctu2s/v0/UP3eHzMsFlK0uaaQ99gTr6RX2acoUpbo+gGKBz2YuRc9D60D6gfIYJDsWU6QqFBNiMMhHLiSI09/Hu78q5HhCK1n2XTk2jLDJr/JqVKUAwoiYfUA9Xge7nkaxQ99H4Mf+F78He6IO2YbLIyhnMsz2SfA+jq6skb9tUupPvEJ4r+cjX70Y7ijRxnNLxDqQAgRzeWWaP5eBCkcoV5l6Qm/zvixj0WW18ELDEfEokBdcmGJ8ylDanHM5B3vZvU3fp+x+hzWHluXY3KZJTHT6Yy1Utjywucz+sF7opMZUgzARQSfwvWnE8LSIvXnP8/h576UcpLELW13mWbvWVofL5FJPWP4y7/C1sf9AnL0cNoVPgunOaTMzSogoOMR07e9nbU//xsKP6RAOFw4DpWRUogzKx00DMMwDMMwDMMErFuogFUrIVSNwUdiTHYfJ9k1FBsXUXqI1aitmyjGmMqTokvKRJtjk0qxUqlTJKriBHenO97RnXnm7+jDHvZwnv6Up85/7BOfeP62LYvx0JHll5GcWLpRJwsAEkIQCohBs3sj5efEGNI4IKcY3QDOOuuax6+jRNEk6LjOsaRKqlMCKcprbqVQJ7dHiCoiKuqUUNe9MO6eWNb7RqN24kDjt4rd65xKcgI1Tp8svDRuKY1RNGyskJpOJ0zX1yXWQShEYnayZItTCrtuLzy21xhay5mknKHQa22YvSwRUk5SJM99Ekuq2Qw3FIl6w6q1DsQ4Wizwtw4On0v5HJ30FXNHO4j4O94Jf6/vJcaYLsFvFHp8VaEIW57yZA6895/R//4qbjAm1LNcLNeVZzbKXUrcSoJspMbf8+74+94HDTn8KQu1raA2neGHA6ovf4F6OsEXBepS5pOXbr2akHVxnkExQNaOUhUO/5CHMve4x+J/6D64Y3e1ZX4xBLSukTBLY4nd4isKboBsHVHc6xjKe50Oj3884WOfYO1P/ozZB/+F0XgeEWE2q3JZYyr7iw5kMEInEd2+DXfPe6Lr6zAYpPwtNtXWzmoGg4Jwh4uQ0SC9z3kIdQ6A73oDlEXJbHIYudf3M37Kk2DrIk572WW9YxcAC0N04IiziHqHRJAQsyCmqSxWhFjXVOMFFh7zGAbfd882c4zNHwZVSOKb96y86/1pbFIw1JrLnHC4gGGM5SpwBnCWfbQbhmEYhmEYhmEC1i0L7T16+qKg8GVXbtX+2zPXuJwPFCG6mB0v+cU5MyuJMLnEyjkkKiHUzNbXiTHKaaedzj++7W36M2f8tH7i45/4jZ1bFi7Zf2TltST/T7d4Pi1f6QvKskw5QtKNOGp6+JeolN7fwCvuHm+9y8cvCoqyaEUnSWYSBKXKAtBgUF5T/MuvL7L7ynufspIa8aonYqXOdNp1a9vwgK4506h7bZOr1GlOikhsVMaUs9Vfx5gO4IsS7xzeF7nTnlzDGqXOZeWlp2r1lI0No5buX+fpQqVUUV/gnKfY7NC7DgZz5fO3BdlxUiUqGsVnAauRSRzNzxE57gQIEZaXk6hTlK2IpXVAYiCGgD/uOMZPeTKrT3gS83HETFybXSY5BEukK31sZ148/vgTU/3l6mq2h7lOhFUlrk/wxVaq8y5E9+xHyiVCzOJpWwGahL+i8DgP9dp+3O3vyvC5T2P4yIehW5dSGeJsioQI5K6DziH5fOI153Xl8s6oSAjEegbOI3Nj3I88gIXvvTfLf/ZXHPrt32dLqHBlQahD26Ev+/ZwCPGKPVDV6PoaTGfoYJAzswLERsCq0C1bkNIjhU+lwUW6dtfuC0l5aE6pxDF+yENxWxeZHT6MK4rkznTSBcDHiMyN0ejAlahOCeKT66sXji8i4D2zakZ5h9MZ3PXOSRCdzZJQ6n06b85no5rBaEy48EtM//4fGfoFquwQvcwjR2HmIl8n3d3mwjIMwzAMwzAM49sOZ1PwjYlRrz3yXHSDzKW93CSRFFQuTtrSpcZ11HY2k/Q47URwzjMYDJmbm2M8HnHw4AGOO+44ee3r/lpPPOkkDh9d/emdc3O7aZqNtasnrVAUY2xDpduhNnFNXvBleQP1q543Q/pCVERdPoNK7oaXqwEhlQZuIvRcUFG1zbiKMTvUmtwfIEqXsB3zL1s5RXtz3AsAT/PZdIuLrZjonGzqPpfeETXkUq2cD6Yb16QLktdcJtmJR+33m4PI8/lj49raIHtKFgC/4dy3AxWRk48V73aLaNCAR+mi0TU5slxEiZR3uRPiHW5ujFtcwC3M4cZjZDTGLcwjS0v4LVvQ6YS5R/4kfM/3EKpVpBgmN6Bu0uU0fRh4BIkzZDjG3/rkJLrOjXFzc8h4nL6GIxiNkMV5xDv0skvw07XszstbNJe3RSJSlojWTNZX4Ocex/w/ncXw8Y9FRwPi8iquqlJ222iIjIbEcgCDEvU+dffEEcQTxaOuQIuSWA7SGMsBUgc4soyOBiy98Jksvu61HB6N0VhRlmUbmO5E8TFQUOIPHoS6hsUl3HwzdyNkNIbxOF3f3Bi8IOMB6tP+CZJyvpoSTCfgfUGsprgTTmbhJ38MYqQcjfBz82m+RmPceISMR8igBHH4nTtwu7aioUqOvSYzLO+xkLOzampGP/YQ3LZFCo248Rg3P48bj3CjUfp3bgTD9P30H99FeWgfcTxHTLKYXu3EraHn7T86ec21fJIZhmEYhmEYhmGYgHVLoK5D1x2PXoc5mixkycHLQgiBqqqZzWZMp1OqqqaqK+qqoq5rqqoihHS85t9G1NGoaEixzPMLi6ysrnDHO97RPfHJT9Za9cHrIdwB0NN6gofbnBMlScCRNp+HPFaHlxu41Ged0Tte44eKbfC3yy3qOgOKbBBrNop/IYsj2gpQMebSycb5k7vEdZ3XJNu7ZGOXt2662xnQTf/bOlfa422QGFN5peZOfbJxvlQ60aX5XTeX+bhZYZDeGDYZwdrRNP/prv/678WoWh8Tle0Rqia7qY1ab8o2A1IuUpxy21yel1OyVFr9LDaSlzh0VuG2b2XhSb/KGhUqnso5Yi4BVXFtOaQSUS9oqGHndtzxx6XxS87Wymvt8hJFV6TSzcv3pM596vKYIXpPLQ5fDCgmq6xLZPg7v8PS61+Nu+2JxLV1cA4/N4aiwBWexl8mLpXixhzKHzWXT2bhqNkqmr+kcMj8HGgkrKyw+JifZMtf/jnrzlNWUwZZLPaqFEQKN0CvuhpWV1PeF+Tw9uTTiioEbfKzFBnPoYOCEANBPLWmez7H9TN0SqhWGDzghyju9F3E6TQ1echCqqp2XSrFQYjI4iJy3A4iqYOjxJRPJlnB8uKQaops3cXwIQ/MinBMnRGzON7sZ6ISh0P0qj3M3vFPlMwhIeKiEsRzNUqlGndDbZ/ohmEYhmEYhmGYgHULZTarCEGvIRRpL7SpM/s0glbnyGo71ZEcSevr68ymMzRCHSIxJFEnBCXkHOfCeQZFSVVV8tOPeDi7jztWV6fTO93vfvcrPgtRVTslga5CcdMQ2186rs2RdP1Iv5RPG7HkusSY9PsP9X5TVTVEqKqKOlTMZlUS92YzqllFVc+o6pq6rgkhEuqauqoIIf2urus2/0iQDePp5nizgqQU3l3jekNQqllNXaXjVnVFNZsxy191lYTGalYzm3XnDyG0X30hqi1hzCWi0rZ+1Bwqr4SoOez9Bs19DMT62JA6EIbGmZavKS23gzrgdh6D27adLoEpiXLNnEjMzrgQcUVJXF9j/FM/jrv39xHW91FKQZCYnIEbugPmgKVQ4XdtR7Zt7cl/TU5Wqg8UspgymcKeq3CpdhaNSsjipRcH1SrVQFh61Z+w9IKno7Eirk1SIPwglT2Kc/SXUnI+nDTCY9MZssle6zncFFI2V+GRwQDnPHF5hYWf+ymWXv4yJrqCk4h3jkIVHypc6dGDB4krK929omxw3KUstkgMETccwbAgEAniCL15dzioK+JoxNzDfxLKAmmy2LJDsBczlqpSQ0QGI4rtW4E6iVqNmAupo6CDWK1Qnn5PyrvckTib9Q7ULjqEmlhVOO+o3v9B+MpX8YM5qGtcVCZOuUriN7hvDcMwDMMwDMMwTMC6RVDHjQHcm7OXRLoSMueEuq4IIcYYY6zrKkynEybTCVVVB1/4uLi4iDjPrKqIMYWH5x586XE9pkwb71II+PHHH88973kvAZ51xfnnbwOC5KT2RqPRbKFpRBtV3RCKjiPlCd0QegYs7/w1xKPr44d6UtJoPAw44mhuHEdzc3E0GsfhcBzH43EcjUdxOBjGwWAQy7KMRVnEoixjUZaxLEotypTr5b3vxMAuzbzLCmrHRxaNYpYAtK/v6KAswuLiYpybH8fhcBQHg2EcDNP5mzGUZRnLQRkHg7L9uSgKLYuSokgZXsmV1YWZN+WFaM8xpoLGSKhD6kZYR+W6y7YE0K0Lo/sMVe5yIqIFUWJupKjZ4Ye4JPbUNW7Xdvz8Qiu00JOZun81FSAWRQpE37WDxV9+HM7XDGcTJBapDM51uoiIw3mfHELHHwc7dxBDRIh5/3RlnhryAqwso1dcCXg0STz5zIIPEypfMf/yP2D8q7+ILi+n65gbp2uBriy0ySITyZlOFUzWcetryPoaMp0gdZVFNlL3T0kluK2Y6VwqAxwOiZMJ8095Iv6XfoH1agU/9IgoLtaIE8Khg8Tl5fwp6Fr3U5Pt5iQmAauukcEIGQyIKEElF3Zm0a0sWK3WkdPvxeD77p3GMSiRomjUuHaHai6ZFQIUHr9tC0rK6EqliCn83gu4GAjOUz7g/rA0DyHgirINpW+vuQ5QFLC2wvqb34KjpvKOmkghyiGJ7JFAgdhnvWEYhmEYhmEY39ZYiPv1MAs1sSdWtaJRFq4QIYQa7z0rKyv81m/9Fl/72tfdYDigqiqq6SwoTIuimFtYXOS00+4ef/oRP+N2H38CVT3DO0dsI7VzB0FA8NShYmlpkbvc5c68573vJYzHDmC4ZXjr6ZHpE4uUreQ2a0tdWR+tq8XdeANWF4TeK5m7xmukyeG6Rqe98vChQ/4Ln/8cK6urDIbDtiTPNWHg2pXXxaid08g7imLAZLLObW5zG3bt3EkIIZVN9YQiNAW/N+Vm0+mU+fl53vvef+Lzn/+8jId+sD4NAOXq8lF/zic+QYgREaEoS5xzOOfTmmaBpuku15QkOi8UxZDJdMoxu47hpJNOzK6wrtywEQ3zRKDAdDpjPB7zlf8+j7e97e0iwuA6Kgklz/WDBnCrE4MLGsWH3CuwyW+K4sAXKFP8ySfA1qUUZC4ONAezi7KhqlSUGAUZjAirq4wf+VNMX/d69JOfxI92JodYjCn3CiVqKk2rCfjb3h5ZWETXJlB61DdllAoaQAPiSuKRw9SXXYHDp9/nEsBShNl0hfGvP5PRk59AXF5BixIpy9wFoCsj1aYjZ4jEqgLvcYMBIoNUvprFMq0DoZrhykGbWSbOtUKikDa6FB7WKzQoiy/4DQ6d/Um4+PIkbIWQsrUOHkH3H9i4rbXL7t8QIjcoiWWZOlLm4HQVBV9Qi2OqgW0/+hA4Zic6myJFyrlqyk21mTZNN6sGhdLhlhaz3Cf5NlZUAq5wUE0Jx53A8EcfkMaUOzx29s/0YRQAV5ZUH/ow1Yc+yWC4hRhqss7HAadcreCJ+wa0H2VmxzIMwzAMwzAMwwSsWxoxhK5esHlK1q68SWPKs3LOsby8zAc/+MH6vPPO+0/gMDAPfALYC/w4cOKb/uGN93jXO97F37/hjew+/jjqWON9bxlcoxbF9jlz67ZtALK6uprGVMVtwJ03vE/7ZVXfnOfTaytL1N65+vVnmgWsO9/5zs0vL33729/+7v88+8N1qCrxZZkbMUqruWVphpR/rsQYBZip6g9UdX3i8cft1rf841tEd+zoBKLujUk0C2lhptMpg0HJ179+Cc97zrPdkcOH17dtXfrC+vQowCUf/+Sn3/OYx/58pSLiSPlESGrr6Ly7xsxFRByqqvr9deC4clDq6//6r+Wkk05M3f2KApH+VHTi16Sa4QrP6uqaPvf5z5Pzv/qVw0vzoy8dXZ1cp3gQRKbjWa3HaEHVypjdnlOE6ByBmuKU28F4COvrUAxSsn/TDbENtOoK7RCPhClsW2L89Kdz9LG/QCGgzuNDDtTPOWASU+aVu/Vt88BqGGz6mIia2wxC2HeAsP8AhS/RqHhAypLp6kGK77svC7/xHEI1SetWlukc0g/jz8OtA1qFFHYeA3Hf1cTLLiHu349Igdu1C7nNbSi3bk0CYl0hvujch3SCsuQxxOkEd8qpDB77aGYv+W2GOsdMa5wUMJuil1/ZCaGNkNQawiTtC1XceAyDEYricy9DUZCyoK7X8MedwOiH75/2QB2hkLbbgqr2MtuyeyoGoMBt3Z4+RTTmDLG0Xs4XzKZHkdNPo7zrXSFoutZeh9F245UlAkzf8GaKegrlAoQKB5QI+wQ5qHG6FuJvH4TJxjvIMAzDMAzDMAzDBKxbDCHEnn61oV6rZ/sAzYFM4/FwdTAYPHk2m12w6VB/deyxx95mefnImz/y0Y/c++/+9m/ib535YjdbWaNwTSizw7mc+xPbR9VUVriBMgrVTJwMNqgASBu23peddKPWdCMUrJ5OlcWaDdlS2T3TF7vOOOOM5kzn7D946Kf2Hzx04zZkUdzPC9/vfKF/9Aev0JNPPlmm0ylFUSS3Tq8rpKgQidRVhRMhhKjPfc5z9Pzzz6927tzytL37j7w5X8VHV1ZXP7qSBcAbw3hYvk+RH3v5779CT7/n6TKZTNpSwm4jdK6tOgQ0BobDAb/1my+O//S+9/it8+NzDq+uv/UbiAcyqWu3LUbZGZUKRcW3wd8OqCUFpDsco1vfphOXfBYGpbdgTWA9JIUughsO0LU1hj/1EIqH3B95378zWthGqGe9Le2TkFQO8Mce00p50DXd1J4w44Bqz5XobBU/WGBWhZQ/NptSLWxh8QUvQI/biayswGBI252QJk8NJChaV6gvcOOC6tOfYfKWNxM+8Wm4bA9hdQV1Hpmbp7j9qQwf9XBGj/o5WJhPWVKuNypJ94qqoIVHnSdQM37MzzD5q79D9+xDh0NUk9spfu3irMcFHK4L52+bCzg0KjI/j19YQgg4IgWaAuE1orNlBvf5SeS0uxEnkyx7JgNW68DS7vNC8o2khcPd6iREPNR12suqqPeowtQXLP7Ej6Z8sPV1dDBM3+cbUjQQQ0QHA+J55xI++M8M3Hxyg0ZtukrqAQ1SOYlOBkcsw90wDMMwDMMwDBOwbskCVtys6fR9Ojlpqc0cUkKIMpvNBqrqRKQvWLirr776ayeffPKjLrvssn87/4LzTwHUOyeC4EVSZVVbkufa8O/Dy0cAdL6Rr4AapMli+sbxVDGrGDdFv5JOnNL+767N4+Wu9RcvfvGLr/c85557rgB88IPvvu3Aj/7h0JHlE37vpb+jP/2zP+1m1SyVHDZjcgIxz7ZLnRurqmZxaZHffNGL9J1vO8vt2Dp/0d5JeOemmXE3ZCwAH/rQh9zZZ59dH7drx7MP7D/ww099+jP0KU9/isyqCoSN46HpcJi6UNbVjIXFRf7yta+Nr3zlK/38eHiBiL5sg7p0TXQW42wche14ZlT5t13ymBPBhwjlHOXu41pRqXELdfpVsye734lXxBVoCDAaM/ekJ7L6nx9jHGoq71NZnea5DRXMj5Ad29sr1Hwy0a4To+ZSSb38cmRWIeMCJAlY9fpR5n76Fyl/9AGEySSFoJOaFLYRcjlrKlZTKEtcPWXyytew/uq/xO25iiEO8YMs6AT08CpcdjnrZ5/N7F/+g8U/eSUcfxxa1UnY8YrG1LEQhSgCUsIsu7C+/95Ub3s7wc0TcXgc4cKvQR3zIoZc9efbIPk2pH9hnnLbTioiJTF1NCwEqddwowXmHvlIdFDCSoUM/QYRuem30JoqGxeiFLiTTkIX5nFrM5Ai3TLOU89WiSecxOgB90fruiux7G/ooFDXFIMBa296K2H/PuJ4e3p9PnlA2O9SqP4cuDX7ODcMwzAMwzAMwwSsWy6x11GQngIhm7w0mp4Ym0fMICLxWpQkYownxRjHg+EoSV+iiJeNIdmanCplUTBZn3DRRRcjUDLPBuHk2kWn67D43JSL143vbk1HTcZWL9BerhmypQBnnnnm9Z2heWPcsmXh4QcOHz3hZ874mfj0Zz5d1ifreOcpyoJOC5QNYsB0OmFxaZG//bvXxz/4gz9wWxbHXxPnHs/K8sFNV6E3Yiz1ccdse+bVew/83gPuf//i+S/8DZ1VM0FhOBh2OWjNGyI4VSaTCQtLS5zziY/GF73oRQ7iRcPx0mMPHjz4qetYGgHi7gV27qnCvXczZEmd1E3omGoquctilKtrdOtW5FYnZEEoOffIW0dz/Vu3Tj0tUcANhoT1dUYPvD/Th/8Y1T+8i3JxC9X6Ws52EmJd4bYfgzvx+FSi6XwKLI9djppqzPtfCZdejosRxBELTwgz6qWdbH3846AAmSlaOsT1QvhzULvOplAUuOmEydOfRfXav2FhtBVZ3EHKJ6+SUKkCfoD3i8zhWD7rHRxdrthy1t+goxGxrlBKooDmytsYUjmjm0Z8AaMfvT9H3/auVCboCiKesGcvOpshgwKN6QKd7y+NA63BFRTbtzFEGGpNEMUXnjBZh+/5PuSHfyA5tcoCXM732rQEXcyatqKg7NyFLM4jK6spwR0YqrAelhn/8CMpTzwJrSrE+43LqZpC7kdDdM/lrL/jPYibo3Ie0To7tJRKhEtdRB1CqMQ+zQ3DMAzDMAzD+HbGOlNdn4AVYg7tbtxXbWFfbwIFjSoqiveeoigWH/SgB81v2bJl67Zt27Zs2bJl2+7du+cWFhbud8UVV7xJkOMf/IAHa9J9BOeTCNEEWseYzlkOBly9dy+f/sQno8Bfnn76zkNZONrQYk96HQivX5u5CQrWhsirLodHtSstc9fe5Eyv56sRdeLOrYtPO3Jk5aWn3+M0/dNX/6kT8RKC4nK5XgrBlg3Sz3QyZX5hgQ9/+MP67Kc9U0vPBaPx+NF7Dy6f0zvHjRmLAO6EndufcfDAkd875ZRTiz/909fojh3bpZpWlGWTe9UFjzeCzvpkwmg85sorLtenP+2Z7N+7d8/WrVsfk8Ur9w10RV2r/N1E5OHf5UqG4kTFUeBSDlMW7ByKVBN053bk1NsQQ0Ccb91XKm31W/dN3heSv1dJQpuWA+ae9gTWd29HpzVFUeBU8U5QneGOPR454XioAjhJ+VtKm1WW6tM8MpsRL7oMxRFFKMQTZyuU970P/t73SCV1uVtg2yUzr7jGSMThioLJmb/H+mv/Er+wnVlRMFlbZbq+Sqgrwqwi1hWxnlJN1qlmE+aXdjP54HtZe83rUre/GCHG7ArL/iPtMqU01rjTTyMsLeEmU1wUFE916BDMqqTMaUqo2jB9Ln2jQLFtiRGOMgacSw0OahEGj3wYsms7Op0iZYG6ZrN1rszmoCK502GeU79lO27rEqo1TgTxig9TXDli7scfCmWZO0W6LIppe03RO8R7Ju9+P/W55+JGY6hniEYiihNYd4HLXCA6XXd1Y+szDMMwDMMwDMMwAesWSYiBus7ZMdILLe/LWKo45wgxsLa+vlDX9Vv+9V//9VNHjhz5+KFDhz5x5MiRj+3Zs+dTKysrb52fmzvxBc97nj7ikQ93s9mMovAbmvzFGAkamFUVzjl9+9vfxWWXfN2N54cfO+usc2cAM7omaT1liWst7dObvtJ6bcFZ2ZGmqhvO17hrXvKSG32a4c7Fxacvr6z94Ym7dw9f97rXceyxxzKbVQxGoySMtTlb3XjqumYwGHDZpZfx60/6dV1dPiLbFsdPu2LvwXNINZN6E+6FeOy2xYcsr66+fGFhy+A1r3q13vHOd5TV1VVG49HGOW8lKGFW14j31HXNc579XP3kpz/ttm6Z+/DBgwc/2Rz3G+4xkaCq4SSRZImUlMfU7QxNApZOKXYei+zcAXXdWa3kG63hpl/4lHNVnv4D+If9BNNqNQXSO8BHlEBxzPHI/ALEAE3uEqCkwHeNCkUBy0eoL70UxRMQimpGQBg89MeQhTFSJ5FNxbWlt6nDXyTGAOMh4Z8+wNqrX0U52MEkOtZmM2qEoOn0KrkXY0zuqhhqYjVlKGPW//4t6P6DaFFCDEgMuaNkMkLhkktN64A75lj8qSfg4oRCQSnQ/QdTEH4Teq+9W4ou+w1Atm3DIfgYEO/Rah2OP4nBQ34k/T2GjfWRqkSNtJJaz5IlLu1pmV9Etm4B6uSFLQvqcJTiLndl+APfR13V6UK8T29MHk7QiI6G6Ooq1RvfzAAgKhJSOWQjZq44dL/AalW/eu9qde43EFINwzAMwzAMwzBu9lgJ4fWJOLlLWPv0GWMOeZb8lBzxRZrGpcUlfuWXf8VdeeUVtx2NxtR11YpAvijYvnUb33vve8V7f9/3OY2NuOBaZ5PG9NC7vrbO0tIS537lq/qaP/kjGTg5Z84PL11lukEM6YtouqEzYC5T0tRZTa5P5bhR5II22RTofuMFMgfExcXBrapYvcQXZflnf/Zn8bvv/j1uefkow+E45xBJV7KJUNc1dV23mVPPfc6z4n9/+Uvu+J2LH7xisvzpGyIYXetFQbjTrl0Ll60effS0CsWr/vC3w4/8+EP80eWjjEbjlFukuVNfHkuMkZDzo8bjMS972W/rm97yZrd9y/wHXKXPuKGCgao6h8ixuRSxP69OmhkXPFCecqtu1cVxzXrGLCaGLKj08rqSKpbcVoiw8MTHc/D9/8L4wFFcUaDeUeOIJxyHliWsr4Evk3SlklxCKFHBeU/cu5/60D6kHBKKglitw61uS/m998qfLgVauF7pm6AxIHUSxtzRwyz/6Wtw6zPCaAs6q1IZI+BxxEbwoqkYTT+HqsL5AeGiCwgfPgf/iB+DtRmU7hqWOl94Qqgptm1ndMc7UH3ui4gIvhzByhH04EFk9zHpvm4C+ZupFIdqKuh027YRvccHZeiFtdkKgwf9HOUd70Ccpq6TGiMqQswilvQ9gK6r7WzuW9myRLnzWCCghcfFwIzI8MH3xx1/HLqyigxHxF5ZqNQRjQHnh+hH/pP6nHMoigW0mqWPJ9VUlizCIUGPOE/p5Mp1Qs1NDcMzDMMwDMMwDMMwAevmT1M22IlD5HKj7CgRR1E4Yoxs3bqVZz3rmVyPaOHW1tZTeZj43iGTU8PhWFpa4rLLL+cJv/J4ufyyS2XLlvE/7jty9KK8XjF5sJIjppWUpO8I6+KW5JpXccPFO+37URovUBcs3tfEnPgbN61Jk5gfF8PnHTi0PP/i33qJPuzhD3erqyuU5SDNdVQioQ1NT9VrSlVVLC4u8oo//EN921lvc7u2zP/z4fX4OFbZz413mTQ2p21XTNdeubw2ffSvP/HX9AlPfKJfXVtjUAzwImgTqJ5FQVBiDMxmM+bn53nve96pL/+DP4gLo+G/bB8v/uKFR67ad0PHsioyWwJ2q0OJeUC6QXhykubenXJytzNzWWDqOngt66YxBcE3+WEiUDiIkTiZMrjb3Rj87CNYf8VrmN+yFSUSRHAnH49IEpJUutJNJQWCO1HUCVy9F46uEIsB4hw1U4rT7k5xp9ulwPiyoDHDJXeTZAGqxi3MUf/T2cw++in8cCshBiRGXH8fu9SZr3Pe5dJNATcocGsHqL90LsUjfgxVwSsEJ20pbhJaPdQBhgPkuOOzC0zxg5JYzdA9VyF3vkO+p3tz2Ibgp5XwW7YQSocLINUExosMHvrQFN6+NkldDyUXDkbwoshsAjJIr2nWRATxLjnGhkPcSSczA+a9R9dXiTuOYfSwh6bMMVIYvea6UIkxZYehOGomr31DEirLAg2zLLDGLHgJV/nIqhDL4Ey4MgzDMAzDMAzj2x4rIbwhQk7sBKymtChuMvk459AYmc5mrK2ty/ramqytrcnq6qqsrq3K6uqqrCwvy8rKSs4Fcq0kpG2JnLC+vsZHP/oRHv3on9WPf+yjYXFp8XX7j6z/HX1n0WyTUNEEUm0Mxsp5SPpNWOVOVGiHDX0pq5eB9ZIbJBhtgW27dyy8+vDh5cef8YhH+Oc8/3myvr6OONd+NblLMURiiK1otLi4yPs/8AF92ctexnhYvD+W419aXV29mpvmvvJA3Lq09MQjR1d/8X73vU/4rTPPlDoEnAjD0SitrUZi/mpKKEMIzM/P88UvfVGf9cxnS5ys79153O5fu/CqVry6vrHo/aBAuftOETmu9Zw1OUxd48IIBOfQE07IMUjShelvOCJIiEhVp1LEtu4xZ2LlfSzZxbb06EdRH38sTNYRVWTsKb7rDpDFk+B9dhRBRIgOos/3wRVXIkfXkWJIjBURR3m3706CSlXThMfHLMc1bsZYOJjOmP7zB4mTI4RyRGy6/tHLj9Ik5mlsNnVs959kUTZO1ujeFVNclPSq+Zq5AhiWeS5Cck9OZuiVV3YTx+aJ7IW8zc8RvaBOmFbrhHvcg8EP3ScJZYVHXer6F7MIJ4cPo3/+58Rzv5JyxELork2kPWw47lhmeAaAr49QfM/dKU47PTmqirLN5UpRWoIGRQcj9HOfY/1DHyaUW5iJQ8W3ZZDN58ClokycOtEwsE9xwzAMwzAMwzBMwLqFE6O2Ie6a24v1hRvtZeeIEwrvGQxKykFJWZQMBgMG5YDBYMBwOGQ4GOKdb7sOqkKMgRgDVV3zBy9/OQ+8/wP0ox/5uOzYvvXI6nT5d4BD1/aU3RRnbRCyrvEY3nQMvPFL3RqwetlTRG3/0LhhgOQqueF7Lg63Ltz3yPL6L93lznfSV776TxmO0jN2WZR4lzJ/HIAKISQxazqdMBgMufCii/RpT3mKrK+tyGhh4ff379+/pxGiboIyV+9cXLzd0ZWVRxy/e7f+0R+9QnYds4vZbMZoOMxh3q6pKcsd7gKz6ZSyHLB37159xlOfzmVfu5id25fe8PWvf33PDRSvBNALFhe3OK9PPxEn2/EETXlXnZCT9YgQCMUAd7vb5flOooloL4FeFbLQF373FchFlybxqQpd+HqziQYlcTKhvPt3M3rUI5hMV3GTiuHCDvxtT0mD9x7J3QlRwWnEhwBRcCjh3K/gZqtIMUBmM2RugcFd7pI3T+rq1wb/NyJYDLhBiR44wPRTn8ZTonVT6pqdU5rdjdCKWGSBS4mIRhwRj6A+3wPSzbhIKr10zZzk9ompRDEdKohLDqzL93RL2wjUkEqE8V0Dg9GY6H0S4bwwfshDcTu2wWSCepfFwtQdUJxDzruQ+lVvgH0H2vsmZWHlktim+cHCEuAJdU3tPPMP/wlkOEzipfedjpazryIR7yKzN7wNPXg1UpZoqNPYm9D+LPVeSvRrdTgP9OzrUOkMwzAMwzAMwzBMwLolsSG63aVZc7Kp4EiTSOScw3ufvsoC7z3O5S9fIM7hnUsujRzWnUqj0gP0gx70IF76spfJve51d91/8PBW54Yv3bFjx2J+PHcbFRDptJC241wecd+NcdPlO66pjGkvc0tbkcvLDd5vYefOxdstr02fOxiM9Y9e9Wp2H3+ChBAZjUZ4V9ClPqXSvajCrAqICEeXj/LEX32CXHjRRSzNzb1i//7Dn2uOexPEK5mfn7/z8mz6JkFP+83f/E1OO/1ebn19nfHc3CYZsHOhVXUSKWazGc973nPl7A/9p+zYvvjyS/YefiHJH3ejhLQYo54QhQUltYrL6yaaXFA4h4Ya2boNd8pt29I2EZ8Cu5t1jkrUGqSgetc7qF73JhAhhoCGQAyBkHOa8AV4RwyB8RMeT7zNbQmzwwx27EaWtqAx9MST1AXRA14VLw6ppswuvACoEXFoNcNt30Jxu1vndoUp5K1nImt1LXGeeMkl6Fcvo3AjJFaggaixcw1mR1XbWTHv6aYLn2gEIm6c10mvxZEmm/5dPQq5Y2Jwwkxr6iuv7t6/4TTa3Fbp1+UAXEmYzfC7T2L8sIegMSZhKt+7rk5lkALUZ3+YsOcSpBimNXap/DI2jRizrdPPjREnzGar6LEnMXjgD6fN45IKJ963G0JDBeMhXHQR0/e8G5ERrg64kJyBzQV4gamHqyQwDfq5/Ueq/8IC3A3DMAzDMAzDMAHrO0jCarOFNok6zUNujNR1zWxWMatqqqqiqmrquqKqK6pZlToaCnjvKAqP9y67XMA7x33ve1+e+9zn8uZ/fJs8+uce5dfXpz+/vLz8Z7t27Vq4hjAiG8eXSq3kGmN3uI2B6zdeubtuBah19Vzv8QWIw+HwlMna7I2TWfX9L/3tl/JD97+/rK2uXbtDzJF0KamIccZwNObMl7xY//3f/2O2fWH+D/cfXXkBsHoTH8wFiBLrF0+ns9N/5VeeEB7/+F+WtdVViqLA51q05LDrcpVCqIiqDAcDXvmKV+gb/vbvZluWxi+/4ocf8kKarPEbM8XLy0Klsj06yihZhWtsbbkDnysg1hTHHYcsbU2bILuFpLcHNEa0KNC9VzObTpi94624fcuo91DVufY1ZpEml7KtreNuf3uGZ/wckZpix1ZkNIYY8aq4GBFRxGtXqjoYEI8uU++7CscApw7qKW7rVmTXTmJV0/iAUqB4FsE0l/gB1VfORVbXEDdI2Vd0opX2lFfJYtYGLSo35FMpcTt35NeRSws3bYbmh2pKOHgIBWqSUBhR6v0HaQ7QOLBaIav39lh4RCDqKv4hP47c4bvQ6QzxvmuSEAJSlsj+A8z+6f1oNYVp6mAq/Tu3l/PlFxdwA0eYrVDc5weQk05GZ9N0PCfthUmIaIg456jf/0Gqr30dyq0p36vJ5Mvj9woTQQ+hlI6y9zlvApZhGIZhGIZhGCZg3VIRtylLR5PLKT1k9yLSc4BzUXiKwuNE1IvgveC9p/SewaDQovTUdaAOdRIGRHBN7hOwPpmwvLzCibtP4nWv+2v91Sc8Ic5ms59fPnTo1aeeemo/yybq5ufR/BDed2S1rpQbIWBpSu122lcNmpDrRiHIodyaA9ZjaAxQL7lOrWsMJ86V/h9W1qb3esL/90vxSb/+FFlbW8MXPuVL5TIraQSM3Oixmk1YXFjgrW95c3zNa/6fDAfl/smKvjJrETfFWeKAuHPr1oeurE/ve58f+IH40pee6Zp8Iu9TZ0jZ4GoTxEFd18zPzfG+f/qn+PKX/6Esjob7ih2Lr+Sss8JNGcsEKhTdjcOhyXHV5MrnwHh1DiUgJxwHZZmcODQdGnPce1S0rnDOU190MfWhZeLF51K/7T34skjiijbx8407z4FzSB0YPP7R6O7b4LZtxW1ZTE6ifP1OWkNQ+p1z6OHDyL5DScAiQpzBtp3Ilq1IrJPLS6XXA1NzAHn6ufr6JQgzfOFwWuM1CWVp4mPbcRDV1gUm2hxJiLOKOJrDn3RCG/iOc105ZbuXY/r96hrV1fuI+LYK1uHQIwegTl0AGydkY13s55DJ/DzOCzq3g+JRPwuDEnEC3iVHpkuqmhsMqc/+OPELX8bhYe/eXAIZU0mj5MNnEa7ctQs/V6KuoHzow2BuhEyrVhRWSaXJaISyQA7uZ/LGtxFkmMoge3eYSCq9LBCOChx0UQtxgRtfWmsYhmEYhmEYhmEC1rcbGunZXDYkmOdobW3zhYJG9u3fz779e+PhI4f18NGj8fCRI/HIkSPxyNHlePjoEa3rWofDAWVZUlUVMQsFzjlEHN6XDIcjptMZIar80R/9kfzoQ35EJ3X9sL17994VgLIsgFGsY190asuS2joobqQdqNXBRJO20pVVNd3YYmwUrFwils9T1/U30K+SYLS4del+h1fW7v1D9/nB+Puv+COZVVUO3HZ5yDGFfPceyuu6ZmlpiS9/+b/12c9+lgt1teKL8vlrrB2g62Z4oy4PiDsXF398/+HDf3Prk0469lWvepXs2HWMTGcVo9E4u8Fibz7TJNRVzfz8Auedd54++7nPc+tryytblsbP3/u1vQdv4lg4BLcewPjWONC4wQHUqjFZwNJbnYQUJS5o7kzXX+rUtVGAsOcqODyBWDB50xvhwDIyKJNQ0yxHo0gNCuJshrv9bZFH/BT1scfDqEzKbVkgzmcRV9Ja10moDJddiVy1H+dLXKhw1Lht23JHvJhzw3oKaN4yjRtLjq7hsu7kXeo0mLpp5siqVjFLTqnNgp2GKbJtK/52t0n70Dlw/hpiMyHgvSMcPEz9tctRhqmzYYw4HBzdD8trUJbd7d04pHoWN79jR8q5ut8PUXzfPdMpyjILz0mViqMBOp0xfcd7kdUZSkF95aXteHyTCd+7KQcnHMdgPKK8wx0Y3v8+aF0jvhPi0spGgoAMBtT/8m9Un/kcbrhAyDlj/S0jwBC4LAbZj4hTHdunuGEYhmEYhmEYtwQKm4IbI31o+ywukr9Bqev0kLx8dJlnPevZXHjBBW40HidxKnteXK5vmpub4573PF1/6Zd+WW518kmEEFNZV0oLRzSVXQ2GA9bX11haWpSXnPnS+MlPf3rboYOHngE8Zsvc3J79a2sfqqvqQdcmz4jmSJ/sIoqQu/ghqirfyI311re+NR5zzDEL+/bte4BzvhG0uidklBi7bKNGcKlD3epXZ15TMAo7ty3+6OGjyy+/3W1uE//iL18rW7Zuk9W1NcqybF+YjpudRwL1rML7gsOHj/BrT3qyXn7FnsNbF+efenh59Y29Y98YHBCP3br40CNrk9ctLCzu/KNX/nG8+2mnuf2Hj7A0P9/me/VlAY3KdDalKEuOHj3KM5/1LD3vvK8c3n3M1qdcuvfwP9xUnRBQCv/MpRhPOAEf69jm8ufFy4V1AhDxp56SBIsQU8B34/dSbf14DohXXoFWK7iF45h99MPU73wf/pcfheo0uXSkCSknda8r0zUP/r9fRA8dQUMSRiSLloK0sfSqmgLHLr8CPbSMG4wIdZ10n9GwFTplkw+t8X41lxem025RssMqSE982vC+tqCQ2glOPMgU2X0r9ORbEWYznHed/a3taZDKKp0vqC++kHD51RTDcXIpxYinhCPL6OFD6LbF5FJrOzY62nwvgMEAyjmKBz8A5gZQVdkNF5GoxFgjgyHxc59l9p8fYjycJ05XCYcPUraC2oYek2mM27ZQjBYYPPgBcMIx6GSCK8vOOZmbSETn8LN1Jn9/FsQKp4KLdb/TAirJ4eZQLou1HIxxFeHf7IPbMAzDMAzDMAwTsL4j0V6pXn5wjAEVmE2nfO6zn10/99xzfxO4FCh7Go+QAr4f9773v//HP/CBD8Y3venN7ran3JaqrnDicgh8UiyiU8bjESsry9zrXvfip37yJ+T1r/+7xTPOOGNw1lln7QHeMp1NHwREQbxsKhXsSsWS4FAOSoA6u6uuS3CR/PcTBR47Gg6zbNd1OdRcfxWz46s533Q6uy7BiJ1zcw+ZrU/+dnHL0jGv/n9/od91xzvI2vo6o9EoiVbQZk1BKscKoSbGyHDoec6znx0+8uEP+8W5uS9k8WpTNPgNFoySeBLDE+q6OvZlL/u98IiffqQ/fHSF0WicBKO2zC6JDgLUddU0weNFL/qt8IH3v9/v3LrwuT0bxaubli+kcTCPsEOdRlLdpmgTY5+FwhiIDPF3uEO+gBqh3HBCjbHNIY+XXoIj4JwgRcH6q/6ChUf8GLJ1AakD4iR1ztPsMPQejZHie+6W1jgEpPDtNbeNBJMdMZmprrwSF0MSwFQ2lts13QRj4xTTVrdp7psUeRZbRxYIzdaM2pyFzmkGqDoCgopDNTL//d+LDgbo+hrqip54lcoOtTm/CvHzX8CFKTpcxMVIERTvC/TQIXTfPuQ2t4J8rySBuhOrAdz6Gpx8Eu4+35+CzuoazV0adVa14uHyO96N7Lkcv/V4JtND+P0HkKrGOYdGTa932st8cwxOuQ3DH3lgmoWiQHNzB82zLdMZbn6O+hOfYXLOOQxljlDXFBo6gbMZu0IQ0X1EWY/xqnI1/v3/aH8ahmEYhmEYhmHcTLASwhuLyHU9C2rQyGA4mG3ZsuXtwFnAm4A35683AW+7053u9Ms7d+744Cc/9Sn32te9XpMeEQghtvlCILjc0TC7oOT+P/wALYriIW9/+1mPzhlV47qa5SdT7QlqG9So9ve3OukkgOO2b99+wmZBZ8NFpGOvee8mW7dubX7ZaXeNtKA98wewvr7KJrFOgDgacWLw8a+mdXXMS8/87fjghzxY1tbWKYsSESic4ETwLmVPiSQ3V11VzM/P84pXvCK+7q9f5xfmx+fXqs/nfxZIHXfvWnj+waNrD3zcLz4uPu3pT3Xrk3WGZcGgCePOAl0yNkWqqiaEyNzcmFf96Z/F17zm1X5+XJ6nMbwA8P8TcSDrQ2wVxzbniF2BXBIlRPAIg+mUYsc2ilud1PYP0J4s1+RDiXMwmRIvvZQCjwszBqMthC9/iupNb0vZVbMpEut0nqweaXYcxRghBJz3NDlqKo33SSGE9LtYU3/toiRkuYLYvKbJQdvQXEDbLSR54AoUW7aiOKLGtutf47VK4lFXfqikTpQBwHlqBS22MDjjoUgIuJjFJtlYTqeqqHeoBmbnfJYBSVEuYo3TAIUnHDpM2H8gb1jpqXWAyxlhQKyV+iEPgdufkl7biFcKBMUNSsJXL2b9He9FiiFTD5FIuGovrE+I2guJ125edX1CvPvd0bveKXcfzOJVUxpY1ckxpsrau99PffAAYTQmaugps3kdJQlvawJXJTFQBqmi0DAMwzAMwzAMwwSsWzox51xtVh76ZU4RCKrEOlDXtRw5cmReVT3J4ebzlwP8ueeee9Xc3Pz/55x7/6c/8xmZTKZRBEKoSaWGtLlQzjkGgwEhRjnt9NPizp07yxjZnl1SOptVfblqo9jUjC2mx+I73+UuYWE8d++1lZWfy1fgN4lY6dQi6px7+NLSloXb3f52nVur14ixCdVGBOc9k8mEg/sPbN5PEWDLeOHRh5cnx/3i434l/tqTf93NqhlFUSR3ECmgWnIIdhObVM8q5hcWeMc73qW/9eIXu7lh+VVfyKPX19c/eRMFIwe4k4/d9vyD+1dedt/73Hf8O7/3cmnqK8uySHFLvQBvjZEQApPJOuO5Me9817v0pWe+2Hkv5xeDuUcfOLr+aTY2qrtRMmiSMZKcc5I4tsYk1Ph2ayVBxysU9Qy3axts20KXbNbLOMslhjhBV1YIe67G4RFVXAwUfsT6a/8SrjoAo2ES6eiX+UmX9+Rcdlvppp0lyUHkHfHIUaoLvgZSEMUR8s7V1TUk7bfsIMqZVXmWopPk3AOKU25DpCRGoRZHLRAbd5ZsvNVSfFYKLC/8AJ0eYvATP4bc815QVYgvutyqngalWsNgiF55KdVn/4sBQ3yo8TEgGhHvYTJDDx5p97Nubj/ok0bpbnUi809+AiwsoDFCUeRrC1k4cqy/4324i85HhgvMqhrBowcOENcn6cbQJjMvz40qurRE8ZjHwq7j8k51WYjLAqJGZDQkXHIJa+95L74Yp7napD9LK/UpKyhXuEjO3zfnlWEYhmEYhmEYJmB9Z0yQ26gJNTk2PVeSIkTx1Jryktpn7o1fze+KSy+9dE+M8SNHjxwkhEqdcxueRBtRx3uP955QVdzqpFtx0gnHt8IQwL4D+/Iztqfz83Td8xr3VVXVfO+9v1fu+t1308ls9ku3u91t7gXkVnatyBaBuHv37qfEGP/wHne/x+gud7mbzKYTnLhcOphUD21cOxoZlCX79u11V12xZwX4ExGJ+Xm63LFt4bn7D6287Ae///v87/7+77vZbEpVVXjv8M6ncUIqryI5ZmKMLCwucN5Xz9dnPOOpQjU7b3Hr4i8cObL2OZKBRnqi4PV9Sd7j8dbHbPvllZWVl51069u4V77qT/WYY3fJdDplOBqm8k3a0DCSBqPMqhmLS0t85dwv63Of82w5evTIV7Zs2frzR44c+a+bMJa+YOgAnRsMfhR40ClRdKTRQcCLtuHmoOCEmhlh93H4HdtTTpN3SO/WFVK5nPMFcWUV3XM10RUEUUI1xc3Pw5c+z/SNb8QVJdH7Nh9KVXqZUVkGyWHpKaRfegqJQFHCkaPIZXvwviQoBHEoBXH/XnR1FfEu71JpxaCIEpxPgpcq7ru/G926BQ01lS+ocURxrVOrEZQku8OCE/xgRLl+lNH2Y5h/3nPQ0TDdWEWROvXRhc07jTnA3TP5lw/B5ZfihkM0VDgCxBq8R3QK+/dtvL8bd1pWVFUVNzem2LENIeIkzZHkMklGQ/TgISbvfieDmH9XVSgFuv8grK6jTpAYiDnTSrMgqFu3Udz1jlD47KTL5Za5QUL0HvEF0w/9B5x3HsVwHqoaJJfxor1mEkKpwkFVrhDFoQfWU6dOwzAMwzAMwzAME7Bu+cSeT2mTy6knbIlo/yXauKQ2fQHUx+3a9fMiPG/Xrl0UxcC14eXtsdjwc9RIORgyP7+wYWRfOf8CDh06RDkYkCSlTa4MEbwvCKHm2GOPdS94wQvYumXrnS644Gt/65x71LHz88eQHnDD/Pz8XQZF8cI9e/b84a6du8rnPOc5umXrFmrVJJBJV5bVXafgvddLL7uUPQf2T4DPNkPesWPLfVbXJr+785hj/B+/6tXs2LGdqqop/KDLSup1fIPkFhsMBqyurfLspz1DrrzisrDj2J2/fvXVBxvnVQWEG/GlQLzTnXYtrKxPHlFH73/39/4g3v3u3y0rKysMh8NW7JMujAlxQh0Cc6Mx08mE5z3/N+TCCy8KJ598wpMOHDjw6Zs4lsAmN4z6eOrIyY7vEhed9grg+g4kgSkV7oSTkLl5pKpR59sDNbqiikBREA/sJe7Zj5YDplGJArGq8X6O2Wtfh15xBVIWMKs2dKoUpS1P3LBdcwJazDsM7+Dgfji4n+hcWxYnfojuvQq9Yg+Uwyy+bdiNWfsVwmRCcYfvwv3o/ZjUByjKeSBnROFAcwmtCOpSeeGgGFDGCbVMWHjxS3H3/m50fR0piizoseGLUKHikemU9Xe/l0FdI6XkHK90RZJ8Y+iVl3ejjLpBx2ru8xgCsa5pg8GE1NWxqhERJh86G/nS5xiUc4QqOSOj98jyEeTqvTQOtv4xG2tjrANtcW6+r0Q1h/UXaDVl/S3vYF4VV0ckdv6rZrlSJ9RUL3hIIvtVWEd+5wgcom/ZMwzDMAzDMAzD+DbFQtxvoIbVPtWqtsJCgyPi1YmX5IYqy7KYzWZy5zvfuRyPxwqwvr4u55577mw0Gj3m0OFDr1Fl6YwzfkaHw4Gsr69RFF03PlXZUEblxBPqiqPLR/ujcv/9xS9x+eWXcpe73I3ZrKIoNlYQOhGKwiMUqCo/8bCHyev/5vX6kpe+9I4XnH/em69eXf2XLDoVq6urj9y6tHjbe9/te/iNF/2GPvhHfkSqumZQDrJTqnF0SS6Fktb1cu5/n8uhgweLxcFgvDybcdtt27bsWT76/1UB//t/8PJ42umnubW19RSU3uYgaZvf1DivnHOsr6/z3Oc8hw/88z+ztLTg9u89+P8JPEjTXtUbsWJj7/17jznmmI9dfcXyaw8srz/4GU97uj7yjJ/2s6pmPB53ImFWAZpOkXWoKZxnZXmZZz/n2bz3ve8GkEsuueKXgYfQZV/dIErQ+flSY+1ee3Q6vbA5Y/RUW6LT24snVhHflK/lTn6d008pd6foMo0xlcwBTQyS0gvuv/wSZH0FGW3NJXkOqSr8YI741XOZ/d0/MHzhc5MY40F8mRx/TfkcXXkojWCZxxBz48P6sq8T146Cy+V0As6XxAOHqC+8iEHOc5Ke0JkcRem4vg7oeMzcs5/Nvk98mvD1yxmOdyGxSm6nHOauTlJpqYKsHWA2cMz99u9T/vovECcTxLmcGdcJv819GmYz/OIS03//N/iPDzMYLKRuiVmtU9V8X5fEyy9BqyqJgD13VCMoNV1H035P7jTRCKFOmVXVlNk7345fXUFHO9FZjapPJYrrq+ieKxHunuaKJpydVtBL9at0a5+dXbGukNGIeM6n0Y9+Au/nqasZHiFIEgg1i1eueZ9D9xFZEdVSOTTrdrgJWIZhGIZhGIZhmIB1i9auYuq2940QcXiXnCOTyXS+qqq/E5FVrulwC5Pp5K7HH7976Vd++Qn6mMc8WjS7jrquZM0DdvfjYDDgssuv4sD+A20gV1mW+/dcdeXk8//1xeFd7/rdGkItMRbpgVhbnSiV/+WQ9xgjD3/4w+X77/ODfOxjH4lf/K8vPPiqq/c+2DvP7hN2c9rpp8fvu9e93datW6WazXDe45xrNavW7YEQNeJcwXQ65SMf/jB1Ha681Sm3Xv/yRRdxIMbtk1n48ac+5ak87nG/KGtraxRl0YkjbHyqlvwf7z1Hjx7lzne6M3/xV38JMUgdwqPrEHO5WyqVizFQ1zXTyYyqmlFVFaqhffgvywH//cUv8Z5/es/hEMIXDx5deeh97nsfff4LX0AIFTFEBuVwQ5c9oQvWBqEclFx+4RUMhgOe8cxnANFVs/oxIcTUOVEF8Slk33lHWRSUZcGwHFCWPu0HgfF4jou/ej5veMNfE4j/BrQCVgDZJSInqSeo4qVT31JGklCEgACDY3Y2cehpb8QsokpyEkXJ3RO/dimeCHhE69SBL5e3OrfI+mtfx+DRj4Ljd6N1DYV23fukJ2FlBSq2mmVaAwHCRRfjJzOKUUkdKjTEFIi+coTqS+cyePhPQFG0WV6Ns0tUU9fCwYhqNqO4xz3Y/td/zaGnPJHq3K8wYISXIVK6FMg2q4mhJhKoT70Niy96McNf+Dl0OsF5j/qiK21sugqoEonocIjMJqz92Z/jV5aR+a0wnebSWsHj0BhwCHr55XDwCG7HVoiRGH3KteqrTXTmqzbBLCoyHhE/9Univ3+YolykRhGnSAyoc8T1FfSqfek+SoWbrVPS0YiG6WaNTSA/ksSxmMpJ197wVuLqGtV4C7GqsktMNwh3TQh+iMoVRaQS0RHqVu0j3DAMwzAMwzAME7C+M1C6cGnVLB70DBOI4J0HEbZu28JTn/pUf/VVV919PB63DpqYHUuFOLZu38Zpp52mp59+ujTleN4lh1TrCGqNXpEQU47PF770Rdm3f59zDhcjvPD7v/9tZ5599k+/+53veuTP/uzPRkCqqqIsy9YZ1VeJmgfcuq45ducuHvGTj3CP+MlH9BPqHeBUlelsSuGLrrSuJ6o12VpVVTM/P8cnzzlH//lf/lVK517x5YsuugzgyJEj8bvvctf15z73uVtDCHjncs5U55LR3v+ogC+SqenYY4/lSU9+8gYNkRvhHgkp9yi89c1vce/74Pune/fu1fn5+clzn/2chWOOOUaPHj3KeDwmxhQc30k2neOoKDyhDnzX7W7Pn73m/93ksTRb6GNn/yev++u/YjAc1ky77nExRD0Ozw51KDVFDipvywNF0BAQKfA7d3QlnJJcejk6vZeqD+GCi/Akt5P0RhpDxA/niV//GpO/fSPjl7yQMJ0g0YPvzG1d/hQ5CywXFGbRjhiIX78UyYKijxHVGqXAE6k/+QlYXUNHw9S10Pu2PlGy0KSFxxeOMJ0xvv99Gb3rPaz9zd9Qve9fiJdcikxWUe9hcRG54x0YP/CBDB710/BdtyOuryPOZ4FM2n3dJc9F4qyimJtj7U1nUb/n/YzHC1RVlV+XXGlRwGVBTffshaNHYde2XJPZ1gHTVZZq+78uKBAI4vBE1t/3HuKeK3GLu6inszZMHRHqUBMOH0ripDRls1yzLLn/gSOgQdHRCP3qV5n88z8jfkSdRW5prjkPNS1/vr8FviYRdeIK1NsnuGEYhmEYhmEYJmB9h4lYSdyIjVFlg+vDFSkEfGlxkSc/6UntM/91HE4AmUwmFIMBRc4RasST7qSRqJFqVjEoB/GjZ39Y1tYnn9+xsPCBAysrcubZZ9ejQXHkn//1X/XDZ39UH/CgH+bIkSOUZbnxQTgPowmKLxSqqqKua1TVqWoXmO0chfeURblBvGpEE3FZkKtC6tJX1/rGN/4De/ftDbu2bDmy78iRdvjHHnec27ZtO1EDRVm2ge1Ru7LBrr9dlz8VY6SqUr58jIoko0ou2+rPk2SHUL8fmzCbzdi6dSurq6ueZORhaWnJ7d69O11/dpV11YP9MkJpw9zFCSEqk8kkuY9QNKrDSbv+0tsfrhFnslATo7K6usKuY47Rw8srjcOu/xZiYLgdx7xGSRfaCI2KihAd1CGii/PICSdADu1Gk6iaU5NSRz2AekZ13kV40hglKk1jRd+UbLoxs7e+hdHjfh45cTcynaWSRJeCyelnnOUFc6poDOAdMplQX3QJLrvwNEacgAsR7+aoPvZR6k9+Bn//+6IrK8h4jIrLwmfEuU69cUVJnMxwp57Cwu/+DuHXnoxe8nU4uooMh7BtK3Libtwxu9L8r60hgxLN4qrmwPPWNSYRXV/Dzc0TLriEtRe8mKGUREpcnKK4tP80vTeKENwQ2XsIPXwUcS6V+WkEcVkk7PaWoPm+BA0VOhwS9+xh+r5/pnQlEgM+VrmXaAQpCETi0SNZbcrdHX12YzVBW9rtxLzCBC94J0w++K/ESy6hGG9LJZC9DaQ9hTKSugqsi+qFom6m+u6hC5/HygcNwzAMwzAMwzAB6ztHvpL+818jDLWOKen9XaiqihijSKf+dFVICjHXZhVlgRfpPRprV16Uy/RmsxlF4dlz1VW8/4MfFBH52oGVlfPIWsfObYt/fPnVh37iD//wD3b9wA9+r5ZlKbPZjOFwyMZH4nyOmNxOzjnKwSA/rGsv4ycFZ/cD5FvxKo8xqlLVNQuLC3zyk5+Mb37Tm33p3LtPuePiB/adcyQ/uUMITSFcdo1kpaEfO9XNYVcyKSIURbFhsrUfGpRfkwUlujnWVC6ZX+NKabPLQi43zG/OYtPmZdY2yL3Vx5xkZ5iHmEQlaUUm2SA4tl0f81w5oKgGQMpFixslhPr4xcHtD9XxqYs1eFQqurD0rIglQYUK2XIc7nanEEMEXxJTgd0GsVOcg8MHCZdeQUHRugbTnGsu+6xwc0vIV85l9g9vYvgbzyNOZ+n9UnTZV23uVVPcqSkbajAk7t/L7PLLGFIQNRKIeASNgXo4hx7Zy+zv38Tc/X6QmNLPk2CjjQCax9M0+huUaFURosLxx+JPOn6D4hLqQJxOEHG4wSB182vnuqvtk6joZB0tBsi04vCvPY3BpRdSjHdS1bM8Z8lFltxaQogQygGyuo4eOZLv9EgrmHa3ez5VLhvUiIaAc45w9kcIXz6P0WDchrE3E+8lhaXp8tF0T0vTIXSznt1eRjpjXafOissrTN/zAbyChGyJU7p8sVYKTX0IvQj7HHopSoBPXrXCvrR5CfY5bhiGYRiGYRjGtzvWhfD65KuYMo/S94oQWzdF/8EzlRcqRVFQliVlWVKUBeWgZDAYUA4GlMMBo9GY0XBE4YueJrapq6GDup5R1TXD4YhXv+Y1fOmLX1qbmxv8R//Z9fKrD52/OD96+7//x7/xJ3/8J8zPzxNCYDab9Q7XPPCn72OMG0QX5zzeeURSIs9m8aoJcG8udDqZMBwMOLp8hBf95ovkwKEDh7dsWXjzOedcvt4fW1G6NgeqFcLauPHeo7smIaovBjnn8N6nfCnnKApP4T1FUVAUBc659suLpH+dT4JTG+rd82U5wXm3QYPsr1tTxtgIN80ceOcpfEHhC3xRUHiP9+l3Po+nLEuKovs5jdNTOE/pUwVXCIGoSt279lnQhYieMI9D1BH6Y2kK3ZygVBQn3Ao5cTcaQ3IfeZ+D17PQlLvVsf8AHD2KMGjl0HYXuEabCshgkdnr/w69+OvIeJTeT9d9L7m2mlLCLGSpIt5TX3YF9f59iBsSia2QV4tQAVIusfb2t1L9+0eQhUXiZJpcRxrbksbGOdWuz2CAHw5wdUBX14gra+jKCrq6itZVyroalKjzKbh90wqLKnF9HXzaGytPey7+39/PYLSDUAVEJXdrlA0tQYM4KAagFRw60OVPbewVuVHMdoLECDhkNmHyjndRTqdoUW5wBrpc3lkgxMOHsw3ONcntSbxz9O6vVF8oMcKsovCe+tOfpP7YJ/HFErGuN5SEtmlZ0n2GFMA+iRx2qgPnSq5Rn2gYhmEYhmEYhmEC1i2W2NRg5cmSGFPJVuOeal0V2bmUBRURwYmDJvupKU/rhUJ3x+jeX9c1K8srzKqKLUtbeNOb3hT+5JWvdOPh4Guj0a3/rvf87YAqaHjtYFCuv+x3f0de99q/ivPz88QYmU4mxBCSQKSayx9jO15HGl97blIY/WZXUUNVVUymU4ajEc57XviCF4Z//9d/c9u3zH9p/6Gjb2+mq3vWdymwu9chj/6ctXPXr8bsdXnsua3YpCek8ScBS5xrw+ad8ykfCTa2idSubK0RGmOMPRdPXptmMD0Ra8O//bm6xvrRuq+6veDypCQRtO4pWBXEutZ6uyTxUHMJnzZChiiNFufv8l1oUeLqgCfpIM4nAcQ1c+c9uncfrE+IUhAbGawZbO4CKFWFDuepLv4qsze8OTm3RFJeVQy03qOcqyWSOu41pab1ZZfhD67giyK5siSFjxNJzqFyRL18mEMvOhPZexQZFKn0r82P6ytXqQOfNta8QYmMR8jcEBmNYTDClWXap7Fx8fXWQxWdTolrq8j8GBFYe+pTqV/35wyGO5jFQExupNzZUVBJ0lIUhxDx4hGNxAN7cyWfo20M2Iaqdw5LRIgxpNLIz3+B6mMfh3KBWiXll+WN4FC8OLx49PAhdDZNi8ZG4bsrGtT0YROSY82Fmulb3k6YrFIPh4Tmxte+57OX4KZKEQNf18jUOyk1lFjpoGEYhmEYhmEYJmB9B9F/BBQ61aDpeBbjhi/V7ivGmEKle8JJ/yvGSAipo15VVcxmM0KILCwsMB7P8brXvTb+2q8+0Vez6eWDonzqgQPnr9Fl2kRA1taqL5aFe3asq9lTn/Z09+o/fXUcDocUZclkMmE2mxFDaMsFO7GGVrzRjU/UG7oW1nXNdDqlrmvG4zEhBH3B854b//ov/9Jv27Jwfq3x2WyQ5egEKIndsVS7LCnV/mN726WuGcLmOWrmWjeoWNqJIM0xtCv3DNkl09MdkpgUu2MjmoWVvH6acrea0PJuPbUV1zbklLX7QtuqO+2UrHbzhDoQN4kJOVNcdlNQo1QkIaiVShp3FQ5/x9vl0yiN3uRdX/xLolN10aXI2iR1v9NeR8FGcIuKy0HsfrjI5E1vQS+5HMoCnVVdansvaLxpjpnchxD37qeYzBDxvZsjy7MRQhXw451Un/4Iq898Ec4NwDt0fYLEkM/RrQubyzFdEphwDin6wf9NXlQuHY0BrSpiHXALi3BwP6uPezzrf/VXDEY7qTR2JabSCFi9jdYuUxL24p69bXNBUES0FXvpiZZJm00OuNl7P4Dfsxct56jrQEiyVbqntGkQUKBHl2F1FSnKTq3ta3mb/x2OiF+9gOq9/4r3c1Qa2w6bXaR8/27Q9p6+wKmsCytO9av24W0YhmEYhmEYxi0Jy8C6HoIG1mcTQghMq4pAKnHr5/RsyL7uG3/0Go+YbSdDaZKpnOCLgjJ3VYsxxi998Yv6x3/6p+6Nf/u3zgmXb1lY/IUDR478J9ceyFwfXVn/f0vz81KF6uXPetaz5y648CJ9wQufr7uPO87NqorJdIJIKsPzhe8yraTJx9K2U1yMnShX1xXeF4zHYwDOO/+r8SW/9Vvy7ne81W1ZmPuqK/yjD+5f+RxN/nhPxAphRjWb4WOJZsdamyP0jZDrEg+Ty0SvI5K6EUDqqmIwHFKHGlxs5RWNgbquqOuK2SxlfVFL11HyRnhVWvfPNf/QLrwqTKYTxuMxk7W1Jk+9Za2qdFw6jvEFk1DLDJfyyXLyvM/rgy8o7niHJB7JJr1DUiaTasQD1cUXQ1gnDufQum6zwtoulM381wE/miNcdB7TN7yF0W8+m1jXUPjOVbZJVRGXPir0yivxkLoLhiqJP9mqlMLRgeiYG25j8g//D7YOmH/5S4lzY3R1FSkKZDRMIentpunnrCWNuAlQF+eTiNmIxqFG6picaqMhflAS/uM/WHveCwif+SzDud1UdY2EkI+XAu5jsz6R7JIUAk33PknutVwm2Sh3co29qGgd0EFJvGIPkw/8Kx5HDEoMmjKx0LSCufOoeE88chRWjiI7d6Yw/N7ctplwOTA/OId3wto7P0jYeynFYHtaGw29hgOCE01z1ITDA+te49e9upnqOUfX4hvziaJ9ihuGYRiGYRiGYQLWdwhbFreo9163bN36rTh8XDl6lCsPHuC/v/Rl3vWud/t/et97uPrqvbPF8eirzvlnZPHKXcvDaKuhHV1d/YvtS0ufXmfyhFe/+lWP/9jHPipPf8Yz6wc+8P5u93HHCUCMKjEGokaIjStKk5AVYy59dBRFwWAwUBhJHaJeeOEF8W1vf6f89V/9lfv6xRet79q29cuzMH3Sgf1HPncd48KXAxbmF1MQ0P8icTgG0HIwbJoQIk50bn5Oi6LUhYWFrlPjtxhfeLz3sSxLD/z90tIpn9q371w5A8qzvDx7ERntUtEaJIjQFFwK4JXkMBqOcbe9bRKkVlehSgJKutgIsynUEeYC4ZKvp+I179CQhSGVNsdKG6Eygs4izo+o/uENDB7zs8jJx8P6OlrVWVVynbI3myVhqoiESy7G4Ygu5Uo5EZx26qVTgVCDKyiHu5j82asIV17K4u+eCXe4YxJw6mnaNrksUHPJLdLrAplL6lQjEiLqs5jmHIwHSZk5/zzW/uK1hL/5G4oja/j5Y6lns65bZa9rgOTGC9pzQDka5VWIe/ai61O0mqUxDIrclTGfVwPUNbGq8Nt3MP3IR+ALX8APlggh4JpmAyrEZjai4MXhjhxF9x1AbnVrmEyRKiRxsguwgroiVjN06zbi4UPU73sHw+jQCEUIyXIp/ay87jqCKIMIq044WHgKFaCyD27DMAzDMAzDMEzA+k5CVfnUpz8t6+vrTGdTKYsS53w2ZKSys5BL9KJGNChB48YuedlpoUjWD5RqNuXQwYNceOEF/oLzL+TCiy7i4osvYDqrvlAUcvGx27d+qBb/NwcOHFi+LpFok5ClB48e/dSuXbvOLcvJgc997rMP/aVf+Pk73fNe9+T+D7w/97nvffie7/4edu08RvsB8tcQgGJkdW1VLr/sMvnMpz/Dhz/8UTn7wx/yF1xwAePSfWj3cdv/tpbhWQf2HF77RuNaWV3mE58+RyS3OBTyg722RYPZjZZMIho1zZ/2563r/iaQysrabLGU2dWWoeV8pVAHFpeW5IorLsPlEKkQavn8F78o02rG2uqaiC+SnJPzhLyXrgNh7NxcSYaIoK4tQ5Oe3S422WLNzzGXlaLEOhDqyOKWRb5y3vk45766b9+5Ky8G98+p8eB3H4N32yNaaz53e9G5k+FsheJOd8edfFvEFzCeQ0sPvmgFGRyIOqhnhL17EHzOCEsd9zYb1lJRoqJ1hYwX0PPPZfb6v2b0spegg2FX/eh86vin2gpMWq1Q77uaAp/2cqON5X2dxq5J0NJAlILB+Fj0ne9i7fOfZ/jsJ+Ef8TNw3PHptXUNISJZVHVNxpRqKyypgAyK7IwCma6hnzqH2T+9j9nb3km84EIG5VZ0vIN6OgVNDqhIbDtc9q1UbS5Yk3+Wyy/j0SOp8+NonDZB6busKQFiPn9RoDEyed/7KespMrcdnU06R2W7b7LAhyeurRLXVpNAPBik8+RxKcm5Ri4Zdc4x/cQ5hP/6PPPlPLMww6P0/Fc9P2d7RXhgP44DLlD2t69hGIZhGIZhGIYJWN8Z+tXa6ursN174wsp7H5y4pJmIyypDLrvTmIQP+gKM9srSpIveyRlPdahlNpstA68GDqRnY1cet337B686ePDcqw8e7r35BpUBRcDt27dvBXje7h07/ubw2vKDz/nkJ08+55OffOKW1/w/d+opp5Sn3u4Uv3v3CezatYudO3cyN79AXQcOHznK3qv2cPlll/K1S75W77lyT33JJZcUdV1fAvzFzm1bDpe3PfVNl332s2vXN64R6Bf+63Ozn37EIysRQqMgxHZCekHtuSaw68SoG8r52rK3LOrg+mJJ/zG+mV+lKMswm6zPOeoAsHrk0PTZz3p2VRQ+xBilK+DsnUOErhHhNSWfzXlI2pQQajd21e7nlH8GCKGuq7nCOTeLUc4ETkunnt4KzzaUQMThk1jWdMrLgpY7Zhd65R60LHJZW9IMNcQktNQT1BeEA4fQS/ekcP4QenPXGHaacTaijKB1RVksUr/+DcQHPBC54ylwdJXoC4h1CmknInUFwyHx6j3I5VfjcGhdJ3+bSHMbpEyxnpSjoSLGwHB8LPHSvUyf/Dz8695A8SM/ivux+yO3vzNs3YIOR7ie1tIG82mFrK+hh5bRK6+iPucTxA/8K3z2s4Q9+xC/QDl/PLNZBbNp280vtuWSnVgl7R7Ka5XFq0hI17N/H/r1r8F4Hqnq5GKLzYXFdOOGAKMx1UVfp/rQxyhlnhADvs1z6xoPREA0gnOEyZT6axfjTr09zGZIkYRCbdcnOSJxAsvLTN/2Tvz6OjqaR8Is5aE1HTy12/GSA+pjbmxwKSr7RPFsaHhpQe6GYRiGYRiGYdwisP+H/hszAO4ElN+SuS/LKVX1hWv5m++JQzflAbR1Rr31rW/1v/zLv3aPo0cPVMDpwNPz38S5VC4YI9R1Rfe0zsuB/wbGu3fvPrxnz57zNu0Z/T+ctxuKH8Ol67C3hLtWaSz/Vw/zHrgUuBLgreB/xrtPPNXN3fN3GOqRupbCObwqLjubvKQywno4QLYsIq5MHfuCIkQkd1GUWKNOkgxz4BAuCtG5LFglwWODgCVdR0hBcZo63um2Lej8HLJeJ2lEa4SQRLUI0Xs0VLgjaxCF2BxfXL89Hj0L0oYfy3KAOIeurqBM0aUF/Em3wd/5tsjJJyHHn4ju3AWDETKdoUcOol//GuHiC6kuvpT60qtwywcoaxC/gI7mCSESqxlobLtAkrsltplf0sZX9cLcs5CXc66KOoJXwtbtCC5pVSKohuxuiuQgMaITmNUUR1YQ8USXxMzWmNdz0kUveAUXI2FxDhktIDHnbIkQNXb5c4A6JYjgr97PIGQxXJIrMgK10oXRZ6daRKmjshXRtw9Vnj+YHV4P1SOOrIb/vIH3qmEYhmEYhmEYhglYxg0WmzbHRcdv0to2Qlb7EHu/+91vYd++fTRffXbt2sWuXbs499xzV65FgGHzsYybtCa6UBT3Wxf9h1cwd/yTtORADDKQVDTnsgjiAM1uK5fLT3EelzstivTaBEZFY8hx5W0seSumJGGl15EvO3YaP5ZznlinQHbxPrmTpNuYMYefa6wQlVQqmcv9VK/5adJu5uzO6oK9BPEl4JC6RqcTHDM0R5/HJM1ll1RzNY4oHso5KEucKloFYgxN5HtO92q6SvbGIHT5X42K5boAdc1z4DTiNAtKrszxVU38u0sxbhqT4BVrXND22rUfwp5LZKM4xHX2N0/udkn6vcaY5z87CCUmoVFTOaV3PuXUNTdc4+pDUJfWszGHqSq1Kosi+rqxyu8OZnt1Mjnt4DqXYwKWYRiGYRiGYRi3IKyE8IaJDt9KvlVdwhQIPfkAgLPPPnvlut7QE7U2X3O4Gc7bDRKLuHlkATVjcUCoB/LopVpPOCm6IIoXaWSn2HYOjE0glwjBFTSCUWqilzoUSnYQCblLYKyTEaonWKWscN2wK5rSxySSSSp3LEpUXN6MqYyzzSzL50JKVEObLaWtkNZ0wuvpJb0mhq22EyGGWRKcJHURrGWcq3Gb4+Vpas6hzXsVnU0gxpTt1XZL7IyK0ql1KSSdbkxtDJZ245O2XFWJAloMsmjVlLum72vpyobFDVKgewztOZtmgG3xZBakXO9GVOfB+5Rl5bpyznQuyV06C1zutKhZfYwS87V0pavQU87ypqoQLneR4JwO47CAqX1yG4ZhGIZhGIZxi8IErBsmBN2Sxi//S9esN6PrvjmNhSrG2baIHiuOGJOQ5JpMJO2ywNrEprpGECQmUUlc7taXM7kavaqNZutSubogcnp5Yqq9zLAssNQRydFJ0vt9t2HqFBafFZu+82hjZll2EuV3aRuWD0Rpc5yiKBpm0HYdTO6iJNSENsi/nRMFJ25jbhldhlp/otvrpCnvky64Xdsp2Hj9qqBV7qioeJXW/eRF2sys9E8WnHpB8I3TqwnXlzbzLjnrVGPqGqmaJLemHLBpAEC2S7bOuBRE3zjSNga393LpNdXGrolyhSiFk7H2YsQMwzAMwzAMwzBuKdiDznceegO+jG8hISALeDnGDZg1QeN5ZZIYpb1iwNylLqbX+SyYuBwmL0llATS5htpQec3/1Sb6KZ1CUxVoG2ae/V7Nsje6ECRNxrXR6CkcXfMx0Vz5ptoeL3VsjJ241IaatwoWaEgCV77GpkyxzanKnR37vxN1eFyum1NUmu6PveB0uowv7c9p7zj9Dd7Mb0Nbbdi/ftI894OtkimuEwFVu3MpMeda5WNpekMzR45Ui+saUS271qT9IJZWYwuNS00hNLMr2QqZRTtRxUknYB30EKO+b2FuegArDzcMwzAMwzAM4xaGObAM4/+A7Ti2K8zIAhW9crccJA4905A0MtJG8UNF2w5+0nM7Cf0WhK0fqVd61nPzkF1DzVF6gefpB0fbBS8fpj1KXytqhbcO3aAkNaVw0rmyei6xqF0Pw7bEMbuaaMW07FpqxDnprkd655OeTIVudCx15Y6yaSboBd9r+xrtXYXk0HpB+tWI+QSxnRuh1y2TRhxLx3SkEsXk/tokpeW11X4NJjG7trQdo2iai6hQiuqKVz3gRNaDvvngYY7Qa+RgGIZhGIZhGIZxS8AELMP4P2AHUGjFChG/oTgsh7nTiC4RkBSknkvWmjI1JGdkpXjw5EyiF3rW1hb2gsCyGBSzRKLtcWndW+IkdyKkzYrq6z8uG8Jcc5Imh0uly97Kr9aeitOVSKbueZrFusZFFVXbEsiGVnhrA+J7pXp5jpqyxL4SFZtXSnvKXHHZk7m0c6H136kCMa9D41FLOVoFnSYkXb5Xb7SN+0y6l7THSfJcDmdX0tyo5LLBXs9GyW63fMyYRTtt3VrJedXLjedKF1kT2FboeN3cV4ZhGIZhGIZh3AIxAcsw/repA7cZDNiKEkPA41JpWStRNKVrbJAiRHrOHOlrFNIqNW0GVBvPpBucQJpfGlv3UN9hpF25XROS3pq2pCep5DH2xpcMWxtr8PpSVuPU6ofEa6/hZhRQJ70C1iwUSVflLL2wLdfP9kIQSUHsKVF9Q8Z5OmMW3FrPlW6OSGvKMpuCytgev8mpUtFuFlR6ol4XYu82HFN65Zsb+wmo0xRq7zYUYKYOiG3xaCLkPo2aO1D6ngBXAEMn7JUI3sciFoGbR+MCwzAMwzAMwzCMbyomYBnG/zKC6KAchAtVwpHoKMTngPCYc5IkZyWBNkYg6VKrOpeSZEdWkw2Vj57PknKSpOe66pe4bfIX9YKiFLpAqDYDSnuSkfZKB/sZUM31aesMitkV1riRmsiuNqtetCt5zA4sFXCaOwm2opPL3Qu7XC1Hel3Kqmq6Lm4Mrm9Oqu2AG/lNu3yw/FLXD3/vvV0RgtD0hgRckrdcFq3auC/FRdkoVcnGb7qCQWnz51Vb3S3lX2V3Vpc9LwSFWhWJghPwojgcEiOjgePiIVEKSjdV+0w3DMMwDMMwDOMWiT3sGMb/MqPSD980W/fvjNGnEkAQn1w3HsULeE1CVs9clT1BjVCTjqUqWUySDalJG5SZ/H3M1iTfRaS3Ee5J4Mrf9LKf2jI9FXxjx9LOIaboBvGlfXdWxUIXKY8qhNiJZs37hSTWNYHxyT0mnSjUiD+umY/UGdCLUJLmyeXyQC/pOM511jAlO7x6mVXaK0Fszus3uN2kDZgHqFuXVsrAitJVLTYiYSNkxXYeuvJNcnWlZEdcM39dGWDozZO0rqvGlqW9MXiUQqAQRwmULrK/dD6EeNm00gvtDjMMwzAMwzAM45aIlZkYxv/u/aajUfGDs8g9ojBNUsh14a67T2jsf+Pal2+I7Xa9P0fA5R96b+kOsfmNvZPEazv45pNc12t6JwrXegHXfe3X9jq/cWpc/7Xedy9zG98TcPjeZPTf56850vyDz38P6fsYCG7jMDZcVrz2KbnGa2Pze59/DteYlQBURAgQY3OogM/X7b3Hddek88JgpvqZvYcmn7DbzDAMwzAMwzAMwzAMw7g5Y/+nhGEYhmEYhmEY9rBjGMY3BWf33rcPZwBnfYuOy//w2Gf0vj+LXiq+YRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRiGYRjGzR/L4dnEi8Gde8YZwlldMs1ZNi2GYXyLOeN6/n49n0OWf2UYhmEYhmEYhmEYhmEYhmEYhmEYhmEY/1eYAyujioigp97qhMeuTVbutbK8MgWkjpFQK3WAYNNkGN9WH24K+Ot5nbuO37mb8CGp34QPXbdpTJHOWlUB9cbz6AKUW7YUX7/iSP2a/GfDMAzDMAzDMIxbHIVNQeIlL3mxwJnqvD7igQ940MN3H38CVVXjvEdQIg40EjSiUYkqxBhRhRACUZUYAiFGosb0txDSk6ZI9zRN/rn3IKuAoOT/tk+5giAiIIJ3gohrD6MKiKIKSkQjiGo+nqZzqIIoMXQ/OhHyW1FpTiVpSJLOlw4T81+ageYzax5xfm3vAhARHA7nBJXm+rrH9RjTPDWHEdHmDBuOo/k62nMBSjMuzdMnaXz5vNI7TDNn7auaoWs6dowR0LxO6VrcphXR3rGcCOJc/wR5AfKcbVIwtD1CWh+aNcpjQOM1VI9Is6+6OZZNgkh73dKfG9dbv3y4dmzgxLXrpHnupNmTvZGrahpzHl/MQxQv6QxO8nWkNRMneWyC5OtRNm6RZtJj87p2HdJ1amz2KiDpOsh7wjmX9/xmYUjbNXXtreVw0l93pRsdOOfyGko7ff15J/ZuTe3NkwAqONftJ/LeQaVTmTSNW5v13iBUabfuKI5ujNqsVbeV0hhpxku65/MeU4UYlToGQh2oQqTwQy46/7/5r8995lzgL7KAJTdQSzMMwzAMwzAMwzAB69uVoyvra2f87KOrh/7kwyPXbs4wDMO42fC2N/0dv/SYxy3DycAlNiGGYRiGYRiGYdwiMQFrE4vz4+G//9u/lJddfjlrqys45wjZMSLZRREjVHWkqivquqKuA6GuW+dVzC4t7dsxGnsRJAdOdttE7V6ngHcOwSWniEuvd9ntoa0DJr1akkUjO1IaR5RkB4m0rppkkGk9Jslho6DaWU+0NVhFCl9QFAUiJCdZVDYWLWn72uaY7flF8BscL+nAMXQOtWY+XP5fyQ4n5wBxOHHdnOXjOuhcVX2nmEYUJfbmu3MctR6ydExpXC8xJV43DjnAFR4nm9xg6S9ARGOas2R1y2Mgu+IEcHmMjYlJI6raudBEukO2DitJDqDsRqJnimrcVq71CzWuHe2cbAjeewrvEfGI5Cvuy655f2k+QWckyk42ce3ctqYpyePOEq6jcZWRHEj52ps9EHvX1Fxfciemn513eSh5fbLLqG/ZitqZFT2CumZD5uNoGnuzr0SS68o5l52J+Tgx5HtE2/vCFx7v05dkZ2MzLyGGPIzOISYkByGNsy+7z0S121N5rdodk51t4lzrlmpcVtKb93Qvpf0RY3N/arsxmm89Dnquv+YWVNU0tqjMqhmD8Txf+e/Psbjo51aWTbwyDMMwDMMwDMMErO8YfFGc/973vPuL6//45jURXIiRUEdiSA+7IUSqGKiqSLAine8I5Dq+v67Xae9fuZHn0RtwHu0du3md2zzOLK44yeLQpr3alI866d7fDkA2WQ/zD21ZoV6zPq2vcSpdEJNumo8bO+/NV5ML5fO1OUnfew89Palf4ZrOmbTglIPlN/7t2iZarmWuhW/8GmTDNLHZutkKeI26GbtSw1pBe+F6IV7zHL2K0LQGSVMlALOAzg0ZjOcGX+rebeWDhmEYhmEYhmHcsp/NjURBm/t8KnBh+4f+T6eeeurGd516Kqfa3LVcuPmbU7lB83Phdf5wrb/orcyNOfKp13zbhdfxGi685usvvCFjus43bNpNp96IGT31hszazYRTb/x63LQddsNefeHNbCpu5FY+9Rte24WQdLPKPnkMwzAMwzAMwzAMwzAMwzAMwzAMwzAM4/8Ac2DZvBiG8e2PlQ0ahmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmEYhmHcDBAs7N4wDMMwDMMwDMMwjP8FTIC4+azD5rVQvrWdxdz1/P3azt+MM17L75Vvvqj1rZ4D4+azf69r73wz9sA3+/66tnsn/i/dp//T+fhWfdbIi3vHPdPuXcMwDMMwDMMwvgUPnsYNm5vvxIcxv+n6I8CJJ5443rZt2+gIcOmXvjQDVm+Ge9cenv9n97jeiOPrDRzDLXpNzjjjjN79chZnnUW8IXNzxhlnODir/cWdzkLP/NYJYt90Xgzu2sZ7xhln+LPOOivYLWgYhmEYhmEYxv/lw+0tHlVEAJUNE6XfivkejUa3mkwmJwPNw54HrgAu/h8IC9+IMXA3oLiOv/vhcHj5dDrdcP6dO3feYzKZzFeTyTOndX1Xgbh1ae6y1Un95Nls9tXxeHyP9fX1MRBHRQFFQVEANdTUTCY1C6MRdfrFpr/3qCdQFF6kvHh9ff1yNookN3bP6v9w/1/befXbaR9/w6tV0ka/4XN3DWF32/z8XQ6trm4jixgjkMHS0rlHjx493A2jffO1zt3Cwv/P3n3Hx1Hc7wN/Znb3+p16s6xiy7bcjbGNwRQDIYTeZUroCQQIoRNqkJWEEiAQegu9BRQgCb2HXh2aMe69qbfT1d35/P44yZiWEBIIv6+fN69D0ul0tzc7e3f7+DMzKLZde0x3yt0QhEQCsBwElnWlUqvw9YKyL1VeGBkb74wXxweOr+IArFheeP7Slv7Wf/vOahAY2hec2N+Z9MGGBAJQ2hfMrulMvgcg8/mbNzZCNzV9s2qkoUMR7FmPiX3uhuNUlRcF0+s7kh8CSH+T/lw/pGhUR2dHWXsq1xb5AVhl4aLFCzo61n3D1xoNwIwoLxxbUBAq/uiT1e6wofl2VsJLF69Zs5rvJERERERE9N/CAOu/04Zf1o5fNczui2eAWjeGQ8HZts+XDgTDiPf0+OP98atE5JRvYVvFB4wJhoKvRAoK85PxuDHiKWgLWmnYjiPiZpxkon9OPJW9H7mQywMQBXA8gKJYOKh32XV3aA088pdHEPD5r+mJJ84piIbeD/j9dVk362YyWeWJbIgufI4Dn88PEUEmm4W2LBjXwDUeRMxGDShwLNtzHMufzqbP6olnLkUu0PP+w/1j/sNjhBVdX6IwL7xTxO+7fcyECRWWdryutjbV0dFmtfX07N7Vm3jqa9yFBcArDTsHR8LB+/ILijKWL6B8fr/pWLfS390Xv2B9b+Y3DYDV/G/2gYFsTmpKwo+OrBu5R8XQ6nRHS4tK9ff4Vq1dc+Si9X13Dt5v4+eG6n3JEDgFQGJ+f11dVdHrsUi0OJ3MeJZWVsZNrV3Vlfjx0MLw5tlMymeMgW057rqkvnv9+vVt/+xYrB9aMCHot/bMptPZvs4+RPMjPssfnP/hsraHCn2+sWPHDH25fuykfKVts3zJQmvp4kXrOuNm6+5PQ71/q00m1hTeXVtVfWjtiPq0MYJ1Kxf5Fy5eduJHq7uv+3ePi8bGRtXU1GRG15bPrM4P3OVY1tDxk6dmP3r/HX9XV9ergUje3e+vXP5AVxd6eKQQEREREdF/ymYTfPGkcmhpwQTbsi+MJ/qQ8TyxLZ/P5/ctWd/adSaA5BfPk79WuCEAygBYVVVVCIVCcF3Xqqur63/ppZfy99xzbzn/V+fbPr8fvzjxRHnm2WdjZaWlRWvXrg1uFL4oAF0AEv/Jk8wA6RlTplvXXn+N5fgc7XpGaZ2rjbFtC48//rj86pyzp1RWlE8JhkIoKChAZWUVxk+cgEkTJkhtbbVMnjwVAuPtuefu+tmnn9MTJ07EvLlzrV+edbbafc+97N6+uHJsG8YzgNbw+XxYtnQJXn7x7zjk0B8D2oKbzSKTzkKUgRIDKA0oQSQUVr86+5fy/Muv6M8HSWVlZSXapE7s7embnsyYFABtKcCyNSCAGJGA3/aFwtGPYkUlVy5YsGDtQNtrfC7EKi4urqiqqvrqEDeRwHu5v0d9ff0QAGhvb0dHR0crPlc09n0yOKSrrqxoB8dv/TKeiGfEM1oNRKiDndUYgecBnhgY14UHgc/xSywaUf0Z9/ctHT1/x0bBa15eXn5PT09koB2taDSaMG76x5Hi0qEXXny5O3rcOGfuB+/hpJ8doxauWpcPYLuSoDrbdSUb8GsrGAonunqzZ3SlUisH9+dYwJqHmao/+YpVXBzC9bffbVfV1Kq+rk5z6Kx9paW108Kn4ZL6J8fW54/jwe3WmWTC2na7HeSXTRfZfT296upLGnH91TcqAGidCSUvQakvDzi/EDqn0mmdn19kXXTFNbqkrEItmT9XXfSrc4oK+uLXOZDxBcWl0Eqju7sDeejbb+iIip7e/szNC9d1PCaAGaxAa2iAbm6GF/I5MwqjgQt1LIqpu++J9ra1ePud916vmTnz8RUvvZSuGTbC+v31f7Ty8gv1Hy6erS79zW907eiJiffff99stK8Hg8DiqqoqVRwKDbxAJDa8UnimX8c9Xzre1xEdN2GyXHT9H20YwQ2XNslrb/6mEDEUFlgIax3MBWKJJDqS6MCXV3qhAdBNTU3eyKH52w0vy7s71d1WtfUP9jBnXXa9/6XHHzF33nj5Nt3d3ZNivrxnutDTg/+ggo6IiIiIiIgB1lecsCbTmdJxY4ftWTNsOJLJFOL9Ccz9aO4iAOd8yYltqc/nm5bJZGTgLNJStt3uuu6b+HRicwDY27L0ZcZIcNWqVRtGVC1ZssQAyM9msmrMmDEaAHp6e5TnurPWrVu7k2VbKhgMoqa6xqxbuybY3dN7tjHmNnzDqiQRUUqpKdrRvpKyCgmHw1AKYlkaxjPwBwMIRaLqqKOPkbPPPc84fh8K8/OgLQsDQYICgL6+XvT19eof7rSTfvrp5xQAuMZIcUmpTJg4Sda3tGLtmtXwh2zkFxSicuhQhCMhrG9pwfSttvqXmxmK5cE1nxm+J6VF+T92U/GLEv39Q+vrR+lp06ejbsRIFBYVIxgII53JYM2alfhgzhy8+/abP+pYv+rg2sqya/pS/X/t6IjP3+i+dHFe3tGpeM8F773XDmujxAMbDxlVsAD8HgCWLlxwuhFk/T7tLyoI3xzs6r94NZD6Pp6UzxvY/GQ2XlNbN2aXHX6wE4wA6XQaGgqiAEvZ0JaCCGBEwfM8FJeUQNsWbrjqCiRT6aqhQ4fuuHr16s7B8C9k4bdDh1cdqLRKdHb1WJ7rma6+TP6IkfWmrm6EHQ6F5I3XX8Pqtetki0n11wQDASsWjRTUjx4DyaTx2F//6mZSqd9svJ/nARngJYT8vqRSkIKCIhkypBL/WLdWOjo7lS8YSCObMq251yr5Z8ftF75vhEITjNaWW1hUrAKBABzHltWrVsNyNCTlqdmzlVEvAeNqKrfPuL3h9rY+M6SyyF7emnq7v7+/5UvC31QgEJKa4SOkorISy5YslPbO7uDkydPG73Xgwd60LbeG43OwcP4neORPd2391qsvIhKyx4VCobdVIrHu80HO6lWtmV2PPcw756IrTSRWpO654TL5x7vvblHVu/7g/qhvvfK8gPFyt1+zZq0k0sbfvWb5rjXFBW0epL+pvftlACgJORPCQeeheMuqwFIXYivA0oDPARwH8PlsNSQUlr6UV9TT3aMAraGB7nhSFeaFT6/Ljx0jnqfFGBHxoApjuiedOmzeyp4XPj/H1cDPXlVFZNvaspK7+nvaq7bcegdz8q8v13YgKDs3HKqisZD5zdmn9fX0pBlaERERERERA6xvS0dPv7v5lC3cK6+5QQGQ5194Rh/UMCvxuRNoDcBYlrVlJpP5a0lxMTabPBnPPf8clDFvA5iJgeqFxsZGq6mp6Yzx48aPuKDxAqxatRrxeByZTAbGGCSTSUzfYjo8z1NKKZx26qk4cNascGFhUTgYCiISjaCurg4/P/54PPPMc9H/JJxTStmRoP/UlrVrQz8++ECTyqS149iwHQee66GiogIrl63A1M2nqNKyEksAdHV1wbIshCMRfPzRh7j77rsw95NPsHzZ8mxry3onHA6mOzs7jQZUIplQnufh6SefVr+//HdI9Pdj5113wTXXXAvP9fD4E49iwfz5WLhwIWpra3HhxRchPy8ft916Cx75y9/gsyxUDBmi5n78oQoEfHYqlVaAcitKSw+O93ZcV1pclHfuOefLvg2zzLDhw7/0iSaTSbz26svqxmuvqXz9tVcuKQjHjrPFd2BLZ+fbIoI8pfJcL3nWjjvuOHTGNtuhp7sbIrk56rVSAwPPAAMDN+Ne7hmBZ1zkFxRi0YL5ePrJJ0/rcpwHkM1+jK85iXnDQBVRM4CG3NcNk+L/tw1OBx5PpLP+QND7+Slnm/zCIm2MB62tz/eJL4aHgaC66DdN+UnXtZDbXtUMIN4bLzqo4YDiQ396LNauW4dEIgHjeRg/YQKieXmI9/epLWdsjT//7VGUl5UWB4IhRCJRE43lmQ/nvKn/8pfH+vs/fc5SHI2OspH4SVufl+xKZibrrj4V74trY4zq7u5GX38S/oCza01Z3v1/X79+hVLqMyFWw4MPonnWLAwEuYP7QQDoKVOOtVTTzdlxdZXb60xys6qaWgGg161eKauXL4dYRiulBIAaO2zICTrbf3lJLC8wYfR4rF2zEiOK9d86LOuoVb29XRs3UlnEd3BpSWnU8fmUiOCV55/HjG12wG+vuFJKKyqswdC6algdtt7+B+aypvPlgbtvragrjx720dLEpYPb39oM1dDQYL38t2a9ZNFiK5MxCoAat/mWyCsoUtlM8qgCk6mSjBuwbUuMcZUYQXFhQWE0Gryzq7MNWqGrrqLg/t5E+pZMIqsKorr2+BN+apUNqYKCQjgSRiAYgM8fgGP74Q8EkHUNyodUQiQ3u98+Bx+GGTN3zHO0zvPcDNKpJGzLwmMP3497/vyX4GBihVyZl2qcOdNqeukld2RVxbZDi8P3pOJd1dvtsKv55YVXan84CktrNe/9t809N1+ntWVbhQUF0r1uHd9UiIiIiIiIAda3RGUyrs5mMsrx+WTVitW6vz+pv+yGnudJwwEHyHnn/EpGjRmJAw+cJY8++tj4YDB4RDKZvAmAtf3226OpqckMraqR/fbb3+Bz8+18PkjYf/8DNoQJGz9UIOD/T+aC2iC/IM+78KKLUDt8GBKJBLS2AaUgYhAORdDe3obzzj0L++69F2KF+Vi3Zh0uOP9XmLnDDnjjzbdxxR+uloCtVNqTPyulnw0EAm39/f0lCvAsy4ZlWdh7372xz357IZNx4fM5sCwLwVAEcz/8GEFfCPvtvz/8vgBCwRC01vD7g6gfMxq11TXy4AP3qfXrWrKxcLg9lVJSUVH6476urhvHjhkbue76m83ULbfUAFRPTw/crItQOASlNJYvW4ri4mIUl5Rgpx/+CFvN2EZmn3+u9+B999RGQ759hg6b8h4ANzY0hs6WvuxWW20jZ51zruBrzAWXSafh8/vlvnvuVo/95a/ZTPYrRxBaDQPfjAVkXgNUczO8jedvav5cqPgVYeM3NhNQLwHKsQDjekgm08gHkEgk4ff7cw8oAs8zSsQoYwyUUnCzGeTlF8iMbbZTfl/AW7ZizWe2N+16bsXQKm/zqVvI5p+uUKmMCDLpDCLhKLbcauuN+67Kuq7KpNOqvaNDi4Z2AJ0R0Uopk04kRtbWDfnl0cfuDVc0iorKUF1bo7TWGDtmnN5nn33x9JOPbdvT0/aEo1RL2IIVCPlhPCCVTOPxA2dJZXFUu7AvaWnvehyAGlJQsGUspH7TtewO/7DigJvt6xqZn5dXOXxUPQCgq7NTKdvCiJqas8r7+g/Tllghn5683a57BQ458nhTWV2rzv750fLWm6/tFikMT1W9vc8NPBlRgI75rX0qh5T78wsKTG9Xl/YHfDj9gvNRWlGhOtpaMX/uh1I5tBo1dSNVIBjUp5zzK7NowTzfG6+9fFhZWdkdLS0trcjtHxfNzRg/rCi9fPFCvPXyC9h1vwPVyLETMLR2JFYvXbztjrv8EFvtuDsCgYBKZ9I48fTTccwJJ8ATJQs/nid33nJ1/ryPPjihLC+y9dJ4+/mA03/IT46Pjh6/+T/r05+5fsy4iRgzbuLGfVAA4OMP31Ptfd4X5gBreukld0xt+XYlEd/d2UR39e57H2xOveAirW0Hjt+PTz541/zulyfqtWtWpLJ2ZHbBkHXrZC2UUhw+SEREREREDLC+JQI1sDxbMplA1vvqKY9EFCZtPhEAcNlll+Gdt98NtbS27BeJRB6Ox+Md999/vwag4v196pWXX1ZGjMrPy0cgGITjcyDGIBaNoaS0FACwZOkStLa2wXM91dXZifWt6+EZVy1YsEh9Sfj17z8z16C2bhgmTdzsS3/f2dEOJQqPPv7EhuvOPuNMAEAkFgIApZQFR7l7eMbM9GkJAuZcAMnBRe+WL1+GJx9/DJ6bxfQtZ2CnH/4QrpuBMS723Xc/HHr4oZ95zMOPOAKHD2xeV0e7fvuNt18sG1J1h8/nHNIf77tuRF1d5J77ms3I0fU6k0nj+eefk99depk69ic/wSGHHoZMNoOzzjwNq1avlXPPOwf77nuACofD6qJLL7PaO9rMc08+cWymbc09Sql5Q4YMUbaTDBoxX68j5E7fFQBkM2kIjN/nOCqbzX5pptm88U/NQF5eXkFRyNnFy3gqmU0iGgpbac+0rm7revqfPuY3tD1gXgIkFArZqWTSuveOWyyfP4B4vA/iusi4WbiuB6UV2lvaUF5RidPOPQ/hSATZTBrXXXOltHW0+YcPH66WLv10IcqiWDjwyfyPrVtuutazoJXPH8DULWdgWG0tbNvCE48/gYWfzAWUINkfV3293ejq7FC9PT1W+7q1CGgR7UdCKWUAoM/z3IqqEd4FF14mfn9AAbA8N4tkMonyIUNwyGGH44933282H10/ZufdfjRm0pQpKCoqQjrjYt2qlXj5hWfx4vPPIOCYW6oqCo9cta7z2cKo/bvamqptJ2+xDSLRMKAsVA6tkRH1YxUADKsbievvuB99vT2jFi9aOOrJv/0Zb/79BVRVDZOJ06ZrBSU/O+UszD/uCN2bSJwF4DnkqsYUANiWSpYNqYBt20ilEthj//1RP2Y85n0wB5f+6mysXLZYR6JhHH/aedhl/4MQzcvX+x14qPfBnHfG5TnWz1qA34gIRlfkTbUdX12yp2fbRDqLTz54X+2634Hw+QI48ZcXwOc4ZtKUqQraVgDgwC+1daM3hEljJ2ymdthlFzn7pGO95596YlRNeeHekKzT2rJe5RWuVvHuLhgvi1QyiUw2i2wmC2MMXNdDxdChqB+Xe71atmQRWtasVlAexPWgFFQoHEbL2pUoCVlYn/Awb15uEYTCIIbW1QzbRXmJsxzLVB913NnmoJ/+QmeMgd/vxzuvPCeX/+pM3dvZljLBvFNemrPoptyLBRcLISIiIiIiBljfGs/zBoaVAUZkw/ef5/P5rD8/1Kyuu+46/PznP0d9/Wh96umnylm/PGtn13V3BPDgzTffDNu202++8Tq2mzlTObaNWCwP/oAfoVAYyWQ/Djn4x/jd734HpRXOPvts/OWRvwAAXNcFAHE0VDaXt2T/8+eW9Z19xlnQto1MJgkoDeMJsiYNW2uk+pOYPn0L/Oy4n6G1tQXiCYaPHAljDEaNqsdpp5+K8uJihMOhvHAklHfv3XfjxVfeVD6fbYyX28ienm589PFH0EYQCcfww513BiAQGPT19cIYgxdefAG333ILxLhwXRcX/e4yjBg5Cl0drVBKpRcvXpweWl58YjgUzLv4d7/3Ro6utzLpFK699mpceumlqq83jtNPPSUXurW3obOzHUuXLFZHHX4kljctl9POOFM5jk+dePIpmPP2m4XdXb3HjR079ox58+b1FcYCt//jnTdnX/Lb2aov3gfLsmE8D8YYeMZARCDGAAIFKHjGQyyWpxYs+AS2ox/Q/dmV/Z9WUCkAEg6jNOKPnJVOpSMwrkDbSmuV9St3OGDvGomFUJFXjt6eHliul6gtLzl++fq2uxoboZuaNoQkgYllE3VL2b/ej2UA0LLxDzmPAMCHH0pGqzlr163/+W+bfu1mskY5NmByc+qrjIvkkCGlO8LLHn7ET46TQCCgbNuRN195EX9//hll+507tNaDk28bAMofi17z6N/+uv6FZ587Ptnfh7qR9bhrq63hDwTw178+jF+ccBxCviDcbBKJ3m4pLIiqeDzx197+1AelhXlnm2w2UB7y/97K8z+wtLX3fgAqnU3BGKPS2azu6exENBaFz+fHx3Pn4sKLf4eTTjhBn3bGmVIzrPYLB+BRxxyHP919p1zcdG6F57p3jRhWdVhfdyvGT55mfn35VYP75TOrhEZieRgRywMAmbzFlrLTLrvhjJ8fo6698nI1bMQY7HbAgWryllur3fc9QO685YbNNx9VdcA/Fq56EI2NCk1NsH0+XVJeDgAIRWMYXzEEa1cuk0vOO0MtW7TAzS+uuKutZfWud95ybdnkGduqsopKNXHqFqiqrlZLFy0ODqS/MqWu9LBQwHdS5egRmLrl1thxtz1VOpUClMK0GdtiMKhO9PWZeR++h08+el+vXbUaRitM33omdth5V+TlF6qmS6+ylixcEGhfv/qnth1MXPabX/Ub10MqmfDEzQa9dNpp7+qEVhplJaVobW3D+KnTcd/jzwHQuOUPl+Oh++90CwvzM0qy4lgWbJ8jSlnWkNKYt355F5qbIWXhcOnoutJbtUntXFxZiVPPu0S22uGHOpHoRygYwlOP3GuuufB87WayyaT4T3slF15pfP1FLoiIiIiIiBhgfROZTAaeMXAAaEsPVGPJ4CToG06Kwz5fezabbb3q6j+U7L/f/igpK8ahhx6Ku+++R+Z+NPfUmkmTnl7xwQfdInIJBFvsscfu0ZLiEqxZuxaJ/n6I5Fa2KyjIh9K58+yS4mLM2Gor1A4bhkg4jG2220b6enr06aed8UI8kbh/YBP/3fmTBlY/a5AnH/vrkoULPpl67uzZqrSkBIl4PwAFbSlEImH8+c8PYruZM7Hv/rM2/LFrPKQzGUybtgWmb7HlZ+73zbfewnMvvjrMH/CHfHZuZNlW07fCzO1mAgAS/Qlks9ncnEnGwLYtaK0xbNhwHHDgQQj6/YhGYyirGAIAULYNbVlSV1NT1dmyJvzjHx8qu+y+mzIiePihZlx2ySUpx3FWjaytGjGkolIBwMpVq7BmzepkYV5svidSf8WlvwttvfXW2GrrbTFmzDhM2WK6euqxR3f2PM8PoC9kBR965+0393jxxWe1goaIgZjBM23JNa4B9OCk7kpBRIw/GPSHg9EH1/R39W50gg4A6O9Hwcjqsp+fcebZfn8gANcY+HwOfLYPlm2Z/MIiDKutxV8fecS9+fqrQ4l4YiaAu+bNa1BAM4LBYEVpNHD3mv75xbLIM55A6YG7N4M5mQKsgTxmraVgaQuWpWCvsaCs3PUaMJuNrAwK9IMfLFrVOLh9SffTnjN58uSJdqb35KrqYTjq2J/BcRwYz8XDDz2E7u742nB+wQOLFy9O49PVG/Xy1etfHjassjvf5z/ei8XQeOHFMqK+XnV2diCZTuHBPz+CiZMmo729FffcdpN6+E/3wu8LTlToqPQHgtb4qVta8b6uvd9///0fDisrXNLa3umtXb3c+tnRR2D5ihVwtIM777sfQ6ur0dbRjp/89FgcfMjBAKBa1q/DggXz0d7aJsWl5ZgwcaKOxiI49OifwnWz3iWNZ5cFtdk3A/iy2TQy6RTifX26v78fkVgM+XkFAASvvvgCVq9cLuM3m6SG149VBUXFaDjkMLzy/HO4/7absMU226CkvBL7Hny4eeWFZ/JXrVx94B5TpjzWPG9eGoA4joPColylpOPzA57BfbfdgKWLFiZD0cJLH3v9H7+eOKz01O7ursuXLlpoyocMRUl5GWpG1OGDd9/b0FfcdMb9+XmN3l6zDhfbtmzHH4TnGWSzGaTTaXw05x28+uwTmPPO63rxwgXo7O5aaDwv7tjOuPtvv8H/q4uuxAGHHYXS8kr5wc4/Urdec+XHTqTk6Mde+kcGgFhA5dihBVdaMKO233kPOfiIn6hhI0aibf06tLW1IpPJwufzG8tydUd3+nde0NzhaC/mJVNeFlk4jqM6UoHB8jtTXh4pMdn+bUeMHm8uuPR6VI+o1/39/QgG/Lj3pivN7ddcpm2fLxMX+7TX5y69URqhVdO3M8cbERERERExwKKNpJNJGC83bZHf8cO2tHJdg4GJnzeciHbF469GQoEHFi9a8osHHnjAO/mUk60hFUNw2GGH4uxfnjWxc9GivQDc5Xne8lAw5J533vnYcsstJZVOK60URAnEM3B8fngiEDG49LJLEfD5YTu+DSHR2+++aeyArw2JRPfng5N/g2pubvYAXDy+rGT3A/Y/IJYXi20UUhnYWmPt2tV4/vlnUFYxBO0dHYhEIpgwfiIKCgrQ09mJ+YsWob8/Dsex4ff71bq1a1AYC55ekFdorVi1Bn/8462qtWUdjBEkUklk0x7y8yMQ8aDNp+OJQqEgqqtrACMQ5KqfAMB2/ABMMhWP/7SkqHBiw4EHCQDd2dGOm264USLBYKq7p+eDouF1daUlRQqAWbF0qW5vbV/Yl3K3r6seekRPV+elTz/xhH/LGduoUCiM8eMn4LG//iXZ3d1tAGB1V9d8ANtu2bClwqrPNlIV8PmrNlj95ptYnauCU18SInrFxeU9Bx56eIHt+Daeh0hho1X0fnbiSdbC+R/LHbfengQANOcGHZpk0lc1dsTkXffcsyD3xwpaa2w8bdHgSolKAVoraG1BKwUFDUGuckxB49W/P4MFCxbWNzQ0WAUFS3VX13DT2tqstsdMNL30khtC9spU1pu8x977m6qaGg0AH334gXnxxRcspa17li5d/VFDQ4M10F8GA1A7oPX+be1t+tjjT8ZOO++qerq7EY3FcNCsQwAAqWQS1dW1OPPc2ejo7ML777wz7MDDjhy21TY7YNsdfyAvPfeUd/isfUPpRLbMjvjntrZ1XP/An/4cjgT0gZVDqgMyMKxz661yq/ml0yncdccdcvsf/6iWLPgI2ayrHMfBDjv+EJdfcx3KK4bg0KN+Yr384tPm3dde+UlFcYlv3gfv4cwTjsa6tWvQsr4Fv73yWmy7/U5YvmQJLjz/bKxcskBV1dTiitvuxdhx41E6pBJlQyox5+038eIzj2P/H/8Eo8eO19v/cFf50x1/3HtV7/odZjXPeRyAbdlWbmgiAL/fj4Vz3zcvP/O0Fsue8+ycj38LwCSSiSd7en1H9fV0jwNgwpEIysoq4G40Crm/tw993b0qGI4YL5MyCz/+UEEpNWxEPSzLwt233oQ/3XlPZ15h9EE7GOxe6RRdn2pbu2rrzUee3Lt+1YXPPPm30F6zDlH+QBBFZUMgCol5q1veHeyTU8ZU/8j094/aZ9ah5pe/vVw7fh8gLkrLyuAEo0imUrkhhRkP4ZA9LFxYse7DDz9c/OkWZgEkBrucLGlp8babODp9+uzLg9Uj6iXRH4etFa7+7Xl45N7bdDAazXTHs6e+tXD1jSKc84qIiIiIiBhgfWcGVwkMhcO5uaoc202lXR2NRov6+voUAFRXV3srV67sCuRF7kgkUvv85S8PVx1+xOGSn5+v9txjT3P1VVcF161dd4SI3KOUsm3bNplMWlzXRSqRRCwahbYtiA14A3NsCQS2ZaOntw/x/jiWLV8ubS2t1htvv45UIhX8XCiCjcKFf0d2+fKV8pMjj4TneZLxXKW0gutmEfT7sWr5CixatBAPPvhniBho7eD222/HbnvshdffeBPHHXcMjJuBUgrBYBDiZlPhUFSdde4F9vY/3AnJRBxZ10UikQSUgs924Pc7KCwshG1ZGJhaDK+8+gruufNOhINhaK0w+7e/RV4sBsexYTmOpBIJZ7MJUzB5ylQDQL/++mtYtmSxCkdCz6PL22Hi5Em6YmiVAFCLlyxCKuW6I3bZJR3yvNta1q85fMXypVMz6bT4AwGUlJbBsrRuaWnZEDYBcN9sfvMLjfPm1wwDv+Q6ncmmI/fcc5eTSvRDFODYPvgcG4lEEpM3n6a2mL4ljBj09SWU43c0UtkNyxKmASkqLU+fcfb54vMHv9bk8l/BW7xwnvXuP97PNDc3e42NkJtvnoPGxkZpamoy0yaNPbIv3r35lKlbmv0OPFi5nod4X4/88abrrXXr1s2zQrE7Z86cZDc3NxsAaGxs1E1NTaZ+ZO0p3e1t5/3wR7vhpNNORzqdRjQaQVtLGx5++M949LGHUVJYgt9cfBmqa2pwzgW/QTwel9raYRvaa8r0rfT4zae4Tzz/mgVgGYCfD6uquDTR3Wnl5+VLJBpT2WwWlqPR1dGOk08+2dx77306bKv+YNB/Vyy/8FFxvT2e/Otjx1VXVqhLr7sZANTe+8/S77zyik4bOW/egoXz3pnzQXFZYezXNbXDKmprhgsALJj3kWppWbUklpf/am9X5xGt61tk3ISJKhKLIRAIwECZxx/6s9rxR3up4rJytecBh8izj//N6mzv/Pm00aPffGf+/E4FQFvWYAeQt179u161anVa+QPXAnAfbGiwZjU3zwsGg68Ykx0HQDQ0/D7/Zw5So+B76+UX9cxdfqSff/wx3HHTH7HZ1Cn4/c13IByNYbMp0/DsU3+ztLJjnR1tgQrHd6mvNKI61qy2Uv1xTJg0RTmOP7ezsxlAQQ/dckv/FVVVmVnNzaardV1kyuTNcdJ5TeL4fVj48Ye4ePb5aG1Zg1PPPAfb77YvNLSKFeTD73gHmZ5V1w50/Y0XFpBPXw+h6seO1yPGjEMykUA62Y8Lzz1NXnvuSRUrLH50VVvvH+atbPk7sGHqQAZYRERERETEAOs7CbD6cwEWABWNxRAKhf3hUOgcn88+ZJutp2c++miuXrN6ZXd+fv4p7evaX4mFfc++/957R3z00Vy93XbbYsyYMZg+fToefviR+MCk1TqZSuadeeaZqiA/HwIFIwLb0kilkpi+xVZobGxEMBTE7F9fgOYHmuG5HlrWt6hkKrPGGG8ZgA+w0ZxLnwtT/q0TxkAwhMOOPAp5+XlI9CcACFKpJMRz8eTjT+DjDz9COBhEOuuis7cTqWQGAJDJZtDT3YuQ3w/HbxmTdrUo9SfPmHBeYfSAEXXDpbWtTfscB/n5+QAA13Oxft16lJaUYsLEiYj3xSEi2PVHu+AHO/wAjuMg4PfDcZxcp7RsaMuCm0nJiBGjEM3NWYR/vDtHpbOZ1VEVGRYMBcJbztgGtu2o3r4eee8f/4Dj6GBeW5s14/DD3U9eeO7xdCY71RucQ8yxlVK54GGjYaD/tsZGqNlNubZWn2v3YDCYWLJk0fOnn/zzmHGNaFtDK+0FfE5xvD8x6eLL/oDpW22FzrYOrFy5XGKRCFp6EtiQYCFXfWbb/v/8wLYdeAMVbfPmQT3Y0IBZTU3epLEjjzSZxLXlQ8rDZ53fKMFgQGkAc95+W5584jFVmJ+fbu1Ntr/00kvuQCXNhnbKJuPTD5h1sHXe7N94kbyYZWmN1155CeecdQYWLVyI7bffCfvsNwuFRYUQkdyKkMUlKpPJytIlC+X1116Tt996Q69ZvVYHfZZOZjxrSGH0uvaW9T+LhkI45cwzEMvLQzbroqujDcccebR57OmndVF+9EVfOHDqmjVtC9CXSmHKsc8Nzdxe+/brL+/W1tpmSkpLMGrMeBUMh9z3F65+NAt8JCJ6fHns1AmTp1QMqaqBMQbvvfMmujs72mx/7M2iWOAIx+8IAOXYDgwEsVhML/jkY8yfNxfblJVj+Mh6bL3DTvjr/fdslfW8PAAdrhFlBubDy6RT+OTDD9GbSM1xdeQZAGpWc7MZXhzaNRLwH+74/MDAEEzP/ewiEPmFhZ/MW/jJm0fN2re/ta0NyHoFCz7+YPL6tWtVXX0MEzabDEfbedOmTTuktLIK8+bNg4gLrTW22HIbHH7siRuGHH/0/vtiw5K6+nr5uPZOASDtnVnZdscfIppfAIjgluuuwl//8iiUAh7+80P4wa57A5bCj/baDw89cG9m0Yr27JcFVxsT79OFLaAt5BcUQwCkk/1TLEdHAZiB+dwYXhEREREREQOs74IFoD/eh77eHlVaViblZWUoKS4ak04kxxgY/Pa3F2PZ8uU44/Qz0NnRfiaAV6Ph/KvWtLYe+PJLL4a3225bAYAZM7aRRx75y4RAIDA9mUwuymQy57/99jsHAdgMgGilVGFhIRKJBApiRfBMbrTWvLmfYNHCJaIA2La1VikcidxqaIOrEH7Z3DL/Vohl+xxM32Iqyssr4BkDS2sYz0BbGsuWLkH96NG4/A9XI5FMYP3atdhs6ubwPA9bbDkdt995JxQ0onlRNP/pXjzc/FBXNBqRG667TqUzkMWLF+Htt17HGaefju13/AGeeeYZnPjz41FTUYb21i4cc+IJAIC3334bt9x0I4oK8hCLxHDEMcdi9OgxAATKQBlAxfLyNpw/d3d3qt7+/geCtjN05MiRm2+3/fZGRPSKZcvx3pz3spZtPTx8+FnpRy47LS/ksw8uLimBP5ibN7uruwvGM4KhQ83nh4H+O5qaIE1fcm4PAMlkcvWaZHJvfFo5pQG4VaWhHxYUFj5TU1MjQG7Os76+PmVZzudDNNXT3encc9dtyvOySmkLls5VrA3e0BiB67pw3dyqckYEllJwbD8CAT98gSCCoaDd3tGGkD/oNDaermc3NRmFZtls7MgjLJO5OhgKhRsv/L3Ujx2vent74RjBlKnT9eFH/kT+dPedk4vCdnPJ2JH3KbXobgCJefPmaQDwsia5+ZQppmxgEvMXnn0Sp/z8BIhSuOrqm3DAQbMGhjwCIgLX9fDMk4/jgfvvVR/MeRvr1qxRyYy71rKtu322f1kshN91d/b9rH7saLn095djxx/uopKpFMLhMB547HF55umndXlB+NV+kzm6bU3fcgB6lxHwPzXn5rSVZ3d5RpBKJQWAsm0ftONTASCSbWzUo6vKDhJgyNQttxHLtlRba4u89fqrcGyfT4wbtCwHgUAgF8y5rnjGU0nXvJzt76145/WXR2yzw07w+wPYY/8GefShBxPLVy3yAMAzLiwr97JpvIy0tKxTSzsTV0vHiu5dRyrfU4uRTqTSkUAgGCooKjUAVCKZQGtLi0BD4AENgNW8rO0GADcO9pFRVQXbdHZ2vvTx+++puvrRGDZiJPIKilBZO9w0XXE92js6xHgeHMdRBQUFG+bge/Kxv8ibr7ygY/n5+qW/34ntV+T6iSuAzx/asC+qa4fB0YAvEMRmk6dAaYVsOoMR9eMxbMRoNWfJq/800C0sjKhP5n6g5r33jkycNgOigPMuulKNGTdOrrvk10NCmfhdk0cNObapae0D3zRUJyIiIiIiYoD1b4pGbInH+6S1tRV1I0ehbngdqocNx3tvvy11w0diaHUNNp8y1RgRfdjBh+QCDMckFdDx2utvhpPJFILBgJ4yZYrnc+xh2Wx2FwBv2bb9USaTObFu+HD89Kc/xYxtZiC/oABe1oXfHxgYmiSYNetArF2zRn34wYdIpTNOOBw8OegL+Nq7ul4GEM8LhTaHLU19vUm7MBYKGK3+0tndf9W/c9LY3dGOn//sGGQ8F67rwVIWstksQpEg5n00F8OHjcCMGTMQCoc+83eVFZXYf/8DNgQ3b77+CjJuxtef8a5atmzF7sGgr/TUU0+R3/y2R61avQpaa4wdMxZHH3U03Ewar73+OiyloJRCZdVQHHHEEQgHA2jv6IQaCD+CgSAgnuu6JmM+Xf1RO44fxQUFB3jZpLPzj3ZDRUWlBiAvvfCC6uzosJQTvLO5eZZXW1V5rmPbtdO2mC6WZalUKiWfzPsYloZvmOPElgEl/2aXUABcAJUjKkvOMcYLaNsKKKWfWLSy5UoReOqzAzplIHGSxsZGff2Vl6iyvHJUV9cCgLS2tljd3b3Le3viNw9UgwkABINBd/nSJSsuOPuMJJT2bNtSuUBINuxVI4DnGRjjwoiBMQLPNbAtC36fD5bjwLa0CYWD0XA01tLU1GTuqR4yY0Zh3rnxeO/WhYVF0cZLrpQZW2+nUuk0/H4fbNuB4zg494JfqxF1I+QPV/5uu1Qitd1mY0bWv//JorMefPBBVymliotLAlde/jsdjUYzY8eNw9mnn2wF/CFcd9MtmDZjBgBg6dIl6OzowNRpW2DN6tWYfe5ZZsWyxfG8WOHT+cXltwWy6d6Va9tej0R0SSaZOGyf/feRi3//e1TXDFfpdBrI7W/jelltafW+BNS+PevS7YP53VOLkUZjozZ/uCjP7/cjFoupXAiVQSKRhGPDTHnsMasz2X/YqJF1+TN23MnkQuH31aL585GB8/t4Ty+qh1YhL5afC0Y7OxDv7fe64qn7Q5bZf94/3qxLJXq9vz/7JO7+440OlI75/X6NVBpKaeNm016yv9ckezuUm0mL1hjch+kxw4ePTHavOal0SBVqauuUAKpt/Rosmv+xikaDvpbO5IZeIgLMnt1oZjc1qYm+QLK/u7Pr3bdeK9rrwIOloLhYbT5tC8z96EOdSSVMcVHRxmGpeMbDXx96QC5vPMf23ExH3LMuwhpk/j5zpsZLL5mSwoD5+3NPmkOP/QUCkQh+cvxJGFU/Bv5QCNvvtDM8181NamZphCLRf3kAFIZ1uqOjo/f8k38W/WXjRWabH+2pkqk09j/8WDWsbpRceM7JsYULP7lum7GVBy9tjTetbe95b+DfAjy+oxAREREREQOsb0lhYaGdTqetJYsWY6utt0F+QSGmT9sSTzz+lDryqJkoKiwEAPXh+x+otOsKAKxe3bZYKXX5/HkfX93e2SGVFRUYPaYetcPrzIL58/uqq6srVq5ceccW06aX3HrrbTJ+wtgvVDxks1mk0xnsv+/+2HXXXfDWm2/h+htuKH3y8Sf36O/v2ikWCb+gLPXHeDzZWBCITjrooD3x9FNPwMtmZ5QWRtHa2TcYYuFfBVlVVbX4+UmnIe1lYDzAUgrZbBbRWBSP/uUveH/Ou+jvj8P1PDz11ONIJvoRCIbR19sDy7Kx9977Ib8gD/19vXAsG+l43KkoK1I1NdXIy4uhprIKrW0dAICamhqc/6vcYnj33nsXFi9cjLVr12LkiJEoyM+H3xdAfn4+Ojs7sWbVKrV61Qr4HLvKsX3W6jVr4Lqutm0bu+2xF+6/966a6qpaHHTYkRAR9PR04/FH/wrbtt+JRSNTKosLj1+/bs1pW227LXbZY08REaxbtwZz3n7HQNl/t7zUHtMn1l8ai8ZSrpfVbtaDiORykw0zpAsUVK7ySetcFZRlCZT2B32+wryCAmTdDBbOn7/t0JKCD5XqeqaxEappcNW1gT3Q0NCApqYmAyBVP6YI+UWFAICWNWuRTCU61/b0zB0MrwAgmUyuW99t7VJUFLTCCAEIA+Ev23v9+Mwv+vs3XOsBSMf71SfL1/ZHo9GyKWNHHyySPre3p2v8+EmT8aumi2X0+AnKdV30dvfg143ny7QtpuGwI49WqVQaBx52pKqpG+41nvdL1bJ23ambjRspSqmzAWQ95f3DH/A1XHfVpb5wKIJ4PI5fX3TZhvDq9VdewnHH/hQHNByEzadMRSabRiwv6sXF39C6ru2ZjZ9BIBCw4/F4Ypvtt1fVNcMl3teLJ554DOPGjMO4iZPguh5cEVm3Lr5V0IJdWVJiWf7Qu2E7U5S46crD2xPZXbffZU/Jy89XALBw3lx0d3ZavkCkN9PdsWs2ndzph7vuacoHVql84ekn0R+PZ2LV9QvivfP2LSgqQn5BAQBIe2uL7urteTml7PvyQ/aEJQsX7XT43j/SK5YvRWd37xp/KPy0uHYCSCs3a0Kzf3mKpbS2bEujs7MHZdFgal1P0h4/vGbPdKrrNEtjm733O1Bi+QVKRPDOG6+qBQsW9QQj0Q/RmcTYRgiaMDAXXJOZB1gfLVn7j7Kgum7hvLkX9MfjJhyJWDvuugvOPfkEnHL0Ibqiuhr5RYUIBMJI9Cfwzluv4x9vvw4Lqjutg8csWtP6iABqVmmpAEBBccy/YN6H+rfn/MI75dzforiiEj/ae38AwOL5H8JWFmrrxyDe14O+np5/9lIhAii1qnfpuKqy41etXXPbmSf8pPio439hjjrxdJ1NZzBpy23VVXc+JJece0rRGy88u3dNfn5tLFB26PzVLXMbAd0ErkZIRERERET036YEUDU1NeVlhZHHzzj1FyIinoiRDz/4QA45cJZ88P57IiLS1d3lbTZpoijg8cE/1lof5VhaXnr5lYyIZHv74+mttt3WBXBqIBCoioSjfY8/+pQREdPd0yMPNDfLySefJEcffaRcccXlkk6nxXVdSWcyMijruvLn5mZvi+lbiK2U5Eejkh+LyUUXX+yJiHflFb93Q35H8sL+VFlh/kkbP5cve34DX0fPmLZld293j7iuZ9o7OqSvr18yWSMiIrfecqvM3HqGdHZ2SFd3t+y600wpK4yY+pHD4sV54fTWW2wu69auFRHxzj7zFMkP+K+oLC24/Yc7zpQVK1Z4nufJjTfcKNdff4OIiPTG++Svf/urXHLJRTJ96uYytKJc7r7zdhERueqqK+TAA/aR1atXyYL58+Xknx8vk0aPkOFDK6R+WLWMGzVc5s6dKyIixhi5+647zNNPP2VczxvY1ptNdUWxjKgd+uKQ0sJ1xZGA7L7TTDN/3jxJp1MiIub6a/5gqsvy3SG1tfV+Rx916vFHy5oVy2TJgk9k0fx5snDe3Nzlk7my8JOPZdEn82TJ/E9k6cL5snTRIlm+ZIksX7ZUli1dLEuXLvPi8X73pReecyeNGmYqiiL7DGYyAHyfuzgAYvlB++b99tpDurq7jYiYi3/9axlaXPA+gPyB2/iRq1b5rxk3bOhPJ9RVPjtheIVsO3W8XHHxb7zO9g6TzWQkm8lIW1ubOe7oI6VuSLmMHlYpl/z2V14qmZB4vF88z5P58+eaA/be1Yyrq5RJY4b/fmAbnYn1w46ePHbUaRUleU377rpT7/pcPzCffPShbLv5RBlaEJZbrrs6t997O2Wfnbfzqoqj944sL7wkZuGK0mjg+rJ83x7V1RMKwrZaNrF+lJtK9mdvv+02b0hpkcz76AMREbnu6iskpJRMqquWmpKYFDmQKPBymQ/r6gotOfWnh0p3d49kXVe8bNYcvPdu3tCo0zu8smz3qnz/wzNG18qSBQs81zXS2rJWZk6ZIIUamdFDi58t8yN58k+PkEzuOPP+eN1VUhHTjwFAHlAwuiz/51Vh+7Sx1SVnja4dOnOjY0dVFYdnDY3Yvyzy4cwCG2dXFUXOrivL+82oioKHhhUGpCrfkct/fb6J9/ZIPN4vHe2t3l47bW2GhPQrgHzZAgxoaMjt+1ElgbO2HTtcPn5vjisiZvGCeWbmhOFSZuHeGHBCFDg9AJymgdPygFPHVuadNbw8uutAWr3xsFVUl0RnTB5WunB8RTQ7a8fp5g8Xnie3XfN7Oenow+SXPz9W1q9ZJSJi3p/zlkwbOyxZFLOn/pPXjQ3Xj6gq2nNyddG60UW2nHjI7rJ66QLjZj1JplIS7+k0l/zypOyEUr9sVp334aQxdeM+t21ERERERET03zJ4Mjm0LO/Mnbab7q1asco1xojrZiWRSEg8HhcRkUcfe9QLhYLZaChw4+Df+nzW3gCy551/gfzxtttkypRp4g8EJBAInAOgctTI+p55H80TETEvvfx3cRzf4FxMsvWWMySZSokxRu644w65+aabxTMmd/FcaW9vl+OOP95opdzNJ23mdXR0ikgucGpu/pMpys+ToGNlC2PhXyBXWaf/WYA1bfLU7rbWNlm9erXZfdcfyZiRI+WRR/4mxhi55upr5Ec77Sjd3d3Sn+iX/fbeTcrywn2VxcUnhxQe3n7GdFm3bq0REe+Xp58shbHwXUX5oYcOO3iWyWSynojIupb10tKyPrd9D/5JLEAmjx8jD/7pfvnHu+9Kd1eXiIjccMM1stm4kfLhB++LiMiK5cvlrTdeka2mTJAR1RUtJQXR3hOPO8aIiKTTaUmnU9Ld0y0iIkuXLJbttpomw4eWy7bTNpf999xD/njTjV5XR7v0x+PieZ68/8F7Zvqk8TJiaNn6LbaYOAzAUWedfLwREXegAf+ti+vmvr760gtmXF21VJUV7Vlamj9xTG3l2/W15a8PG1LwRnV5wRu1FcVvjKgqe33cyJo5FQVBOev0U0VEJJNJyxE/PlhK8sL99cOHvDWiasiro4dVvldbWXzsRv1P/YcXe3J97Zydt9lcLm46N7t44QIRYySZTIqIyLIlS7wf77+PjBha7k0aN/L8SfV1z42uLZdzzzjZ7e/rk3h/v2QzWVm3do389PCDvWljR8rm40ae+7m+VHTkwbPWdrS3i4iYd954XbaaME7GDCmRXWduJRecdZqc+JNDZVxNmYweWihbjBsuB+yxs0wfP1KiPnUdICoatE+pLAjLcUf9WCqKCqSmvExWLF0iIiI3XfcHUYBcd/nvzLKlC0zzfXd7f7z+Wrn1phvljVdf8hL9/ZJMpkRE5I4/3uzW5gdleGn0+pFDiv5c5odc87sLjefmQs7rr7pC9tp5R7n28ktkUt1QGVdbKS8+9+xgRuydffLxUmjjsa96TWj8kmNp1JD8E4YV+l8t0niyNt/pmVhbJAfsMtN74i8PmUQiIX39/eKarJx/xkluWVDJyMrCBxq+IqQcfM0ZP7Tw6HEVeemH773LExHT19PtHbXfjyRs69P/VfD+ZVcWR30jR5Xl3zC2LCy1MXgTqgvl7F+cIOvXrhMvFwCbyy5sks1HDTMTRtRs+S8CrA2/G1YcHTWppmT2qELILpOGyZvPP21ERHq6e8RzXbnn5mu8zYbkychC3wcjK0smfY37JSIiIiIi+pc4hPCLp5MAmlVJcWm0rWWdvvfuW92zzssNf9OWhm1bMMbIXbffptKpVOfwuhGX9i1aBAAqk/H+BuCkSy7+zbaeJ4OT3YRCodAcAHZPT7dJ51Y2RE1NLU4//XTM/fgjRKMxHHfsz+DYNpRS+Oijufj97y/He++9h3POPQdl5WUoKirC8OHDIVDWxwvm48BZB+Due+6R4uIStd9+s1ReXoE59JAf211dHRfn5YVe7+lJzME/mRNLBv7Lz8uHzxfCmnUtSCf6oZSCpRWCgSBsbUErjUmbbQ6tdCg/r/D83t6uYGXlUNiOT3mep1zPQyQUOFDEqM03n6ocx1Z9vX0oLiqGbVm51ehKS7HVVlvCVjaqq4dh8pQp6O3pxrnnnYXbbrgFnuviuKOPxvEn/QKHHn4kervbjYjonr74TYFgcLu/PPLQzMqhVXLcCb+Q/IJ8+Hx+LFm8GOec9UskEil1171/UqXlFVJSUoz8gkItYqCUxsIFC8zZp5+q1q5fs84fivzs7bc/XOa39S49vb3q4w/fU/3xuBLkhg+KyIb1HVVuUiMoAJaVy5M8Y+B5LjxPUFxSghXLlyGdSS/Sgcii1hUrqvY5YtdpPz3+BHR2dEJbFizLgmM5sGwL/f0JqRs5SmVdF1k3i58cdxz22W/fUCwvf4t0KonLLmzCkmVLhn1m93wzG/a363p9v730Sm/aVtspGZjs3edz8ORjf5XLL75Ir1mzOhsMRc7/4ONFl1aVlNxfVlp8z98efnBLv99vzm38rc5kMiivGIKrrrvJnHjMEerlV14bDkA1NDQ4DQ0N3qxZs6KtrS3IZrMAgImTN8cfbrgJ/5jzLjo72tHd2YG8vEL84tRfombYMDO8vl4qKsq9Hx+wt35r7qIUoMRo51nX4K477rg3pYD6+rramZ4YAaC0pSAAIgUFqB02CrXDRinkhqKpjcJZue/ue8zFF5xnaVu1ezrwt3hH5682m7KF7H/oEcgYF24iiXvvuB2jx4zDz08/CzN33s1oQNUMH66yroeerg7MeecdBH3aB9cAgJ45E7q0NNeOzc2QjYfATZkCZ/icBvNW8rEp0yZP2nqzaVujum4kJkze3KuvH2P5giEoZcF103LheWfLTddcbYUioefaE3JG86fb/5n929ycu/+5dRPuqnjrpYYlCz/eBYCJRCJqxMh6FPufPmfsyKqnaj9ZNb8VUKWDf98AjP3c9g32g8ZGqEubMomaspKd60aOxGZTt1B77HcAJmw2DUYEWim88dor8uLzLyOaV/hGW3/X2q8RMkkjoJva+xaivW/2ZsNLzZq1qxtPPvLH6rKb7pBtd91dxXt78eNjTtT5BUXeFbPPntje0f5rAHsLEywiIiIiIqL/Og0AI2trJ00ZN+KD6ZNGy7V/uCybSSddEXE98dxLf3dxpjAaltLCvIvHjh3r+zrnZpGiyGgAveedd763oXTqixU+sr611Ww5fbo4ju3ZSpmxY8d48+fNz34098NMLBYVpdRDBQX57wKQHbffXtauXSvd3V0iIua5554xefl5Sdu2p2/8XD4XcADA6KlTpnavX7deEqmMWb58pXz44Vzp6OoW13Xl2muvlv332kt6e3okm3Ulk8lIxs1IMpWWVColyWRSEsmEJBP9cvopJ0hJLCg/2mFbWb9urfE8z9x9z93ml2ecYVLJ5ODzlLa2Nvn73/8u77zzrqTSSZk/70O57uo/yPXXXCN33na73HTtddL8wP3S1t4q69eucWfts5vYtv5leXnJAdUVpe1DivNlj112kjNPP1nOPP1U2W7rLWX08GEyYfRIefKJx4yImFQqLSLipTMZ98/N97tbbbG51JQXmzF1Q28fbIChZXlHjx81LLn52PquiaOH9UwaM7xn0phhPZNG1/ZMHlPXs/m4kT1TJtT3TJs4umfqxPqeKRPqe6aMr+/ZbOyonolj63omjRvVs8Xm4zq3nDw+Pbq2/MKBu931lBNPyIhIeuCS2eiSHaj2+rJLVkRSO8/cJlNWEGwCPq3G+Q8CLGDmTHt4RckbB+61m9eybl1GRNz357zjnnbicd7omkoZO3xo79hRw88CPq0uqi4rGzZt4tg3JtXXyPVXX+EO7rdHHm7O7rztNDNl7PBbctvXYAFARUVB9bjhla03Xnf1P+vPn7l4bjp7wB4/9ABc/rn+CMBqGFNVIQs/+djLZjNy641XCQBz9qknuetb1rr98b4N7RaPx90nnnzCO+bIQ6WmMCLDCgMtlcXR3QHoGPDmjVdfvqG67i/N94mllPezo490c8OBRVzXNf3xfiMi5qorLvfKg3Z/bXH4wC9u05eyACAK3HzY3juLl81mNj5+PfHMu++86R64z25SZEMq8wLP1pSUlP+L+1aDw+wqA3jqJwfsJcl43PM8MY8/eI+MKvT1loWd8V9z+za+zYhD9turo60tVyXnGZFEIiEiIq+/+op72EEHu/vvubfMnDHjmI337dd8jVQA9MTq/AsmlDhm2pAieeO5XCVWe64qz7vqgtO9ITaeGkhkFTMsIiIiIiL6T7AC64sMALVo+fIPhg4t+3FZQf69d/7xpolvvv4q6uvHYMHC+XjttdetaDTc2Z9JPzhv3rzMwAmd/JPQSLyEF9faWvm7Sy4e52az7k+OOUbn50Xh8/kBAOl0Gu9/+KFc/NsL1T/mvIOAP3hxuCCQN2/eJyceNOsADVujr7fPCwd993Z1db+Qnx879IW///23xx17bPSee+9RqVQSL7zwoiT6E37Xdf/liailldi2JT6/IwVFBSgqLkAkHMl1CsuGP+BHKBSGQBDvi0NpBaW0eK6rlFYqGouJbQGWshRESTqbke7ebpSVV+C5Z57C4399FD3d7Tjy6J/IuPETVXFxMWbOnLnh8evHTED9mAlftXmitTaua9T69W1/Lo5GP4jGwse8/fabP3z11ZeTtmXraCyCvFiB6uvrm3TVFZf6h1YOMW2t7ebdOXOst954De/94x0oI61OMPTztp721wb2i+lL4ZGuztXv9GezJuwAgAP4chNWfWrwpwwyuS+f0Z/NiuPAisWKOkRE+W3bWrdmlfOXP/8JvX290JYNrQAxKrf3PYHk2g/iesh6GbjZLLRtIZvN2vH+Xvj8/gCQ/E/7bq4PvvSSBEfWLH79jVe3vOQ3F+jS0gr86b67EI/3wQkE7u33rAtXLF26GAAGqnf0ypaWZUmTPLSmtPyeW2+6Ycu6ESO9aDSiLrzgXOVzbKUdZ/lnHqgvm7TynBV/vPHakvWrV7kHHnqYLi0fAtvxwbZtKAg8z4ObzSARj6Onu0v1xvvE9TLasVQg68lgGKTQ0GDQ3OwzRiSvIF9s25HC4mJEbK3uue1267FH/4Lignzk5eXDsi20t3Vg5Ypl6Ovq+zC/KJJMeOqSde19jwMIBkJ25JrLL7cWLlzuHXP8sfquO243CtAfznkb11x1BUpLSrwh5WU6GIriyScf9266+irbdvRHvcb/ZG4a/H+uEZAmAIUlkaVPPf5s6v47brUPPvoYWbViifrwvQ/Mw39+UD/99FNWV2dfa2F+4OXOtDo12dO2frD//ZP9pgDAAxYtWrr0B719vbo0HEZt3XApLSs1Cz9Z/W9X5VVXV5u5c+d7zz/1rBx46EHiuWmsWbsWzzz+pHn57y9Ztm2hP5W4vW3lyocGVsM0/85rpACiVnb/emJ1TPX3987+xdFH4Ko77pEZO/xA3XHDH+SO2+6wCotD/rXrE3xXISIiIiKi/xj/RfyrT1R1E2BqhgyZXFIUO7Krq8uL9/ZA+7SJ5RcFEonEM6vWdvxNBhcT++fDvhQAcRxnivHc+zwjowoLCzCkcgjCgSBs20ZPXy+WL10KN5uFz++7vDeePAdAsDAWOqWzN1EIwI76/e/1pdN3DJ4I5+VFHuzpiTccf+xPkcm6uPX2O2Db+kXH8R+WTCbX4ovbNTh8aczWM7Z6+5777o34bB8uu/RSvPzi8zjmuBMwZdoU3H3Hbejt6cNFl16GdDqNiy76LT547wPE8qLo6+tDTXUVTj/jLBQXFeDSiy/EM08/AwEwbPgw7L3vvrjv3nuR6I3DMy4cXwBjxo7FhM0mYnjtCBQUFsIX8EErC0YEruci67nIplPIJJJQUFi1ahluvOkmtHd0ndfdm7h48AR/lxG7+KK+qADAe5lmtXgxZHjtkF+GAv4mv+PT/X396O7t/QjGe6KgqDDqZs0Ti1auefzb7isVFUWjLdf7WXd3t0ADeqCVjcntKePldpgM7DizUQpgA1JZURgQrR9bsab9ycZG6A2rGX7zY1qCQQyprhh6YndnZzCZTEpJSbFyAqH46vUdV8Xj8XZ8cbVKDcCMGFFVF7V99xaVFE9XWiPZ34tUyr3y3bkLzgWQxuCqdIAMHVo2viQavi+bSU9w/EGUVFQgFonBcXwQcXOraiZT6O3uRFtbG3p6emGUdHue/KStJ/nwwGNqAK6j9aEjq8vuPvyoo+Fz/Fi9ejnuvfPu1ZblNKfTSeW5Ihkvd2O/AxWO+PtD0eh181e2twFwB56PUxS0j0kl3VlZYLvCWAiemwUgj2aS7kdxwe4hC5MigVxulsx4CAWtD4ytD1nfmfkE//o43lisJKBfnzZt83Ej6sfj1Zeex4plq5B0EfcH0BzOz79z9frulzZu26+z3wBUj6vIf/uC3/62rKOzC48/8hBWLVuSXNWe2qIrm52LfzIs+PP3NWHkyOFK6XdHjhpVcOwJJ+CD9z/Au2//A4lkHD6fb04yk3zoyaefvh5Az9e83698/xhXVdCY7ur+Vd3IUXq/Qw7EjVddj/6+btfJC5/78cqeywb7DN9ZiIiIiIiIAda31z7yH/z+C7cNO84E5fePjsfj2c+3fyTog+33u93dfc/hn5fjaAASCNjbaWVdn06miwxgB3zOB7DsI5LJ5Oqv2DYFQHw+38jy4sInKoZWxzKppPR1dyKVSAC2hu34YTJphKJhxAqKoAyktXWtvXp1y81ZwYuRoO+Q/LzYPnmx/JTls5BM9AfSSfc527HuXLd+vT+Z8WRoaSF8wRAcx3EzqUSNEZzpeW4AENFKA5YFhdzprBEPxngwroF4BsZ4AojjBAIfp73sT9vb+xYhV6njfUVbODXlxT/0lPGHwxHbgu/jeYsXz/t8W31JkPffIP+X+vVAFY5UVVXV+ZWZLMa4Ib/ffLRk+XMAEl92HxVFRaOLi2MTenv7s62trSr5JXvJByAaDIoT9Nmu8Vrau+OvfG7/mLygbz+fllt6+rNpARC2EPAFfU+3xjMH/7tPLhQKVQS0zOiJJ72gBR338CKArlEVRaNT6fSEVMbLAh4CPp/Tn8182NGXWfANAhyrssi/XWdHuigJuBaA8sKgpW2nd1Vr77Pf9PUhBJQPKfA/ZSupSPRn7IyHV+2QdUO/5b3R1YWer90xc/vSv9n48WdGw9GTPA9ZbWlHFG4KBALvLFu5bN7SpUsXfYPt/Kr3EDWyLDjbdt0Tenuz6bwCny8L+8pFrYmL/sP7JyIiIiIi+q+eyP+fbqOGhoaBYYHNyE3yDjQ3Nw8W1Pw79Df4G+tzgYn53Ml6uc/n84uIAnp6enrQ9TVOGB0A5dhouGNtbS3Wr1+OVAoIBAJIpVIbbpwfCKArmVytlPL22GOP0CuvvFLa09NjNnpOHQD6vurBRoyoHOo4jpVMppBKpZDqTgEBAIMPEQACqQAQAELBoDg+n17X1dXb29vb+U/6q3zV8xycy+cb7qNv3keam7/yBs3/5I8bGhq+jW0d6LeDj7yh35p/0Te+qo9+WVvrb7jNX3ZfoQBQkl9ejkAggK6uLuX29CR3A9o/334Nn7bplz2Xrwo7beQqtf5bx+W/Os7xDe/TDgZRlkzCri8rU5Gh4a45c5b2/AfbYpeXl1eGQiHJy8tT77333prBdmhoaLCam5u9/9L7iABwyvMDQwKBfHR3d6M7lVrzT9qciIiIiIiIvuf017h83WBRfc3r/lusf/E73djYqAHoxoHLf7g9X3fC6g2PiS/OQUbfoN0bG6Fzl8Z/tQ9VY+PX6tMbX7717f+K40n9B8fav3Msq2/hNeO/ZiDgVd9Cm+M7fj0iIiIiIqJN6USVTfB/bh/Kt7Tv5T94zG/azzjsiK9Hsom3g3wP7+/78lhERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERP8HKDYBERERfd8+n4gIlFJy1m51TwwtDu2qlTaOZetjb5vDzy5EREREmyDNJiAiIqLvn4GcSilAKQgUjGJ2RURERLSpstkERERE9P0jyIVYFrSyoJSGUhabhYiIiGgTxQosIiIi+t5SOld9BVH80EJERES0CWMFFhEREX1vGRGBCCCAZziEkIiIiGhTxQCLiIiIvn9mKzWisDDqeSYMpZAbUmjYLkRERESbKFbjExER0fdK48yZlmqCmVofOKw46tvOcz1jIJr1V0RERESbLgZYRERE9L3R2Ajd9NJLbl1BoGpofmhW2O9TllLQXIGQiIiIaJPGIYRERET0vdAAWE1N8AoCqN59auVdlUXh7QyUWLbWjqWhNP/djYiIiGhTxU+CRERE9L+mHmxosJoBr64gUPXj7UbeVV0amwlYxrYspbUFaAu2bbGliIiIiDZRrMAiIiKi/zWZ1dzsVRQEqnefVn1XTVlkpjIwlmVr27ZgawXLBizFAIuIiIhoU8UAi4iIiP4nGhoarO4FLwfmre4t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"

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

            def mincho(size):
                # Prefer bold Japanese serif. Fallback is the known-working IPAex Gothic,
                # never Pillow's default font (which caused □□□□ tofu).
                paths = [
                    "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc",
                    "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
                    "/usr/share/fonts/opentype/noto/NotoSerifCJKJP-Bold.otf",
                    "/usr/share/fonts/opentype/noto/NotoSerifCJKJP-Regular.otf",
                    "/opt/render/project/src/.venv/lib/python3.13/site-packages/japanize_matplotlib/fonts/ipaexg.ttf",
                ]
                for p in paths:
                    try:
                        return ImageFont.truetype(p, size)
                    except Exception:
                        pass
                raise RuntimeError("Japanese font not found")

            # ONLY variable design element: current buzzword.
            keyword = (row["keyword"] or "").strip()
            size = 122
            while size > 54:
                f = mincho(size)
                bb = d.textbbox((0,0), keyword, font=f, stroke_width=2)
                if bb[2]-bb[0] <= 720:
                    break
                size -= 2

            x, y = 48, 158
            d.text((x+4,y+4), keyword, font=f, fill=(0,0,0,220),
                   stroke_width=3, stroke_fill=(0,0,0,230))
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
