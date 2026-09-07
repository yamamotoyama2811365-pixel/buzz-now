
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
APP_VERSION = os.getenv("APP_VERSION", "35.9.0")
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


_BUZZ_NOW_APPROVED_OVERLAY_B64 = "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6USVTfB/bh/Kt7Tv5T94zG/azzjsiK9Hsom3g3wP7+/78lhERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERERP8HKDYBERERfd8+n4gIlFJy1m51TwwtDu2qlTaOZetjb5vDzy5EREREmyDNJiAiIqLvn4GcSilAKQgUjGJ2RURERLSpstkERERE9P0jyIVYFrSyoJSGUhabhYiIiGgTxQosIiIi+t5SOld9BVH80EJERES0CWMFFhEREX1vGRGBCCCAZziEkIiIiGhTxQCLiIiIvn9mKzWisDDqeSYMpZAbUmjYLkRERESbKFbjExER0fdK48yZlmqCmVofOKw46tvOcz1jIJr1V0RERESbLgZYRERE9L3R2Ajd9NJLbl1BoGpofmhW2O9TllLQXIGQiIiIaJPGIYRERET0vdAAWE1N8AoCqN59auVdlUXh7QyUWLbWjqWhNP/djYiIiGhTxU+CRERE9L+mHmxosJoBr64gUPXj7UbeVV0amwlYxrYspbUFaAu2bbGliIiIiDZRrMAiIiKi/zWZ1dzsVRQEqnefVn1XTVlkpjIwlmVr27ZgawXLBizFAIuIiIhoU8UAi4iIiP4nGhoarO4FLwfmre4t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"

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


@app.get("/api/social/test-image-post/{trend_id}")
def social_test_image_post(trend_id: int):
    """V32.2 direct Buffer/X test using an already-generated cached image."""
    with db() as c:
        row = c.execute("""
            SELECT
                t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.status,t.why_now,
                COALESCE(tt.traffic_potential,0) AS traffic_potential,
                COALESCE(cf.confidence_score,0) AS confidence_score,
                si.trend_id AS image_exists
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
