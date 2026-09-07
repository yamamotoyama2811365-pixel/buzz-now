
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
APP_VERSION = os.getenv("APP_VERSION", "35.8.0")
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


_BUZZ_NOW_APPROVED_OVERLAY_B64 = "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iIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI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"

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
                paths = [
                    "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc",
                    "/usr/share/fonts/opentype/noto/NotoSerifCJKJP-Bold.otf",
                    "/opt/render/project/src/.venv/lib/python3.13/site-packages/japanize_matplotlib/fonts/ipaexm.ttf",
                ]
                for p in paths:
                    try:
                        return ImageFont.truetype(p, size)
                    except Exception:
                        pass
                return ImageFont.load_default()

            # ONLY variable design element: current buzzword.
            keyword = (row["keyword"] or "").strip()
            size = 122
            while size > 54:
                f = mincho(size)
                bb = d.textbbox((0,0), keyword, font=f, stroke_width=2)
                if bb[2]-bb[0] <= 720:
                    break
                size -= 2

            x, y = 48, 160
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
