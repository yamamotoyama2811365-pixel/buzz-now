
import os
import base64
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
APP_VERSION = os.getenv("APP_VERSION", "31.0.0")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

REAL_DATA_MODE = os.getenv("REAL_DATA_MODE","true").lower() == "true"
REAL_DATA_INTERVAL_MINUTES = int(os.getenv("REAL_DATA_INTERVAL_MINUTES","30"))

# V26: Make.com webhook bridge for BUZZ NOW social automation
MAKE_WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL", "").strip()
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


def _build_social_post_text(row) -> str:
    keyword = str(row["keyword"]).strip()
    pre = int(round(float(row["pre_buzz_score"] or 0)))
    traffic = int(round(float(row["traffic_potential"] or 0)))
    status = str(row["status"] or "急上昇")
    status_plain = re.sub(r"^[^ぁ-んァ-ヶ一-龠A-Za-z0-9]+\s*", "", status).strip() or "急上昇"
    detail_url = _social_short_url(row["id"])
    return (
        f"🚨 BUZZNOW SNS捜査官｜{status_plain}を検知\n"
        f"「{keyword}」\n"
        f"検索・閲覧シグナルが上昇中。\n"
        f"Pre-Buzz：{pre} / Traffic：{traffic}\n"
        f"詳細 → {detail_url}"
    )


def _social_candidate_rows(c, limit: int = 20):
    limit = max(1, min(int(limit), 100))
    return c.execute("""
        SELECT
            t.id,t.keyword,t.slug,t.category,t.pre_buzz_score,t.buzz_score,t.acceleration,
            t.status,t.why_now,t.updated_at,
            COALESCE(tt.traffic_potential,0) AS traffic_potential,
            COALESCE(cf.confidence_score,0) AS confidence_score
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
            "source": "buzz-now-v30.5-auto",
            "sent_at": ts,
        }

        try:
            make_result = _send_to_make(payload)
            ok = bool(make_result.get("ok"))
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
                logger.info("V30 social auto-post sent: %s", row["keyword"])
            else:
                result["errors"].append({"keyword": row["keyword"], "error": "Make returned not-ok"})
        except Exception as exc:
            logger.exception("V30 social auto-post failed for %s", row["keyword"])
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
    init_db()

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
        collect_real_sources()
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


def _social_image_jpeg_payload(trend_id: int) -> bytes:
    """Return a compact JPEG cached in PostgreSQL.

    V31 converts the AI image only once. Buffer's HEAD/GET requests then only
    decode a small cached base64 value, avoiding Pillow work during media fetch.
    """
    with db() as c:
        cached = c.execute(
            "SELECT jpeg_b64 FROM social_image_derivatives WHERE trend_id=?",
            (trend_id,),
        ).fetchone()
        if cached and cached["jpeg_b64"]:
            try:
                return base64.b64decode(cached["jpeg_b64"])
            except Exception:
                c.execute("DELETE FROM social_image_derivatives WHERE trend_id=?", (trend_id,))

        row = c.execute(
            "SELECT image_b64 FROM social_images WHERE trend_id=?",
            (trend_id,),
        ).fetchone()
        if not row:
            raise HTTPException(404, "Social image not found")

        try:
            original = base64.b64decode(row["image_b64"])
            image = Image.open(BytesIO(original))
            image.load()
            if image.mode != "RGB":
                if image.mode in ("RGBA", "LA"):
                    bg = Image.new("RGB", image.size, (255, 255, 255))
                    alpha = image.getchannel("A") if "A" in image.getbands() else None
                    bg.paste(image.convert("RGB"), mask=alpha)
                    image = bg
                else:
                    image = image.convert("RGB")

            max_width = 1200
            if image.width > max_width:
                new_h = max(1, round(image.height * max_width / image.width))
                image = image.resize((max_width, new_h), Image.Resampling.LANCZOS)

            out = BytesIO()
            image.save(out, format="JPEG", quality=78, optimize=True, progressive=False)
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
            logger.exception("JPEG derivative failed trend_id=%s", trend_id)
            raise HTTPException(500, f"JPEG derivative failed: {str(exc)[:200]}")


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


@app.get("/api/social/test-image-post/{trend_id}")
def test_image_post_to_make(trend_id: int):
    """Send one existing generated image + current post text to Make for an intentional X test."""
    if not MAKE_WEBHOOK_URL:
        raise HTTPException(500, "MAKE_WEBHOOK_URL is not configured")

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
        raise HTTPException(
            400,
            "No generated image exists for this trend. Generate it first.",
        )

    payload = {
        "keyword": row["keyword"],
        "pre_buzz_score": round(float(row["pre_buzz_score"] or 0), 1),
        "traffic_potential": round(float(row["traffic_potential"] or 0), 1),
        "status": row["status"],
        "why_now": row["why_now"] or "",
        "detail_url": _social_short_url(row["id"]),
        "image_url": _social_image_url(row["id"]),
        "image_ready": True,
        "post_text": _build_social_post_text(row),
        "source": "buzz-now-v31-image-test",
        "sent_at": now_iso(),
    }

    # V31: build/cache the JPEG BEFORE Buffer receives the URL.
    _prewarm_social_jpeg(row["id"])
    make_result = _send_to_make(payload)
    return {
        "ok": bool(make_result.get("ok")),
        "message": "Image test payload sent to Make.com",
        "payload": payload,
        "make": make_result,
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
