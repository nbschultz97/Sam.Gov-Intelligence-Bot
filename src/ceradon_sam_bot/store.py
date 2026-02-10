from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SCHEMA_VERSION = 2


@dataclass
class StoredOpportunity:
    dedupe_key: str
    notice_id: str
    solicitation_number: str
    posted_date: str
    agency: str
    title: str
    notice_type: str
    naics: str
    set_aside: str
    response_deadline: str
    link: str
    score: int
    reasons: List[str]
    normalized: Dict[str, Any]
    raw: Dict[str, Any]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        _migrate(conn)


def _migrate(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
    )
    current = conn.execute("SELECT version FROM schema_version").fetchone()
    if current is None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL UNIQUE,
                notice_id TEXT,
                solicitation_number TEXT,
                posted_date TEXT,
                agency TEXT,
                title TEXT,
                notice_type TEXT,
                naics TEXT,
                set_aside TEXT,
                response_deadline TEXT,
                link TEXT,
                score INTEGER,
                reasons TEXT,
                normalized_json TEXT,
                raw_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    else:
        version = current["version"]
        if version == 1:
            conn.execute("ALTER TABLE opportunities ADD COLUMN link TEXT")
            conn.execute("UPDATE schema_version SET version = 2")
            version = 2
        if version != SCHEMA_VERSION:
            raise RuntimeError(
                f"Unsupported schema version {version} (expected {SCHEMA_VERSION})"
            )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_opportunities_notice_id ON opportunities(notice_id)"
    )


def compute_dedupe_key(normalized: Dict[str, Any]) -> str:
    notice_id = str(normalized.get("notice_id", "")).strip()
    if notice_id:
        return f"notice:{notice_id}"
    solicitation = str(normalized.get("solicitation_number", "")).strip()
    posted = str(normalized.get("posted_date", "")).strip()
    agency = str(normalized.get("agency", "")).strip()
    return f"fallback:{solicitation}|{posted}|{agency}"


def upsert_opportunity(
    db_path: Path,
    normalized: Dict[str, Any],
    raw: Dict[str, Any],
    score: int,
    reasons: List[str],
) -> bool:
    dedupe_key = compute_dedupe_key(normalized)
    created_at = dt.datetime.now(dt.timezone.utc).isoformat()
    with _connect(db_path) as conn:
        try:
            conn.execute(
                """
                INSERT INTO opportunities (
                    dedupe_key,
                    notice_id,
                    solicitation_number,
                    posted_date,
                    agency,
                    title,
                    notice_type,
                    naics,
                    set_aside,
                    response_deadline,
                    link,
                    score,
                    reasons,
                    normalized_json,
                    raw_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dedupe_key,
                    normalized.get("notice_id"),
                    normalized.get("solicitation_number"),
                    normalized.get("posted_date"),
                    normalized.get("agency"),
                    normalized.get("title"),
                    normalized.get("notice_type"),
                    normalized.get("naics"),
                    normalized.get("set_aside"),
                    normalized.get("response_deadline"),
                    normalized.get("link"),
                    score,
                    json.dumps(reasons),
                    json.dumps(normalized),
                    json.dumps(raw),
                    created_at,
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def fetch_since_days(db_path: Path, days: int) -> Iterable[sqlite3.Row]:
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).isoformat()
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM opportunities WHERE created_at >= ? ORDER BY score DESC",
            (cutoff,),
        ).fetchall()
    return rows


def fetch_by_notice_id(db_path: Path, notice_id: str) -> Optional[StoredOpportunity]:
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM opportunities WHERE notice_id = ?", (notice_id,)
        ).fetchone()
    if not row:
        return None
    return _row_to_stored(row)


def fetch_latest_for_digest(db_path: Path, min_score: int, limit: int) -> List[sqlite3.Row]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT * FROM opportunities
            WHERE score >= ?
            ORDER BY posted_date DESC
            LIMIT ?
            """,
            (min_score, limit),
        ).fetchall()
    return rows


def _row_to_stored(row: sqlite3.Row) -> StoredOpportunity:
    return StoredOpportunity(
        dedupe_key=row["dedupe_key"],
        notice_id=row["notice_id"],
        solicitation_number=row["solicitation_number"],
        posted_date=row["posted_date"],
        agency=row["agency"],
        title=row["title"],
        notice_type=row["notice_type"],
        naics=row["naics"],
        set_aside=row["set_aside"],
        response_deadline=row["response_deadline"],
        link=row["link"],
        score=row["score"],
        reasons=json.loads(row["reasons"] or "[]"),
        normalized=json.loads(row["normalized_json"] or "{}"),
        raw=json.loads(row["raw_json"] or "{}"),
    )
