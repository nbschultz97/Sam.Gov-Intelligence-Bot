from __future__ import annotations

import argparse
import csv
import json
import logging
import logging.handlers
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

from ceradon_sam_bot.config import ConfigError, load_config
from ceradon_sam_bot.normalize import normalize_opportunity
from ceradon_sam_bot.notify_email import send_email
from ceradon_sam_bot.render import render_digest
from ceradon_sam_bot.sam_client import SamClient, SamClientConfig
from ceradon_sam_bot.scoring import score_opportunity
from ceradon_sam_bot.store import (
    fetch_by_notice_id,
    fetch_latest_for_digest,
    fetch_since_days,
    init_db,
    upsert_opportunity,
)

LOGGER = logging.getLogger(__name__)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        standard_keys = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
        }
        payload = {
            "timestamp": datetime.now(tz=None).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "run_id"):
            payload["run_id"] = record.run_id
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        extras = {k: v for k, v in record.__dict__.items() if k not in standard_keys}
        payload.update(extras)
        return json.dumps(payload)


class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def _setup_logging(log_dir: Path, run_id: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = JsonFormatter()
    handler_stdout = logging.StreamHandler(sys.stdout)
    handler_stdout.setFormatter(formatter)
    handler_stdout.addFilter(RunIdFilter(run_id))

    handler_file = logging.handlers.RotatingFileHandler(
        log_dir / "ceradon_sam_bot.log", maxBytes=1_000_000, backupCount=3
    )
    handler_file.setFormatter(formatter)
    handler_file.addFilter(RunIdFilter(run_id))

    logging.basicConfig(level=logging.INFO, handlers=[handler_stdout, handler_file])


def _require_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _load_client() -> SamClient:
    api_key = _require_env("SAM_API_KEY")
    api_key_in_query = os.getenv("SAM_API_KEY_IN_QUERY", "false").lower() == "true"
    return SamClient(
        SamClientConfig(api_key=api_key, api_key_in_query=api_key_in_query)
    )


# SAM.gov v2 API uses 'title' for keyword search (no general fulltext param).
# We search by title keywords AND by NAICS codes separately.
TITLE_SEARCH_TERMS = [
    # Core Ceradon — WiFi sensing / through-wall
    "wifi sensing",
    "through-wall",
    "through wall",
    "STTW",
    "rf sensing",
    "rf detection",
    "presence detection",
    # ISR / surveillance
    "ISR",
    "surveillance",
    "SIGINT",
    "geospatial",
    # Drone / counter-UAS
    "counter-UAS",
    "drone detection",
    "UAS",
    "unmanned",
    "counter uas",
    # Electronic warfare / spectrum
    "electronic warfare",
    "spectrum sensing",
    # SOF / tactical
    "SOCOM",
    "special operations",
    "SOFWERX",
    # R&D / prototyping
    "SBIR",
    "STTR",
    # Sensor tech
    "sensor",
    "radar",
]

# Also search by NAICS codes relevant to Ceradon
NAICS_SEARCH_CODES = [
    "541715",  # R&D in Physical/Engineering/Life Sciences
    "541330",  # Engineering Services
    "541512",  # Computer Systems Design
    "334511",  # Search/Detection/Navigation Instruments
    "334290",  # Other Communications Equipment
]


def _build_query_params(
    days: int,
    title: str | None = None,
    ncode: str | None = None,
    ptype: str | None = None,
) -> Dict[str, Any]:
    posted_from = (datetime.utcnow() - timedelta(days=days)).strftime("%m/%d/%Y")  # noqa: DTZ003
    posted_to = datetime.utcnow().strftime("%m/%d/%Y")  # noqa: DTZ003
    params: Dict[str, Any] = {
        "postedFrom": posted_from,
        "postedTo": posted_to,
    }
    if title:
        params["title"] = title
    if ncode:
        params["ncode"] = ncode
    if ptype:
        params["ptype"] = ptype
    return params


def _process_opportunities(
    raw_items: Iterable[Dict[str, Any]],
    config,
    db_path: Path,
) -> Dict[str, int]:
    counts = {"processed": 0, "saved": 0, "skipped": 0}
    for raw in raw_items:
        counts["processed"] += 1
        try:
            normalized = normalize_opportunity(raw)
            if normalized.get("notice_type") in config.filters.exclude_notice_types:
                counts["skipped"] += 1
                continue
            # NAICS is now a soft boost in scoring, not a hard filter.
            # Many SBIR/R&D opportunities lack NAICS or use unexpected codes.
            score, reasons = score_opportunity(normalized, config)
            saved = upsert_opportunity(db_path, normalized, raw, score, reasons)
            if saved:
                counts["saved"] += 1
            else:
                counts["skipped"] += 1
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to process opportunity", extra={"error": str(exc)})
            counts["skipped"] += 1
    return counts


def run_once(config_path: Path, data_dir: Path, no_email: bool = False) -> None:
    config = load_config(config_path)
    db_path = data_dir / "ceradon_sam_bot.sqlite"
    init_db(db_path)

    client = _load_client()
    seen_ids: set[str] = set()
    total_counts: Dict[str, int] = {"processed": 0, "saved": 0, "skipped": 0}

    def _run_search(label: str, params: Dict[str, Any]) -> None:
        LOGGER.info("Searching SAM.gov", extra={"search": label, "params": {k: v for k, v in params.items() if k != "api_key"}})

        def _deduped():
            for item in client.search_opportunities(params):
                nid = item.get("noticeId", "")
                if nid and nid in seen_ids:
                    continue
                if nid:
                    seen_ids.add(nid)
                yield item

        counts = _process_opportunities(_deduped(), config, db_path)
        for k in total_counts:
            total_counts[k] += counts[k]

    # Search by title keywords
    for term in TITLE_SEARCH_TERMS:
        params = _build_query_params(config.filters.posted_from_days, title=term)
        _run_search(f"title={term}", params)

    # Search by NAICS codes
    for ncode in NAICS_SEARCH_CODES:
        params = _build_query_params(config.filters.posted_from_days, ncode=ncode)
        _run_search(f"naics={ncode}", params)

    digest_rows = fetch_latest_for_digest(
        db_path,
        config.scoring.include_in_digest_score,
        config.digest.max_items,
    )

    body = render_digest(digest_rows)

    if no_email:
        LOGGER.info("Email skipped (--no-email)", extra={"digest_items": len(digest_rows)})
        print(body)
    else:
        smtp_pass = os.getenv("SMTP_PASS", "")
        if not smtp_pass:
            LOGGER.warning("SMTP_PASS not set, skipping email. Use --no-email to suppress.")
            print(body)
        else:
            smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
            smtp_port = int(os.getenv("SMTP_PORT", "587"))
            smtp_user = os.getenv("SMTP_USER", "noah@ceradonsystems.com")
            email_to = os.getenv("EMAIL_TO", "noah@ceradonsystems.com")
            email_from = os.getenv("EMAIL_FROM", "noah@ceradonsystems.com")
            subject = f"Ceradon SAM Digest ({len(digest_rows)} items)"
            send_email(smtp_host, smtp_port, smtp_user, smtp_pass, email_to, email_from, subject, body)

    LOGGER.info("Run completed", extra={"counts": total_counts, "digest_items": len(digest_rows)})


def run_daemon(config_path: Path, data_dir: Path, interval_minutes: int, no_email: bool = False) -> None:
    while True:
        run_once(config_path, data_dir, no_email=no_email)
        time.sleep(interval_minutes * 60)


def backfill(config_path: Path, data_dir: Path, days: int) -> None:
    config = load_config(config_path)
    db_path = data_dir / "ceradon_sam_bot.sqlite"
    init_db(db_path)

    client = _load_client()
    seen_ids: set[str] = set()
    total_counts: Dict[str, int] = {"processed": 0, "saved": 0, "skipped": 0}

    def _run_search(label: str, params: Dict[str, Any]) -> None:
        LOGGER.info("Backfill searching", extra={"search": label})

        def _deduped():
            for item in client.search_opportunities(params):
                nid = item.get("noticeId", "")
                if nid and nid in seen_ids:
                    continue
                if nid:
                    seen_ids.add(nid)
                yield item

        counts = _process_opportunities(_deduped(), config, db_path)
        for k in total_counts:
            total_counts[k] += counts[k]

    for term in TITLE_SEARCH_TERMS:
        params = _build_query_params(days, title=term)
        _run_search(f"title={term}", params)

    for ncode in NAICS_SEARCH_CODES:
        params = _build_query_params(days, ncode=ncode)
        _run_search(f"naics={ncode}", params)

    LOGGER.info("Backfill completed", extra={"counts": total_counts})


def export_data(data_dir: Path, since_days: int, fmt: str) -> None:
    db_path = data_dir / "ceradon_sam_bot.sqlite"
    rows = fetch_since_days(db_path, since_days)
    if fmt != "csv":
        raise ValueError("Only csv export is supported")
    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "notice_id",
            "title",
            "agency",
            "notice_type",
            "naics",
            "set_aside",
            "posted_date",
            "response_deadline",
            "score",
            "link",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row["notice_id"],
                row["title"],
                row["agency"],
                row["notice_type"],
                row["naics"],
                row["set_aside"],
                row["posted_date"],
                row["response_deadline"],
                row["score"],
                row["link"],
            ]
        )


def explain_notice(data_dir: Path, notice_id: str) -> None:
    db_path = data_dir / "ceradon_sam_bot.sqlite"
    stored = fetch_by_notice_id(db_path, notice_id)
    if not stored:
        print(f"Notice {notice_id} not found")
        return
    print(f"Title: {stored.title}")
    print(f"Score: {stored.score}")
    print("Reasons:")
    for reason in stored.reasons:
        print(f"- {reason}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ceradon SAM Opportunity Bot")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run the bot")
    run_parser.add_argument("--config", required=True, help="Path to config YAML")
    run_parser.add_argument("--once", action="store_true", help="Run once and exit")
    run_parser.add_argument("--daemon", action="store_true", help="Run as daemon loop")
    run_parser.add_argument(
        "--interval-minutes", type=int, default=1440, help="Loop interval in minutes"
    )
    run_parser.add_argument(
        "--no-email", action="store_true", help="Skip email, print digest to stdout"
    )

    backfill_parser = subparsers.add_parser("backfill", help="Backfill past days")
    backfill_parser.add_argument("--config", required=True, help="Path to config YAML")
    backfill_parser.add_argument("--days", type=int, default=60, help="Days to backfill")

    export_parser = subparsers.add_parser("export", help="Export data")
    export_parser.add_argument("--format", default="csv", choices=["csv"])
    export_parser.add_argument("--since-days", type=int, default=30)

    explain_parser = subparsers.add_parser("explain", help="Explain a notice score")
    explain_parser.add_argument("--notice-id", required=True)

    return parser


def main() -> None:
    run_id = str(uuid.uuid4())
    data_dir = Path(os.getenv("BOT_DATA_DIR", "/var/lib/ceradon-sam-bot"))
    log_dir = data_dir / "logs"
    _setup_logging(log_dir, run_id)

    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        try:
            config_path = Path(args.config)
            no_email = getattr(args, "no_email", False)
            if args.daemon:
                run_daemon(config_path, data_dir, args.interval_minutes, no_email=no_email)
            else:
                run_once(config_path, data_dir, no_email=no_email)
        except ConfigError as exc:
            LOGGER.error("Configuration error", extra={"error": str(exc), "run_id": run_id})
            sys.exit(1)
    elif args.command == "backfill":
        config_path = Path(args.config)
        backfill(config_path, data_dir, args.days)
    elif args.command == "export":
        export_data(data_dir, args.since_days, args.format)
    elif args.command == "explain":
        explain_notice(data_dir, args.notice_id)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
