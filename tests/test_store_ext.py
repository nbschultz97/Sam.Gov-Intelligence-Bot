"""Extended tests for ceradon_sam_bot.store — queries, migration, edge cases."""
from pathlib import Path

import pytest

from ceradon_sam_bot.store import (
    init_db,
    upsert_opportunity,
    compute_dedupe_key,
    fetch_since_days,
    fetch_by_notice_id,
    fetch_latest_for_digest,
)


def _sample_normalized(**overrides):
    base = {
        "notice_id": "N001",
        "solicitation_number": "SOL-1",
        "posted_date": "2024-06-01",
        "agency": "DARPA",
        "title": "Test Opp",
        "notice_type": "Sources Sought",
        "naics": "541715",
        "set_aside": "SDVOSB",
        "response_deadline": "2024-07-01",
        "link": "https://sam.gov/opp/N001",
    }
    base.update(overrides)
    return base


class TestComputeDedupeKey:
    def test_notice_id_key(self):
        assert compute_dedupe_key({"notice_id": "ABC"}) == "notice:ABC"

    def test_fallback_key(self):
        key = compute_dedupe_key({
            "solicitation_number": "SOL-1",
            "posted_date": "2024-01-01",
            "agency": "Army",
        })
        assert key == "fallback:SOL-1|2024-01-01|Army"

    def test_empty_notice_id_uses_fallback(self):
        key = compute_dedupe_key({"notice_id": "", "solicitation_number": "S1"})
        assert key.startswith("fallback:")

    def test_whitespace_notice_id_uses_fallback(self):
        key = compute_dedupe_key({"notice_id": "  ", "solicitation_number": "S2"})
        assert key.startswith("fallback:")


class TestFetchSinceDays:
    def test_returns_recent(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        upsert_opportunity(db, _sample_normalized(), {}, 15, ["r1"])
        rows = fetch_since_days(db, days=7)
        assert len(rows) == 1

    def test_empty_db(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        rows = fetch_since_days(db, days=7)
        assert len(rows) == 0


class TestFetchByNoticeId:
    def test_found(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        upsert_opportunity(db, _sample_normalized(notice_id="X1"), {}, 10, ["r"])
        result = fetch_by_notice_id(db, "X1")
        assert result is not None
        assert result.notice_id == "X1"
        assert result.score == 10

    def test_not_found(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        assert fetch_by_notice_id(db, "NOPE") is None


class TestFetchLatestForDigest:
    def test_filters_by_min_score(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        upsert_opportunity(db, _sample_normalized(notice_id="A"), {}, 5, [])
        upsert_opportunity(db, _sample_normalized(notice_id="B"), {}, 20, [])
        rows = fetch_latest_for_digest(db, min_score=10, limit=10)
        assert len(rows) == 1

    def test_respects_limit(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        for i in range(5):
            upsert_opportunity(db, _sample_normalized(notice_id=f"L{i}"), {}, 15, [])
        rows = fetch_latest_for_digest(db, min_score=0, limit=3)
        assert len(rows) == 3


class TestUpsertOpportunity:
    def test_missing_optional_fields(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        result = upsert_opportunity(db, {"notice_id": "MIN"}, {}, 0, [])
        assert result is True

    def test_different_keys_both_insert(self, tmp_path):
        db = tmp_path / "test.sqlite"
        init_db(db)
        r1 = upsert_opportunity(db, _sample_normalized(notice_id="A"), {}, 10, [])
        r2 = upsert_opportunity(db, _sample_normalized(notice_id="B"), {}, 20, [])
        assert r1 is True
        assert r2 is True
