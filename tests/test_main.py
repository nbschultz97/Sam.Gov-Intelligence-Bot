"""Tests for ceradon_sam_bot.main — orchestrator functions."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ceradon_sam_bot.main import (
    JsonFormatter,
    RunIdFilter,
    _build_query_params,
    _process_opportunities,
    _require_env,
    build_parser,
)


class TestBuildQueryParams:
    def test_basic_params(self):
        params = _build_query_params(7, title="sensor")
        assert "postedFrom" in params
        assert "postedTo" in params
        assert params["title"] == "sensor"

    def test_ncode(self):
        params = _build_query_params(7, ncode="541715")
        assert params["ncode"] == "541715"
        assert "title" not in params

    def test_no_optional(self):
        params = _build_query_params(30)
        assert "title" not in params
        assert "ncode" not in params


class TestRequireEnv:
    def test_returns_value(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR_XYZ", "hello")
        assert _require_env("TEST_VAR_XYZ") == "hello"

    def test_uses_default(self):
        assert _require_env("NONEXISTENT_VAR_ABC", "fallback") == "fallback"

    def test_raises_when_missing(self):
        with pytest.raises(RuntimeError, match="Missing required"):
            _require_env("NONEXISTENT_VAR_ABC")


class TestJsonFormatter:
    def test_formats_json(self):
        import json
        import logging

        fmt = JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=(), exc_info=None,
        )
        output = fmt.format(record)
        data = json.loads(output)
        assert data["message"] == "hello"
        assert data["level"] == "INFO"


class TestRunIdFilter:
    def test_adds_run_id(self):
        import logging

        f = RunIdFilter("abc-123")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hi", args=(), exc_info=None,
        )
        f.filter(record)
        assert record.run_id == "abc-123"


class TestProcessOpportunities:
    @patch("ceradon_sam_bot.main.upsert_opportunity", return_value=True)
    @patch("ceradon_sam_bot.main.score_opportunity", return_value=(80, ["keyword match"]))
    @patch("ceradon_sam_bot.main.normalize_opportunity")
    def test_processes_items(self, mock_norm, mock_score, mock_upsert, tmp_path):
        mock_norm.return_value = {"notice_type": "solicitation", "title": "Test"}
        config = MagicMock()
        config.filters.exclude_notice_types = []

        items = [{"noticeId": "1", "title": "Test"}]
        counts = _process_opportunities(items, config, tmp_path / "test.db")

        assert counts["processed"] == 1
        assert counts["saved"] == 1

    @patch("ceradon_sam_bot.main.normalize_opportunity")
    def test_skips_excluded_types(self, mock_norm, tmp_path):
        mock_norm.return_value = {"notice_type": "award"}
        config = MagicMock()
        config.filters.exclude_notice_types = ["award"]

        counts = _process_opportunities([{"noticeId": "1"}], config, tmp_path / "test.db")
        assert counts["skipped"] == 1


class TestBuildParser:
    def test_run_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["run", "--config", "cfg.yaml", "--once"])
        assert args.command == "run"
        assert args.config == "cfg.yaml"

    def test_backfill_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["backfill", "--config", "cfg.yaml", "--days", "90"])
        assert args.command == "backfill"
        assert args.days == 90
