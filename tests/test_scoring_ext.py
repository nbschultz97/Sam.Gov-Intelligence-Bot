"""Extended tests for ceradon_sam_bot.scoring — edge cases, date parsing, set-asides."""
import datetime as dt

import pytest

from ceradon_sam_bot.config import Config, Digest, Filters, KeywordWeights, Scoring
from ceradon_sam_bot.scoring import score_opportunity, _parse_date


def _make_config(**overrides):
    defaults = dict(
        filters=Filters(naics_include=["541715"], preferred_notice_types=["Sources Sought"],
                         exclude_notice_types=[], posted_from_days=14),
        keywords=KeywordWeights(positive={"sensor": 3, "radar": 2}, negative={"construction": 5}),
        scoring=Scoring(include_in_digest_score=10, naics_match_boost=4, notice_type_boost=3,
                        set_aside_boost=2, deadline_urgency_boost=2),
        digest=Digest(max_items=10),
    )
    defaults.update(overrides)
    return Config(**defaults)


class TestParseDate:
    def test_iso_format(self):
        assert _parse_date("2024-06-15") == dt.date(2024, 6, 15)

    def test_iso_with_time(self):
        assert _parse_date("2024-06-15T12:00:00Z") == dt.date(2024, 6, 15)

    def test_empty_string(self):
        assert _parse_date("") is None

    def test_garbage(self):
        assert _parse_date("not-a-date") is None

    def test_iso_with_offset(self):
        assert _parse_date("2024-06-15T12:00:00+05:00") == dt.date(2024, 6, 15)


class TestScoreOpportunityEdgeCases:
    def test_negative_keywords(self):
        config = _make_config()
        opp = {"title": "Construction project", "description": "", "agency": ""}
        score, reasons = score_opportunity(opp, config)
        assert score < 0
        assert any("construction" in r for r in reasons)

    def test_sdvosb_set_aside(self):
        config = _make_config()
        opp = {"title": "Test", "description": "", "agency": "",
               "set_aside": "SDVOSB", "set_aside_description": ""}
        score, reasons = score_opportunity(opp, config)
        assert any("sdvosb" in r for r in reasons)

    def test_service_disabled_description(self):
        config = _make_config()
        opp = {"title": "Test", "description": "", "agency": "",
               "set_aside": "", "set_aside_description": "Service-Disabled Veteran"}
        score, reasons = score_opportunity(opp, config)
        assert any("sdvosb" in r for r in reasons)

    def test_deadline_urgency(self):
        config = _make_config()
        soon = (dt.date.today() + dt.timedelta(days=3)).isoformat()
        opp = {"title": "Test", "description": "", "agency": "",
               "response_deadline": soon}
        score, reasons = score_opportunity(opp, config)
        assert any("deadline_in" in r for r in reasons)

    def test_no_deadline_no_urgency(self):
        config = _make_config()
        opp = {"title": "Test", "description": "", "agency": "",
               "response_deadline": ""}
        score, reasons = score_opportunity(opp, config)
        assert not any("deadline" in r for r in reasons)

    def test_far_deadline_no_urgency(self):
        config = _make_config()
        far = (dt.date.today() + dt.timedelta(days=60)).isoformat()
        opp = {"title": "Test", "description": "", "agency": "",
               "response_deadline": far}
        score, reasons = score_opportunity(opp, config)
        assert not any("deadline" in r for r in reasons)

    def test_empty_opportunity(self):
        config = _make_config()
        score, reasons = score_opportunity({}, config)
        assert score == 0
        assert reasons == []

    def test_naics_match(self):
        config = _make_config()
        opp = {"title": "Test", "description": "", "agency": "", "naics": "541715"}
        score, reasons = score_opportunity(opp, config)
        assert any("naics" in r for r in reasons)

    def test_notice_type_boost(self):
        config = _make_config()
        opp = {"title": "Test", "description": "", "agency": "",
               "notice_type": "Sources Sought"}
        score, reasons = score_opportunity(opp, config)
        assert any("notice_type" in r for r in reasons)
