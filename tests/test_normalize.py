"""Tests for ceradon_sam_bot.normalize."""
from ceradon_sam_bot.normalize import normalize_opportunity


class TestNormalizeOpportunity:
    def test_basic_fields(self):
        raw = {
            "noticeId": "abc123",
            "solicitationNumber": "SOL-001",
            "title": "Test Contract",
            "agency": "DoD",
            "noticeType": "Presolicitation",
            "naicsCode": "541511",
            "typeOfSetAside": "SDVOSBC",
            "postedDate": "2026-01-15",
            "responseDeadLine": "2026-02-15",
            "description": "Test description",
        }
        result = normalize_opportunity(raw)
        assert result["notice_id"] == "abc123"
        assert result["title"] == "Test Contract"
        assert result["agency"] == "DoD"
        assert result["naics"] == "541511"
        assert result["set_aside"] == "SDVOSBC"
        assert result["link"] == "https://sam.gov/opp/abc123/view"

    def test_fallback_fields(self):
        raw = {
            "noticeId": "",
            "solicitationNumber": "SOL-002",
            "title": "Fallback Test",
            "fullParentPathName": "Army",
            "naics": "334511",
            "setAside": "SBA",
            "responseDeadline": "2026-03-01",
            "summary": "A summary",
        }
        result = normalize_opportunity(raw)
        assert result["agency"] == "Army"
        assert result["naics"] == "334511"
        assert result["set_aside"] == "SBA"
        assert result["response_deadline"] == "2026-03-01"
        assert result["description"] == "A summary"
        assert "SOL-002" in result["link"]

    def test_empty_raw(self):
        result = normalize_opportunity({})
        assert result["notice_id"] == ""
        assert result["title"] == ""
        assert result["link"] == ""
        assert result["raw"] == {}

    def test_none_values_become_empty_string(self):
        raw = {"noticeId": None, "title": None, "agency": None}
        result = normalize_opportunity(raw)
        assert result["notice_id"] == ""
        assert result["title"] == ""

    def test_raw_preserved(self):
        raw = {"noticeId": "x", "extra_field": "keep_me"}
        result = normalize_opportunity(raw)
        assert result["raw"] is raw
