"""Tests for ceradon_sam_bot.render."""
from ceradon_sam_bot.render import render_digest


class TestRenderDigest:
    def test_no_rows(self):
        result = render_digest([])
        assert "No opportunities" in result

    def test_single_row(self):
        rows = [{
            "title": "WiFi Sensor Contract",
            "agency": "Army",
            "notice_type": "Presolicitation",
            "naics": "541511",
            "set_aside": "SDVOSBC",
            "posted_date": "2026-01-15",
            "response_deadline": "2026-02-15",
            "score": 95,
            "link": "https://sam.gov/opp/abc/view",
        }]
        result = render_digest(rows)
        assert "WiFi Sensor Contract" in result
        assert "Army" in result
        assert "95" in result
        assert "sam.gov" in result
        assert result.startswith("Ceradon SAM Opportunity Digest")

    def test_multiple_rows_numbered(self):
        rows = [
            {"title": f"Opp {i}", "agency": "DoD", "notice_type": "RFP",
             "naics": "541", "set_aside": "", "posted_date": "2026-01-01",
             "response_deadline": "2026-02-01", "score": 50 + i,
             "link": f"https://sam.gov/opp/{i}/view"}
            for i in range(3)
        ]
        result = render_digest(rows)
        assert "1. Opp 0" in result
        assert "2. Opp 1" in result
        assert "3. Opp 2" in result
