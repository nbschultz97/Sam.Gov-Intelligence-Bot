"""Tests for ceradon_sam_bot.sam_client."""
from unittest.mock import patch, MagicMock
import pytest
import requests

from ceradon_sam_bot.sam_client import SamClient, SamClientConfig


@pytest.fixture
def client():
    cfg = SamClientConfig(
        api_key="test-key",
        max_retries=1,
        backoff_seconds=0.01,
        rate_limit_per_second=1000,
        timeout_seconds=5,
    )
    return SamClient(cfg)


class TestSamClientConfig:
    def test_defaults(self):
        cfg = SamClientConfig(api_key="k")
        assert cfg.page_size == 100
        assert cfg.max_retries == 4
        assert "sam.gov" in cfg.base_url


class TestSearchOpportunities:
    def test_single_page(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "totalRecords": 2,
            "opportunitiesData": [
                {"noticeId": "a", "title": "One"},
                {"noticeId": "b", "title": "Two"},
            ],
        }
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_resp):
            results = list(client.search_opportunities({"keywords": "WiFi"}))

        assert len(results) == 2
        assert results[0]["noticeId"] == "a"

    def test_empty_response(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"totalRecords": 0, "opportunitiesData": []}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_resp):
            results = list(client.search_opportunities({}))

        assert results == []

    def test_pagination(self, client):
        page1 = MagicMock()
        page1.status_code = 200
        page1.json.return_value = {
            "totalRecords": 3,
            "opportunitiesData": [{"id": "1"}, {"id": "2"}],
        }
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.status_code = 200
        page2.json.return_value = {
            "totalRecords": 3,
            "opportunitiesData": [{"id": "3"}],
        }
        page2.raise_for_status = MagicMock()

        # Override page_size for test
        client._config.page_size = 2

        with patch.object(client._session, "get", side_effect=[page1, page2]):
            results = list(client.search_opportunities({}))

        assert len(results) == 3

    def test_server_error_retries_then_raises(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500")

        with patch.object(client._session, "get", side_effect=requests.HTTPError("500")):
            with pytest.raises(requests.HTTPError):
                list(client.search_opportunities({}))

    def test_api_key_in_query(self):
        cfg = SamClientConfig(api_key="qk", api_key_in_query=True, max_retries=0, rate_limit_per_second=1000)
        c = SamClient(cfg)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"totalRecords": 0, "opportunitiesData": []}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(c._session, "get", return_value=mock_resp) as mock_get:
            list(c.search_opportunities({"keywords": "test"}))
            call_kwargs = mock_get.call_args
            assert call_kwargs[1]["params"]["api_key"] == "qk"
