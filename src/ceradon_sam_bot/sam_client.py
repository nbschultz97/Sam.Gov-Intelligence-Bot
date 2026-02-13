from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import requests

LOGGER = logging.getLogger(__name__)


@dataclass
class SamClientConfig:
    api_key: str
    api_key_in_query: bool = False
    base_url: str = "https://api.sam.gov/opportunities/v2/search"
    page_size: int = 100
    timeout_seconds: int = 30
    max_retries: int = 4
    backoff_seconds: float = 1.5
    rate_limit_per_second: float = 0.8


class SamClient:
    def __init__(self, config: SamClientConfig) -> None:
        self._config = config
        self._session = requests.Session()
        self._last_request_time: Optional[float] = None

    def _rate_limit(self) -> None:
        if not self._last_request_time:
            return
        min_interval = 1.0 / self._config.rate_limit_per_second
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def _request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        attempt = 0
        headers = {}
        if not self._config.api_key_in_query:
            headers["X-API-Key"] = self._config.api_key
        while True:
            self._rate_limit()
            try:
                response = self._session.get(
                    self._config.base_url,
                    params=params,
                    headers=headers,
                    timeout=self._config.timeout_seconds,
                )
                self._last_request_time = time.monotonic()
                if response.status_code >= 500:
                    raise requests.HTTPError(f"Server error {response.status_code}")
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                attempt += 1
                if attempt > self._config.max_retries:
                    LOGGER.error("SAM API request failed after retries", exc_info=exc)
                    raise
                sleep_seconds = self._config.backoff_seconds * (2 ** (attempt - 1))
                LOGGER.warning(
                    "SAM API request failed, retrying",
                    extra={"attempt": attempt, "sleep": sleep_seconds},
                )
                time.sleep(sleep_seconds)

    def search_opportunities(self, params: Dict[str, Any], max_pages: int = 3) -> Iterable[Dict[str, Any]]:
        page_size = self._config.page_size
        offset = 0
        total_records = None
        pages_fetched = 0
        while True:
            page_params = dict(params)
            page_params["limit"] = page_size
            page_params["offset"] = offset
            if self._config.api_key_in_query:
                page_params["api_key"] = self._config.api_key
            payload = self._request(page_params)
            data = payload.get("opportunitiesData", []) or []
            if total_records is None:
                total_records = payload.get("totalRecords")
                LOGGER.info("SAM.gov query matched %s records (capping at %d pages)", total_records, max_pages)
            for item in data:
                yield item
            pages_fetched += 1
            if not data:
                break
            offset += page_size
            if pages_fetched >= max_pages:
                LOGGER.info("Reached max pages (%d), moving on", max_pages)
                break
            if total_records is not None and offset >= int(total_records):
                break
