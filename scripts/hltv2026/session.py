"""Shared HTTP session with the existing scraper's rate-limit behavior.

Uses curl_cffi Chrome impersonation because plain cloudscraper is currently
blocked by HLTV Cloudflare on many match pages. Delays and 429 backoff match
the legacy scrapers.
"""

from __future__ import annotations

import time
from typing import Optional

from bs4 import BeautifulSoup
from curl_cffi import requests as creq


class HLTVSession:
    def __init__(self, page_delay: float = 0.3, match_delay: float = 0.1, warm: bool = True):
        self.page_delay = page_delay
        self.match_delay = match_delay
        self.session = creq.Session()
        self.impersonate = "chrome"
        self.base_url = "https://www.hltv.org"
        self._warmed = False
        if warm:
            self.warm_up()

    def warm_up(self) -> bool:
        try:
            response = self.session.get(
                f"{self.base_url}/results",
                impersonate=self.impersonate,
                timeout=30,
            )
            if response.status_code == 200 and "Just a moment" not in response.text:
                self._warmed = True
                time.sleep(self.page_delay)
                return True
            print(f"Warm-up got status {response.status_code}")
        except Exception as exc:
            print(f"Warm-up failed: {exc}")
        return False

    def get_soup(self, url: str, max_retries: int = 3, timeout: int = 30) -> Optional[BeautifulSoup]:
        for attempt in range(max_retries):
            try:
                response = self.session.get(
                    url,
                    impersonate=self.impersonate,
                    timeout=timeout,
                )
                if response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    print(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                if response.status_code == 403 or "Just a moment" in response.text:
                    print(f"Challenge/403 on {url}; re-warming (attempt {attempt + 1}/{max_retries})")
                    self.warm_up()
                    time.sleep((attempt + 1) * 5)
                    continue
                response.raise_for_status()
                return BeautifulSoup(response.content, "html.parser")
            except Exception as exc:
                if attempt == max_retries - 1:
                    print(f"Failed to fetch {url}: {exc}")
                    return None
                wait_time = (attempt + 1) * 5
                time.sleep(wait_time)
        return None

    def absolute_url(self, href: Optional[str]) -> Optional[str]:
        if not href:
            return None
        if href.startswith("http"):
            return href
        if href.startswith("//"):
            return "https:" + href
        if not href.startswith("/"):
            href = "/" + href
        return f"{self.base_url}{href}"
