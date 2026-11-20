#!/usr/bin/env python3
"""
HLTV Match Snapshot Creator

Quickly scrape match IDs from HLTV results pages into a frozen snapshot.

Usage:
    python scripts/create_match_snapshot.py --num_ids 30 --output 2026-data/scrape_test/match_snapshot_smoke.json
    python scripts/create_match_snapshot.py --until_date 2023-09-27 --output 2026-data/match_snapshot_cs2.json
    python scripts/create_match_snapshot.py --until_date 2023-09-27 --resume --output 2026-data/match_snapshot_cs2.json
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup
from curl_cffi import requests as creq

CS2_DEFAULT_CUTOFF = "2023-09-27"


def text_or_none(el) -> Optional[str]:
    if el is None:
        return None
    value = el.get_text(" ", strip=True)
    return value or None


class MatchSnapshotCreator:
    def __init__(
        self,
        num_ids: Optional[int] = 15000,
        output_file: str = "data/match_snapshot.json",
        until_date: Optional[str] = None,
        resume: bool = False,
        checkpoint_every_pages: int = 5,
        page_delay: float = 2.0,
    ):
        self.num_ids = num_ids
        self.output_file = output_file
        self.until_date = until_date
        self.resume = resume
        self.checkpoint_every_pages = checkpoint_every_pages
        self.page_delay = page_delay
        self.base_url = "https://www.hltv.org"
        self.results_url = f"{self.base_url}/results"
        self.checkpoint_file = f"{os.path.splitext(output_file)[0]}.checkpoint.json"

        # curl_cffi Chrome impersonation — cloudscraper is currently CF-blocked on HLTV.
        self.session = creq.Session()
        self.impersonate = "chrome"

        out_dir = os.path.dirname(output_file)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        self.until_dt = None
        if until_date:
            self.until_dt = datetime.strptime(until_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    def get_page_content(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, impersonate=self.impersonate, timeout=30)
                if response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    print(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                if response.status_code == 403 or "Just a moment" in response.text:
                    time.sleep((attempt + 1) * 5)
                    continue
                response.raise_for_status()
                return BeautifulSoup(response.text, "html.parser")
            except Exception as exc:
                if attempt == max_retries - 1:
                    print(f"Error fetching {url}: {exc}")
                    return None
                time.sleep((attempt + 1) * 5)
        return None

    def extract_match_id_from_element(self, match_element, page_offset: int) -> Optional[Dict[str, Any]]:
        try:
            match_link = match_element.select_one("a.a-reset")
            if not match_link or "href" not in match_link.attrs:
                return None

            href = match_link["href"]
            parts = href.split("/")
            if len(parts) < 3:
                return None
            try:
                match_id_int = int(parts[2])
            except ValueError:
                return None

            team_elements = match_element.select(".team")
            team1_name = team_elements[0].get_text(strip=True) if len(team_elements) > 0 else "Unknown"
            team2_name = team_elements[1].get_text(strip=True) if len(team_elements) > 1 else "Unknown"

            result_score = match_element.select_one(".result-score")
            score = result_score.get_text(strip=True) if result_score else "0-0"

            # Results rows expose completion-ish grouping time as data-zonedgrouping-entry-unix
            unix_ms = match_element.get("data-zonedgrouping-entry-unix")
            if not unix_ms:
                unix_el = match_element.select_one("[data-unix], [data-zonedgrouping-entry-unix]")
                if unix_el is not None:
                    unix_ms = unix_el.get("data-zonedgrouping-entry-unix") or unix_el.get("data-unix")

            match_date_utc = None
            data_unix_ms = None
            if unix_ms and str(unix_ms).isdigit():
                data_unix_ms = int(unix_ms)
                try:
                    match_date_utc = datetime.fromtimestamp(data_unix_ms / 1000, tz=timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                except (TypeError, ValueError, OSError):
                    match_date_utc = None

            return {
                "match_id": match_id_int,
                "team1": team1_name,
                "team2": team2_name,
                "score": score,
                "url": f"{self.base_url}{href}",
                "page_offset": page_offset,
                "match_date_utc": match_date_utc,
                "data_unix_ms": data_unix_ms,
            }
        except Exception as exc:
            print(f"Error extracting match ID: {exc}")
            return None

    def _load_checkpoint(self) -> Dict[str, Any]:
        if not self.resume or not os.path.exists(self.checkpoint_file):
            return {"page_offset": 0, "matches": [], "pages_scraped": 0, "reached_cutoff": False}
        with open(self.checkpoint_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(
            f"Resuming from checkpoint: offset={data.get('page_offset')} "
            f"matches={len(data.get('matches', []))}"
        )
        return data

    def _save_checkpoint(self, page_offset: int, matches: List[Dict[str, Any]],
                         pages_scraped: int, reached_cutoff: bool = False) -> None:
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "page_offset": page_offset,
            "pages_scraped": pages_scraped,
            "total_matches": len(matches),
            "until_date": self.until_date,
            "reached_cutoff": reached_cutoff,
            "matches": matches,
        }
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    def _parse_headline_date_unix(self, headline: Optional[str]) -> Optional[int]:
        """Parse 'Results for November 20th 2026' into ms since epoch at 12:00 UTC."""
        if not headline:
            return None
        import re
        from calendar import month_name

        m = re.search(
            r"Results for\s+([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\d{4})",
            headline,
        )
        if not m:
            return None
        month_s, day_s, year_s = m.groups()
        months = {name.lower(): i for i, name in enumerate(month_name) if name}
        month = months.get(month_s.lower())
        if not month:
            return None
        try:
            dt = datetime(int(year_s), month, int(day_s), 12, 0, 0, tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None

    def _past_cutoff(self, match: Dict[str, Any], current_day_unix: Optional[int]) -> bool:
        if not self.until_dt:
            return False
        unix_ms = match.get("data_unix_ms")
        if unix_ms is None and current_day_unix is not None:
            unix_ms = current_day_unix
        if unix_ms is None:
            return False
        try:
            dt = datetime.fromtimestamp(int(unix_ms) / 1000, tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return False
        # Stop once results are strictly before the cutoff day
        return dt.date() < self.until_dt.date()

    def create_snapshot(self) -> List[Dict[str, Any]]:
        state = self._load_checkpoint()
        all_match_data: List[Dict[str, Any]] = list(state.get("matches") or [])
        page_offset = int(state.get("page_offset") or 0)
        pages_scraped = int(state.get("pages_scraped") or 0)
        matches_per_page = 100
        reached_cutoff = bool(state.get("reached_cutoff"))

        mode = f"until_date={self.until_date}" if self.until_date else f"num_ids={self.num_ids}"
        print(f"Creating snapshot ({mode})")
        if self.num_ids and not self.until_date:
            print(f"Estimated pages: ~{max(1, self.num_ids // matches_per_page)}")
        print("")

        start_time = time.time()
        seen_ids = {m["match_id"] for m in all_match_data}

        try:
            while True:
                if self.until_date and reached_cutoff:
                    break
                if self.num_ids is not None and not self.until_date and len(all_match_data) >= self.num_ids:
                    break

                current_page_url = f"{self.results_url}?offset={page_offset}"
                print(
                    f"Page {pages_scraped + 1} (offset={page_offset}) - "
                    f"{len(all_match_data)} IDs collected...",
                    end="\r",
                )

                soup = self.get_page_content(current_page_url)
                if not soup:
                    print(f"\nFailed to fetch page at offset {page_offset}")
                    break

                # Date headers on results pages often carry data-unix for the day
                current_day_unix = None
                day_stops_on_page = False

                # Walk DOM roughly in order: update current day when headline date nodes appear
                nodes = soup.select(".results-all .standard-box, .result-con, .results-sublist")
                if not nodes:
                    nodes = soup.select(".result-con")

                all_matches_on_page = soup.select(".result-con")
                if not all_matches_on_page:
                    print(f"\nNo matches found on page at offset {page_offset}")
                    break

                # Headline dates: "Results for November 20th 2026" under each .results-sublist
                headline_by_match = {}
                last_headline_unix = None
                for el in soup.select(".results-sublist, .result-con"):
                    classes = set(el.get("class") or [])
                    if "results-sublist" in classes:
                        headline = el.select_one("span.standard-headline")
                        last_headline_unix = self._parse_headline_date_unix(text_or_none(headline))
                    if "result-con" in classes:
                        headline_by_match[id(el)] = last_headline_unix

                for match_element in all_matches_on_page:
                    if self.num_ids is not None and not self.until_date and len(all_match_data) >= self.num_ids:
                        break

                    match_data = self.extract_match_id_from_element(match_element, page_offset)
                    if not match_data:
                        continue

                    if match_data.get("data_unix_ms") is None:
                        headline_unix = headline_by_match.get(id(match_element))
                        if headline_unix is not None:
                            match_data["data_unix_ms"] = headline_unix
                            try:
                                match_data["match_date_utc"] = datetime.fromtimestamp(
                                    headline_unix / 1000, tz=timezone.utc
                                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                            except (TypeError, ValueError, OSError):
                                pass

                    if match_data.get("data_unix_ms") is not None:
                        current_day_unix = match_data["data_unix_ms"]

                    if self._past_cutoff(match_data, current_day_unix):
                        reached_cutoff = True
                        day_stops_on_page = True
                        break

                    if match_data["match_id"] in seen_ids:
                        continue
                    seen_ids.add(match_data["match_id"])
                    all_match_data.append(match_data)

                pages_scraped += 1
                page_offset += matches_per_page

                if pages_scraped % self.checkpoint_every_pages == 0 or reached_cutoff:
                    self._save_checkpoint(page_offset, all_match_data, pages_scraped, reached_cutoff)
                    print(
                        f"\nCheckpoint saved: {len(all_match_data)} matches, next offset={page_offset}"
                    )

                if day_stops_on_page or reached_cutoff:
                    print(f"\nReached until_date cutoff {self.until_date}")
                    break

                # Continue condition
                should_continue = True
                if self.num_ids is not None and not self.until_date:
                    should_continue = len(all_match_data) < self.num_ids
                if should_continue and not reached_cutoff:
                    time.sleep(self.page_delay)
                else:
                    break

            elapsed_time = time.time() - start_time
            print("\n\nSnapshot creation complete!")
            print(f"Total match IDs collected: {len(all_match_data)}")
            print(f"Pages scraped this run: {pages_scraped}")
            print(f"Time elapsed: {elapsed_time / 60:.1f} minutes")
            self._save_checkpoint(page_offset, all_match_data, pages_scraped, reached_cutoff)
            return all_match_data

        except KeyboardInterrupt:
            print("\n\nSnapshot creation interrupted!")
            print(f"Match IDs collected so far: {len(all_match_data)}")
            self._save_checkpoint(page_offset, all_match_data, pages_scraped, reached_cutoff)
            return all_match_data
        except Exception as exc:
            print(f"\nError during snapshot creation: {exc}")
            self._save_checkpoint(page_offset, all_match_data, pages_scraped, reached_cutoff)
            return all_match_data

    def save_snapshot(self, match_data: List[Dict[str, Any]]):
        snapshot = {
            "metadata": {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "total_matches": len(match_data),
                "num_ids_requested": self.num_ids,
                "until_date": self.until_date,
                "cutoff_note": (
                    f"Results on/after {self.until_date} UTC retained; earlier rows stop the crawl"
                    if self.until_date
                    else None
                ),
                "first_match_id": match_data[0]["match_id"] if match_data else None,
                "last_match_id": match_data[-1]["match_id"] if match_data else None,
            },
            "matches": match_data,
        }
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
        print(f"\nSnapshot saved to: {self.output_file}")
        print(f"File size: {os.path.getsize(self.output_file) / 1024:.1f} KB")

    def run(self):
        print("=" * 60)
        print("HLTV Match Snapshot Creator")
        print("=" * 60)
        print(f"Target IDs: {self.num_ids}")
        print(f"Until date: {self.until_date}")
        print(f"Output file: {self.output_file}")
        print(f"Resume: {self.resume}")
        print("=" * 60)
        print("")

        match_data = self.create_snapshot()
        if match_data:
            self.save_snapshot(match_data)
            print("\nSnapshot creation successful!")
            print(
                f"First match: {match_data[0]['team1']} vs {match_data[0]['team2']} "
                f"(ID: {match_data[0]['match_id']})"
            )
            print(
                f"Last match: {match_data[-1]['team1']} vs {match_data[-1]['team2']} "
                f"(ID: {match_data[-1]['match_id']})"
            )
        else:
            print("\nSnapshot creation failed - no data collected")


def main():
    parser = argparse.ArgumentParser(description="HLTV Match Snapshot Creator")
    parser.add_argument("--num_ids", "-n", type=int, default=None,
                        help="Number of match IDs to collect (ignored when --until_date is set unless also provided as a cap)")
    parser.add_argument("--output", "-o", type=str, default="data/match_snapshot.json",
                        help="Output JSON path")
    parser.add_argument(
        "--until_date",
        type=str,
        default=None,
        help=f"Walk results until this UTC date (YYYY-MM-DD). Use {CS2_DEFAULT_CUTOFF} for CS2-era backfill.",
    )
    parser.add_argument(
        "--cs2",
        action="store_true",
        help=f"Shortcut for --until_date {CS2_DEFAULT_CUTOFF}",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint beside the output file")
    parser.add_argument("--checkpoint_every_pages", type=int, default=5)
    parser.add_argument("--page_delay", type=float, default=2.0)
    args = parser.parse_args()

    until_date = args.until_date
    if args.cs2:
        until_date = CS2_DEFAULT_CUTOFF

    num_ids = args.num_ids
    if num_ids is None and until_date is None:
        num_ids = 15000

    creator = MatchSnapshotCreator(
        num_ids=num_ids,
        output_file=args.output,
        until_date=until_date,
        resume=args.resume,
        checkpoint_every_pages=args.checkpoint_every_pages,
        page_delay=args.page_delay,
    )
    creator.run()


if __name__ == "__main__":
    main()
