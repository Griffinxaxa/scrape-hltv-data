"""Scrape HLTV match result pages into the 2026 multi-grain archive."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

# Allow `python scripts/hltv2026/runner.py` and `python -m hltv2026.runner`
_SCRIPTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from hltv2026.session import HLTVSession
from hltv2026.parsers import parse_match_page
from hltv2026.writers import ArchiveWriter


def load_snapshot(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("matches", data if isinstance(data, list) else [])


def is_valid_keep(parsed: Dict[str, Any], require_bo3_plus: bool = True) -> tuple[bool, str]:
    series = parsed.get("series") or {}
    if not series.get("hltv_match_id"):
        return False, "missing_match_id"
    status = (series.get("status") or "").lower()
    if status and "match over" not in status:
        return False, f"status={series.get('status')}"
    if series.get("forfeit") or series.get("walkover"):
        return False, "forfeit_or_walkover"
    best_of = series.get("best_of")
    if require_bo3_plus and best_of is not None and best_of < 3:
        return False, f"best_of={best_of}"
    maps = [m for m in (parsed.get("maps") or []) if m.get("played_status") == "played"]
    if require_bo3_plus and len(maps) < 1:
        return False, "no_played_maps"
    lineups = parsed.get("lineups") or []
    if len(lineups) < 2:
        return False, "missing_lineups"
    player_stats = parsed.get("player_stats") or []
    series_both = [
        r for r in player_stats
        if r.get("map_stats_id") is None and r.get("side") == "both"
    ]
    if len(series_both) < 2:
        return False, "missing_series_player_stats"
    if series.get("team1_id") is None or series.get("team2_id") is None:
        return False, "missing_team_ids"
    if series.get("winner_team_id") is None and not series.get("incomplete"):
        return False, "missing_winner"
    return True, "ok"


def _load_already_scraped(output_dir: str, progress_path: str) -> Set[int]:
    """IDs already written to disk — safe to skip after Ctrl+C / restart."""
    import csv

    seen: Set[int] = set()
    if os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                progress = json.load(f)
            for mid in progress.get("seen_ids") or []:
                seen.add(int(mid))
        except Exception:
            pass

    series_csv = os.path.join(output_dir, "series.csv")
    if os.path.exists(series_csv):
        try:
            with open(series_csv, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    mid = row.get("hltv_match_id")
                    if mid not in (None, ""):
                        seen.add(int(mid))
        except Exception:
            pass
    return seen


def scrape_match_ids(
    match_refs: List[Dict[str, Any]],
    output_dir: str,
    target_valid: int = 5,
    extra_ids: Optional[List[int]] = None,
    page_delay: float = 0.3,
    match_delay: float = 0.1,
) -> Dict[str, Any]:
    session = HLTVSession(page_delay=page_delay, match_delay=match_delay)
    writer = ArchiveWriter(output_dir)
    progress_path = os.path.join(output_dir, "scraper_progress.json")

    seen: Set[int] = _load_already_scraped(output_dir, progress_path)
    # Count series rows already on disk toward the target so resume doesn't re-aim for N more.
    import csv
    kept = 0
    series_csv = os.path.join(output_dir, "series.csv")
    if os.path.exists(series_csv):
        with open(series_csv, newline="", encoding="utf-8") as f:
            kept = sum(1 for _ in csv.DictReader(f))
    processed = 0
    results: List[Dict[str, Any]] = []
    if seen:
        print(f"Resuming: skipping {len(seen)} already-scraped match IDs ({kept} series rows on disk)")

    queue: List[Dict[str, Any]] = []
    for mid in extra_ids or []:
        queue.append({"match_id": mid, "url": f"https://www.hltv.org/matches/{mid}/-"})
    queue.extend(match_refs)

    state = {
        "seen": seen,
        "kept": kept,
        "processed": processed,
        "results": results,
    }
    try:
        return _scrape_loop(
            queue=queue,
            session=session,
            writer=writer,
            progress_path=progress_path,
            output_dir=output_dir,
            target_valid=target_valid,
            extra_ids=extra_ids or [],
            state=state,
            match_delay=match_delay,
        )
    except KeyboardInterrupt:
        print("\nInterrupted — CSV rows for finished matches are already saved.")
        print(f"Re-run the same command to resume; {len(state['seen'])} IDs will be skipped.")
        summary = {
            "processed": state["processed"],
            "kept": state["kept"],
            "target_valid": target_valid,
            "results": state["results"],
            "output_dir": output_dir,
            "interrupted": True,
        }
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({
                "processed": state["processed"],
                "kept": state["kept"],
                "target_valid": target_valid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "seen_ids": sorted(state["seen"]),
                "interrupted": True,
            }, f, indent=2)
        with open(os.path.join(output_dir, "smoke_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        return summary


def _scrape_loop(
    queue,
    session,
    writer,
    progress_path,
    output_dir,
    target_valid,
    extra_ids,
    state,
    match_delay,
) -> Dict[str, Any]:
    seen: Set[int] = state["seen"]
    kept = state["kept"]
    processed = state["processed"]
    results = state["results"]

    for ref in queue:
        if kept >= target_valid and not (extra_ids and ref.get("match_id") in (extra_ids or [])):
            # Still process forced fixture ids even after target reached
            if extra_ids and ref.get("match_id") in set(extra_ids):
                pass
            elif kept >= target_valid:
                # Only skip non-forced once target met AND this isn't a forced id still pending
                remaining_forced = [i for i in (extra_ids or []) if i not in seen]
                if not remaining_forced:
                    break

        match_id = ref.get("match_id")
        if match_id in seen:
            continue
        seen.add(match_id)
        state["seen"] = seen
        url = ref.get("url") or f"https://www.hltv.org/matches/{match_id}/-"
        print(f"Fetching match {match_id}: {url}")
        soup = session.get_soup(url)
        processed += 1
        state["processed"] = processed
        if soup is None:
            results.append({"match_id": match_id, "kept": False, "reason": "fetch_failed"})
            time.sleep(match_delay)
            continue

        # Prefer canonical URL for subsequent reference
        parsed = parse_match_page(soup)
        series = parsed.get("series") or {}
        # Re-key seen with canonical id if redirect changed it
        canonical_id = series.get("hltv_match_id") or match_id
        seen.add(canonical_id)
        state["seen"] = seen

        is_forced = match_id in set(extra_ids or []) or canonical_id in set(extra_ids or [])
        ok, reason = is_valid_keep(parsed, require_bo3_plus=not is_forced)
        # Forced fixture always written for acceptance even if filter would skip
        if ok or is_forced:
            if not ok and is_forced:
                print(f"  Forced keep for fixture {canonical_id} despite filter: {reason}")
            counts = writer.write_match(parsed)
            if ok:
                kept += 1
                state["kept"] = kept
            print(
                f"  Saved match {canonical_id} "
                f"(kept={kept}/{target_valid}) maps={counts.get('maps')} "
                f"veto={counts.get('veto_steps')} lineups={counts.get('lineups')} "
                f"player_stats={counts.get('player_stats')}"
            )
            results.append({
                "match_id": canonical_id,
                "kept": ok or is_forced,
                "reason": reason,
                "forced": is_forced,
                "counts": counts,
                "best_of": series.get("best_of"),
                "teams": f"{series.get('team1_name')} vs {series.get('team2_name')}",
            })
        else:
            print(f"  Skipped match {canonical_id}: {reason}")
            # Still record coverage for skipped pages
            coverage = parsed.get("coverage") or {}
            coverage["skipped"] = True
            coverage["skip_reason"] = reason
            writer._coverage = [c for c in writer._coverage if c.get("hltv_match_id") != canonical_id]
            writer._coverage.append(coverage)
            with open(writer.coverage_path, "w", encoding="utf-8") as f:
                json.dump(writer._coverage, f, indent=2)
            results.append({"match_id": canonical_id, "kept": False, "reason": reason})

        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({
                "processed": processed,
                "kept": kept,
                "target_valid": target_valid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "seen_ids": sorted(seen),
            }, f, indent=2)

        time.sleep(match_delay)

        if kept >= target_valid:
            remaining_forced = [i for i in (extra_ids or []) if i not in seen]
            if not remaining_forced:
                break

    summary = {
        "processed": processed,
        "kept": kept,
        "target_valid": target_valid,
        "results": results,
        "output_dir": output_dir,
    }
    summary_path = os.path.join(output_dir, "smoke_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"\nDone. kept={kept}/{target_valid}, processed={processed}")
    print(f"Summary: {summary_path}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="HLTV 2026 multi-grain match scraper")
    parser.add_argument("--snapshot_file", "-s", required=True, help="Match snapshot JSON")
    parser.add_argument("--output_dir", "-o", default="2026-data/scrape_test")
    parser.add_argument("--num_matches", "-n", type=int, default=5)
    parser.add_argument(
        "--fixture_id",
        type=int,
        action="append",
        default=None,
        help="Always scrape this match id (repeatable). Default includes 2400286.",
    )
    parser.add_argument("--no_fixture", action="store_true", help="Do not scrape the gold fixture")
    parser.add_argument("--match_delay", type=float, default=0.1)
    parser.add_argument("--page_delay", type=float, default=0.3)
    args = parser.parse_args()

    fixtures = [] if args.no_fixture else (args.fixture_id or [2400286])
    refs = load_snapshot(args.snapshot_file)
    scrape_match_ids(
        match_refs=refs,
        output_dir=args.output_dir,
        target_valid=args.num_matches,
        extra_ids=fixtures,
        page_delay=args.page_delay,
        match_delay=args.match_delay,
    )


if __name__ == "__main__":
    main()
