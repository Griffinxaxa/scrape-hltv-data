"""Append multi-grain scrape results to CSV / JSON coverage files."""

from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, Iterable, List, Optional


GRAIN_COLUMNS: Dict[str, List[str]] = {
    "series": [
        "hltv_match_id", "match_url", "series_stats_id", "series_stats_url",
        "event_id", "event_name", "event_url",
        "team1_id", "team1_name", "team1_region",
        "team2_id", "team2_name", "team2_region",
        "best_of", "environment", "stage_raw", "format_notes",
        "scheduled_start_utc", "completed_at_utc", "status",
        "team1_score", "team2_score", "winner_team_id",
        "forfeit", "walkover", "incomplete", "match_notes",
        "demo_url", "capture_time_utc", "source_url",
    ],
    "maps": [
        "hltv_match_id", "map_ordinal", "map_name", "map_stats_id", "map_stats_url",
        "pick_side", "team1_rounds", "team2_rounds", "winner_team_id",
        "half1_team1_rounds", "half1_team2_rounds", "half1_team1_side", "half1_team2_side",
        "half2_team1_rounds", "half2_team2_rounds", "half2_team1_side", "half2_team2_side",
        "ot_team1_rounds", "ot_team2_rounds", "ot_team1_side", "ot_team2_side",
        "played_status", "left_team_name", "right_team_name",
    ],
    "veto_steps": [
        "hltv_match_id", "step_order", "actor_team_id", "action", "map_name", "raw_text",
    ],
    "lineups": [
        "hltv_match_id", "team_id", "team_name", "team_ordinal", "player_id",
        "nickname", "full_name", "nationality", "role_badges",
        "hltv_rank", "hltv_rank_url", "hltv_rank_date",
        "vrs_rank", "vrs_rank_url", "vrs_rank_date",
    ],
    "player_stats": [
        "hltv_match_id", "team_id", "player_id", "nickname", "map_stats_id", "side",
        "kills", "deaths", "eco_kills", "eco_deaths", "swing_pct",
        "adr", "eco_adr", "kast_pct", "eco_kast_pct", "rating", "rating_version",
    ],
    "h2h_maps": [
        "source_hltv_match_id", "prior_match_id", "prior_match_url", "prior_time_utc",
        "team1_id", "team1_name", "team1_players",
        "team2_id", "team2_name", "team2_players",
        "event_id", "event_name", "map_name",
        "team1_score", "team2_score", "winner_team_id",
    ],
    "past_match_links": [
        "source_hltv_match_id", "view", "subject_team_id", "subject_team_name",
        "opponent_team_id", "opponent_team_name", "recency_text", "format_or_map",
        "score_text", "outcome_for_subject", "prior_match_id", "prior_match_url",
    ],
    "vrs_forecast": [
        "hltv_match_id", "team_name", "before_points", "before_rank",
        "result_points_delta", "result_rank", "result_rank_delta",
    ],
}


class ArchiveWriter:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.coverage_path = os.path.join(output_dir, "coverage.json")
        self._coverage: List[Dict[str, Any]] = []
        if os.path.exists(self.coverage_path):
            try:
                with open(self.coverage_path, "r", encoding="utf-8") as f:
                    self._coverage = json.load(f)
            except Exception:
                self._coverage = []

    def _csv_path(self, grain: str) -> str:
        return os.path.join(self.output_dir, f"{grain}.csv")

    def append_rows(self, grain: str, rows: Iterable[Dict[str, Any]]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        columns = GRAIN_COLUMNS[grain]
        path = self._csv_path(grain)
        write_header = not os.path.exists(path) or os.path.getsize(path) == 0
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col) for col in columns})
        return len(rows)

    def write_match(self, parsed: Dict[str, Any]) -> Dict[str, int]:
        counts = {}
        series = parsed.get("series")
        if series:
            counts["series"] = self.append_rows("series", [series])
        for grain in ("maps", "veto_steps", "lineups", "player_stats",
                      "h2h_maps", "past_match_links", "vrs_forecast"):
            counts[grain] = self.append_rows(grain, parsed.get(grain) or [])

        coverage = parsed.get("coverage") or {}
        # Replace existing coverage entry for same match id
        mid = coverage.get("hltv_match_id")
        self._coverage = [c for c in self._coverage if c.get("hltv_match_id") != mid]
        self._coverage.append(coverage)
        with open(self.coverage_path, "w", encoding="utf-8") as f:
            json.dump(self._coverage, f, indent=2)
        return counts
