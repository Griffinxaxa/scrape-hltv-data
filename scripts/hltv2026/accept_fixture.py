"""Validate scraped grains for fixture match 2400286 against the field guide."""

from __future__ import annotations

import argparse
import csv
import json
import os
from typing import Any, Dict, List, Optional


FIXTURE_ID = 2400286

EXPECTED_SERIES_BOTH = [
    # team, nick, player_id, kills, deaths, eco_k, eco_d, swing, adr, eco_adr, kast, eco_kast, rating
    ("GamerLegion", "REZ", 9278, 48, 45, 47, 47, 1.27, 78.1, 77.1, 75.0, 74.0, 1.18),
    ("GamerLegion", "FL4MUS", 20994, 54, 49, 55, 51, -0.47, 87.5, 87.2, 73.6, 71.2, 1.14),
    ("GamerLegion", "Tauson", 20301, 49, 47, 49, 48, -0.04, 65.7, 65.3, 69.4, 65.7, 1.02),
    ("GamerLegion", "shalfey", 17011, 40, 43, 40, 45, -1.10, 68.1, 67.2, 77.8, 74.0, 0.97),
    ("GamerLegion", "hypex", 23766, 44, 39, 44, 42, -1.17, 59.8, 60.2, 75.0, 76.5, 0.95),
    ("Luminosity", "yxngstxr", 22047, 53, 47, 56, 48, 0.92, 72.2, 73.8, 75.0, 76.8, 1.20),
    ("Luminosity", "Rainwaker", 17145, 44, 46, 46, 48, 1.40, 81.2, 82.5, 69.4, 64.4, 1.11),
    ("Luminosity", "afro", 19926, 45, 38, 43, 41, 3.41, 57.6, 56.2, 69.4, 62.1, 1.10),
    ("Luminosity", "lux", 20563, 45, 52, 44, 54, -2.92, 72.0, 70.8, 68.1, 67.6, 0.85),
    ("Luminosity", "AZUWU", 22106, 36, 54, 37, 55, -1.65, 59.5, 60.8, 69.4, 71.5, 0.84),
]


def read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def as_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def as_float(v: Optional[str]) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def close(a: Optional[float], b: Optional[float], tol: float = 0.051) -> bool:
    if a is None or b is None:
        return a == b
    return abs(a - b) <= tol


def validate(output_dir: str) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})
        mark = "PASS" if ok else "FAIL"
        print(f"[{mark}] {name}" + (f" — {detail}" if detail else ""))

    series_rows = [r for r in read_csv(os.path.join(output_dir, "series.csv"))
                   if as_int(r.get("hltv_match_id")) == FIXTURE_ID]
    maps = [r for r in read_csv(os.path.join(output_dir, "maps.csv"))
            if as_int(r.get("hltv_match_id")) == FIXTURE_ID]
    veto = [r for r in read_csv(os.path.join(output_dir, "veto_steps.csv"))
            if as_int(r.get("hltv_match_id")) == FIXTURE_ID]
    lineups = [r for r in read_csv(os.path.join(output_dir, "lineups.csv"))
               if as_int(r.get("hltv_match_id")) == FIXTURE_ID]
    stats = [r for r in read_csv(os.path.join(output_dir, "player_stats.csv"))
             if as_int(r.get("hltv_match_id")) == FIXTURE_ID]

    check("series_row_present", len(series_rows) == 1, f"count={len(series_rows)}")
    series = series_rows[0] if series_rows else {}

    check("event_id_9258", as_int(series.get("event_id")) == 9258, f"got={series.get('event_id')}")
    check("team1_id_9928", as_int(series.get("team1_id")) == 9928, f"got={series.get('team1_id')}")
    check("team2_id_6290", as_int(series.get("team2_id")) == 6290, f"got={series.get('team2_id')}")
    check("winner_6290", as_int(series.get("winner_team_id")) == 6290, f"got={series.get('winner_team_id')}")
    check("score_1_2", as_int(series.get("team1_score")) == 1 and as_int(series.get("team2_score")) == 2,
          f"{series.get('team1_score')}-{series.get('team2_score')}")
    check("best_of_3", as_int(series.get("best_of")) == 3, f"got={series.get('best_of')}")
    check("environment_LAN", (series.get("environment") or "").upper() == "LAN",
          f"got={series.get('environment')}")
    check("scheduled_start", series.get("scheduled_start_utc") == "2026-11-17T17:30:00Z",
          f"got={series.get('scheduled_start_utc')}")

    played = [m for m in maps if m.get("played_status") == "played"]
    check("played_maps_3", len(played) == 3, f"count={len(played)}")
    check("veto_steps_7", len(veto) == 7, f"count={len(veto)}")

    player_ids = {as_int(r.get("player_id")) for r in lineups}
    check("distinct_lineup_players_10", len(player_ids) == 10, f"count={len(player_ids)}")
    check("player_stat_rows_120", len(stats) == 120, f"count={len(stats)}")

    mirage = next((m for m in maps if (m.get("map_name") or "").lower() == "mirage"), None)
    if mirage:
        check("mirage_14_16", as_int(mirage.get("team1_rounds")) == 14 and as_int(mirage.get("team2_rounds")) == 16,
              f"{mirage.get('team1_rounds')}-{mirage.get('team2_rounds')}")
        check("mirage_ot_2_4", as_int(mirage.get("ot_team1_rounds")) == 2 and as_int(mirage.get("ot_team2_rounds")) == 4,
              f"{mirage.get('ot_team1_rounds')}-{mirage.get('ot_team2_rounds')}")
        check("mirage_ot_sides_null",
              (mirage.get("ot_team1_side") in (None, "")) and (mirage.get("ot_team2_side") in (None, "")),
              f"{mirage.get('ot_team1_side')}/{mirage.get('ot_team2_side')}")
        check("mirage_map_stats_id", as_int(mirage.get("map_stats_id")) == 244584,
              f"got={mirage.get('map_stats_id')}")
    else:
        check("mirage_present", False, "missing")

    # Series/both player table side-by-side
    series_both = [r for r in stats if r.get("map_stats_id") in (None, "") and r.get("side") == "both"]
    check("series_both_10", len(series_both) == 10, f"count={len(series_both)}")

    by_player = {as_int(r.get("player_id")): r for r in series_both}
    player_mismatches = []
    for _team, nick, pid, k, d, ek, ed, swing, adr, eadr, kast, ekast, rating in EXPECTED_SERIES_BOTH:
        row = by_player.get(pid)
        if not row:
            player_mismatches.append(f"missing {nick} ({pid})")
            continue
        fields = [
            ("kills", as_int(row.get("kills")), k),
            ("deaths", as_int(row.get("deaths")), d),
            ("eco_kills", as_int(row.get("eco_kills")), ek),
            ("eco_deaths", as_int(row.get("eco_deaths")), ed),
            ("swing_pct", as_float(row.get("swing_pct")), swing),
            ("adr", as_float(row.get("adr")), adr),
            ("eco_adr", as_float(row.get("eco_adr")), eadr),
            ("kast_pct", as_float(row.get("kast_pct")), kast),
            ("eco_kast_pct", as_float(row.get("eco_kast_pct")), ekast),
            ("rating", as_float(row.get("rating")), rating),
        ]
        for fname, got, exp in fields:
            if isinstance(exp, float):
                if not close(got, exp):
                    player_mismatches.append(f"{nick}.{fname}: got={got} expected={exp}")
            else:
                if got != exp:
                    player_mismatches.append(f"{nick}.{fname}: got={got} expected={exp}")

    check("series_both_player_values", len(player_mismatches) == 0,
          "; ".join(player_mismatches[:8]) + ("..." if len(player_mismatches) > 8 else ""))

    passed = sum(1 for c in checks if c["ok"])
    failed = [c for c in checks if not c["ok"]]
    report = {
        "fixture_id": FIXTURE_ID,
        "passed": passed,
        "failed": len(failed),
        "total": len(checks),
        "checks": checks,
        "failed_names": [c["name"] for c in failed],
    }

    md_path = os.path.join(output_dir, "acceptance_2400286.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Acceptance: match {FIXTURE_ID}\n\n")
        f.write(f"**Result:** {passed}/{len(checks)} checks passed\n\n")
        f.write("| Check | Status | Detail |\n|---|---|---|\n")
        for c in checks:
            status = "PASS" if c["ok"] else "FAIL"
            detail = (c["detail"] or "").replace("|", "\\|")
            f.write(f"| {c['name']} | {status} | {detail} |\n")
        if failed:
            f.write("\n## Failures\n\n")
            for c in failed:
                f.write(f"- `{c['name']}`: {c['detail']}\n")
        f.write("\n## Series/both player sample\n\n")
        f.write("| player_id | nick | K-D | Swing | ADR | KAST | Rating |\n|---|---|---|---|---|---|---|\n")
        for r in sorted(series_both, key=lambda x: (x.get("team_id") or "", x.get("nickname") or "")):
            f.write(
                f"| {r.get('player_id')} | {r.get('nickname')} | "
                f"{r.get('kills')}-{r.get('deaths')} | {r.get('swing_pct')} | "
                f"{r.get('adr')} | {r.get('kast_pct')} | {r.get('rating')} |\n"
            )

    json_path = os.path.join(output_dir, "acceptance_2400286.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"\nWrote {md_path}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", "-o", default="2026-data/scrape_test")
    args = parser.parse_args()
    report = validate(args.output_dir)
    if report["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
