"""Match-result page parsers aligned with HLTV_match_result_field_guide.md."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://www.hltv.org"


def absolute_url(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if not href.startswith("/"):
        href = "/" + href
    return f"{BASE_URL}{href}"


def parse_id_from_href(href: Optional[str], kind: str) -> Optional[int]:
    if not href:
        return None
    match = re.search(rf"/{kind}/(\d+)(?:/|$|\?)", href)
    if match:
        return int(match.group(1))
    return None


def text(el: Optional[Tag]) -> Optional[str]:
    if el is None:
        return None
    value = el.get_text(" ", strip=True)
    return value or None


def safe_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    cleaned = value.strip().replace(",", "")
    if not re.fullmatch(r"[+-]?\d+", cleaned):
        return None
    return int(cleaned)


def safe_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.strip().replace("%", "").replace("−", "-").replace(",", "")
    if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", cleaned):
        return None
    return float(cleaned)


def split_kd(value: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not value:
        return None, None
    match = re.fullmatch(r"(\d+)\s*-\s*(\d+)", value.strip())
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def parse_best_of_and_environment(raw: Optional[str]) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    if not raw:
        return None, None, None
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    best_of = None
    environment = None
    stage_raw = None
    if lines:
        m = re.search(r"Best\s+of\s+(\d+)(?:\s*\(([^)]+)\))?", lines[0], re.I)
        if m:
            best_of = int(m.group(1))
            environment = m.group(2).strip() if m.group(2) else None
    if len(lines) > 1:
        stage_raw = lines[1].lstrip("*").strip()
        if len(lines) > 2:
            stage_raw = " | ".join([stage_raw] + [ln.lstrip("*").strip() for ln in lines[2:]])
    return best_of, environment, stage_raw


def unix_ms_to_iso(unix_ms: Optional[str]) -> Optional[str]:
    if not unix_ms:
        return None
    try:
        seconds = int(unix_ms) / 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OSError):
        return None


def extract_rank_number(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    match = re.search(r"#\s*(\d+)", raw)
    if match:
        return int(match.group(1))
    digits = re.search(r"(\d+)", raw)
    return int(digits.group(1)) if digits else None


def ranking_date_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    # /ranking/teams/2026/11/16/... or /vrs/.../2026/11/17/...
    match = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", url)
    if match:
        y, m, d = match.groups()
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    return None


def parse_series(soup: BeautifulSoup, capture_time_utc: str) -> Dict[str, Any]:
    coverage_ok = True
    errors: List[str] = []

    canonical = soup.select_one('link[rel="canonical"]')
    match_url = absolute_url(canonical.get("href") if canonical else None)
    if not match_url:
        og = soup.select_one('meta[property="og:url"]')
        match_url = absolute_url(og.get("content") if og else None)
    match_id = parse_id_from_href(match_url, "matches")
    if match_id is None:
        coverage_ok = False
        errors.append("missing_match_id")

    team1_a = soup.select_one(".teamsBox .team1-gradient > a")
    team2_a = soup.select_one(".teamsBox .team2-gradient > a")
    team1_name = text(team1_a.select_one(".teamName")) if team1_a else None
    team2_name = text(team2_a.select_one(".teamName")) if team2_a else None
    team1_id = parse_id_from_href(team1_a.get("href") if team1_a else None, "team")
    team2_id = parse_id_from_href(team2_a.get("href") if team2_a else None, "team")

    team1_flag = soup.select_one(".teamsBox .team > img.team1")
    team2_flag = soup.select_one(".teamsBox .team > img.team2")
    team1_region = (team1_flag.get("title") or team1_flag.get("alt")) if team1_flag else None
    team2_region = (team2_flag.get("title") or team2_flag.get("alt")) if team2_flag else None

    def score_from_gradient(selector: str) -> Optional[int]:
        node = soup.select_one(selector)
        if not node:
            return None
        score_el = node.select_one(":scope > .won, :scope > .lost")
        return safe_int(text(score_el))

    team1_score = score_from_gradient(".teamsBox .team1-gradient")
    team2_score = score_from_gradient(".teamsBox .team2-gradient")

    winner_team_id = None
    if team1_score is not None and team2_score is not None:
        if team1_score > team2_score:
            winner_team_id = team1_id
        elif team2_score > team1_score:
            winner_team_id = team2_id

    time_el = soup.select_one(".teamsBox .timeAndEvent .time")
    scheduled_start_utc = unix_ms_to_iso(time_el.get("data-unix") if time_el else None)

    event_a = soup.select_one(".teamsBox .timeAndEvent .event a")
    event_name = text(event_a) or (event_a.get("title") if event_a else None)
    event_url = absolute_url(event_a.get("href") if event_a else None)
    event_id = parse_id_from_href(event_url, "events")

    status = text(soup.select_one(".teamsBox .countdown"))

    # Format block: first nonempty .preformatted-text under .maps .veto-box that looks like Best of
    best_of = None
    environment = None
    stage_raw = None
    format_notes = None
    for box in soup.select(".maps .veto-box .preformatted-text"):
        raw = box.get_text("\n", strip=False)
        if re.search(r"Best\s+of\s+\d+", raw, re.I):
            best_of, environment, stage_raw = parse_best_of_and_environment(raw)
            format_notes = raw.strip()
            break

    detailed_a = soup.select_one("#match-stats .stats-detailed-stats a")
    if not detailed_a:
        for link in soup.select('a[href*="/stats/matches/"]'):
            if "detailed stats" in (text(link) or "").lower():
                detailed_a = link
                break
    series_stats_url = absolute_url(detailed_a.get("href") if detailed_a else None)
    series_stats_id = None
    if series_stats_url:
        m = re.search(r"/stats/matches/(\d+)", series_stats_url)
        if m:
            series_stats_id = int(m.group(1))

    notes_el = soup.select_one(".padding.preformatted-text")
    notes = text(notes_el)
    forfeit = False
    walkover = False
    incomplete = False
    if notes:
        low = notes.lower()
        forfeit = "forfeit" in low
        walkover = "walkover" in low
        incomplete = "incomplete" in low or "awarded" in low

    demo_el = soup.select_one('.streams [data-demo-link]')
    demo_url = absolute_url(demo_el.get("data-demo-link") if demo_el else None)

    if team1_id is None or team2_id is None:
        coverage_ok = False
        errors.append("missing_team_ids")

    return {
        "hltv_match_id": match_id,
        "match_url": match_url,
        "series_stats_id": series_stats_id,
        "series_stats_url": series_stats_url,
        "event_id": event_id,
        "event_name": event_name,
        "event_url": event_url,
        "team1_id": team1_id,
        "team1_name": team1_name,
        "team1_region": team1_region,
        "team2_id": team2_id,
        "team2_name": team2_name,
        "team2_region": team2_region,
        "best_of": best_of,
        "environment": environment,
        "stage_raw": stage_raw,
        "format_notes": format_notes,
        "scheduled_start_utc": scheduled_start_utc,
        "completed_at_utc": None,
        "status": status,
        "team1_score": team1_score,
        "team2_score": team2_score,
        "winner_team_id": winner_team_id,
        "forfeit": forfeit,
        "walkover": walkover,
        "incomplete": incomplete,
        "match_notes": notes,
        "demo_url": demo_url,
        "capture_time_utc": capture_time_utc,
        "source_url": match_url,
        "_coverage_ok": coverage_ok,
        "_errors": errors,
    }


def _names_match(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()


def _parse_half_scores_oriented(center: Optional[Tag], remap_left_is_team1: bool = True) -> Dict[str, Any]:
    """Parse half/OT scores. Keys use team1/team2 assuming left==team1; caller remaps."""
    out = {
        "half1_team1_rounds": None,
        "half1_team2_rounds": None,
        "half1_team1_side": None,
        "half1_team2_side": None,
        "half2_team1_rounds": None,
        "half2_team2_rounds": None,
        "half2_team1_side": None,
        "half2_team2_side": None,
        "ot_team1_rounds": None,
        "ot_team2_rounds": None,
        "ot_team1_side": None,
        "ot_team2_side": None,
        "_left_right_raw": True,
    }
    if center is None:
        return out

    side_spans: List[Tuple[Optional[int], str]] = []
    for span in center.find_all("span"):
        classes = set(span.get("class", []))
        if "ct" in classes or "t" in classes:
            side = "CT" if "ct" in classes else "T"
            side_spans.append((safe_int(text(span)), side))

    # First four side spans: half1 left/right, half2 left/right
    if len(side_spans) >= 2:
        out["half1_team1_rounds"], out["half1_team1_side"] = side_spans[0]
        out["half1_team2_rounds"], out["half1_team2_side"] = side_spans[1]
    if len(side_spans) >= 4:
        out["half2_team1_rounds"], out["half2_team1_side"] = side_spans[2]
        out["half2_team2_rounds"], out["half2_team2_side"] = side_spans[3]

    full = center.get_text(" ", strip=True)
    # Regulation is usually "( 5 : 7 ; 7 : 5 )" (semicolon, so no simple a:b paren match).
    # OT is a later unmarked pair "( 2 : 4 )" with no side classes.
    paren_pairs = re.findall(r"\(\s*(\d+)\s*[:\-]\s*(\d+)\s*\)", full)
    if paren_pairs:
        # When regulation uses a semicolon, the only simple a:b paren pair is OT.
        # If multiple simple pairs appear, the last one is OT.
        out["ot_team1_rounds"] = int(paren_pairs[-1][0])
        out["ot_team2_rounds"] = int(paren_pairs[-1][1])
        out["ot_team1_side"] = None
        out["ot_team2_side"] = None

    # Fallback: unmarked numeric spans after the four side-class spans
    if out["ot_team1_rounds"] is None:
        unmarked = []
        for span in center.find_all("span"):
            classes = set(span.get("class") or [])
            if classes.intersection({"ct", "t"}):
                continue
            val = safe_int(text(span))
            if val is not None:
                unmarked.append(val)
        if len(unmarked) >= 2:
            out["ot_team1_rounds"] = unmarked[0]
            out["ot_team2_rounds"] = unmarked[1]
            out["ot_team1_side"] = None
            out["ot_team2_side"] = None

    return out


def remap_half_scores_for_team_order(half: Dict[str, Any], left_is_team1: bool) -> Dict[str, Any]:
    if left_is_team1:
        return {k: v for k, v in half.items() if not k.startswith("_")}
    swap_pairs = [
        ("half1_team1_rounds", "half1_team2_rounds"),
        ("half1_team1_side", "half1_team2_side"),
        ("half2_team1_rounds", "half2_team2_rounds"),
        ("half2_team1_side", "half2_team2_side"),
        ("ot_team1_rounds", "ot_team2_rounds"),
        ("ot_team1_side", "ot_team2_side"),
    ]
    remapped = {k: v for k, v in half.items() if not k.startswith("_")}
    for a, b in swap_pairs:
        remapped[a], remapped[b] = half.get(b), half.get(a)
    return remapped


def parse_maps(soup: BeautifulSoup, match_id: Optional[int], team1_id: Optional[int], team2_id: Optional[int],
               team1_name: Optional[str], team2_name: Optional[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    holders = soup.select(".maps .mapholder")
    for ordinal, card in enumerate(holders, start=1):
        map_name = text(card.select_one(".mapname"))
        played = card.select_one(".results.played") is not None

        left_name = text(card.select_one(".results-left .results-teamname"))
        right_name = text(card.select_one(".results-right .results-teamname"))
        left_score = safe_int(text(card.select_one(".results-left .results-team-score")))
        right_score = safe_int(text(card.select_one(".results-right .results-team-score")))

        left_block = card.select_one(".results-left")
        right_block = card.select_one(".results-right")
        left_classes = set(left_block.get("class", [])) if left_block else set()
        right_classes = set(right_block.get("class", [])) if right_block else set()

        left_pick = "pick" in left_classes
        right_pick = "pick" in right_classes
        if left_pick:
            pick_side = "team1" if _names_match(left_name, team1_name) else "team2"
        elif right_pick:
            pick_side = "team1" if _names_match(right_name, team1_name) else "team2"
        elif played:
            pick_side = "decider"
        else:
            pick_side = None

        left_is_team1 = _names_match(left_name, team1_name)
        if left_is_team1:
            team1_rounds, team2_rounds = left_score, right_score
        elif _names_match(left_name, team2_name):
            team1_rounds, team2_rounds = right_score, left_score
            left_is_team1 = False
        else:
            team1_rounds, team2_rounds = left_score, right_score
            left_is_team1 = True

        winner_team_id = None
        if team1_rounds is not None and team2_rounds is not None:
            if team1_rounds > team2_rounds:
                winner_team_id = team1_id
            elif team2_rounds > team1_rounds:
                winner_team_id = team2_id

        half_raw = _parse_half_scores_oriented(card.select_one(".results-center-half-score"))
        half = remap_half_scores_for_team_order(half_raw, left_is_team1=left_is_team1)

        stats_a = card.select_one("a.results-stats")
        map_stats_url = absolute_url(stats_a.get("href") if stats_a else None)
        map_stats_id = None
        if map_stats_url:
            m = re.search(r"/mapstatsid/(\d+)", map_stats_url)
            if m:
                map_stats_id = int(m.group(1))

        if not played and left_score is None and right_score is None:
            status = "unplayed"
        elif played:
            status = "played"
        else:
            status = "unknown"

        rows.append({
            "hltv_match_id": match_id,
            "map_ordinal": ordinal,
            "map_name": map_name,
            "map_stats_id": map_stats_id,
            "map_stats_url": map_stats_url,
            "pick_side": pick_side,
            "team1_rounds": team1_rounds,
            "team2_rounds": team2_rounds,
            "winner_team_id": winner_team_id,
            **half,
            "played_status": status,
            "left_team_name": left_name,
            "right_team_name": right_name,
        })
    return rows


def parse_veto_steps(soup: BeautifulSoup, match_id: Optional[int],
                     team1_id: Optional[int], team2_id: Optional[int],
                     team1_name: Optional[str], team2_name: Optional[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    steps = soup.select(".maps .veto-box.padding > div")
    if not steps:
        steps = soup.select(".maps .veto-box.real-veto-only > .padding > div")
    if not steps:
        # Fallback: any real-veto-only children
        for box in soup.select(".maps .veto-box"):
            classes = set(box.get("class", []))
            if "real-veto-only" in classes or any("real-veto" in c for c in classes):
                pad = box.select_one(".padding") or box
                steps = [c for c in pad.find_all("div", recursive=False) if text(c)]
                if steps:
                    break

    for step in steps:
        raw = text(step)
        if not raw:
            continue
        # "1. Team removed Map" / "7. Map was left over"
        m = re.match(r"(\d+)\.\s*(.*)", raw)
        if not m:
            continue
        order = int(m.group(1))
        body = m.group(2).strip()
        actor_team_id = None
        action = None
        map_name = None

        left_over = re.search(r"(.+?)\s+was left over", body, re.I)
        removed = re.search(r"^(.+?)\s+removed\s+(.+)$", body, re.I)
        picked = re.search(r"^(.+?)\s+picked\s+(.+)$", body, re.I)
        if left_over:
            action = "left_over"
            map_name = left_over.group(1).strip()
        elif removed:
            action = "removed"
            actor_name = removed.group(1).strip()
            map_name = removed.group(2).strip()
            actor_team_id = _team_id_for_name(actor_name, team1_name, team2_name, team1_id, team2_id)
        elif picked:
            action = "picked"
            actor_name = picked.group(1).strip()
            map_name = picked.group(2).strip()
            actor_team_id = _team_id_for_name(actor_name, team1_name, team2_name, team1_id, team2_id)
        else:
            action = "unknown"

        rows.append({
            "hltv_match_id": match_id,
            "step_order": order,
            "actor_team_id": actor_team_id,
            "action": action,
            "map_name": map_name,
            "raw_text": raw,
        })
    return rows


def _team_id_for_name(name: str, t1: Optional[str], t2: Optional[str],
                      id1: Optional[int], id2: Optional[int]) -> Optional[int]:
    if t1 and name.lower() == t1.lower():
        return id1
    if t2 and name.lower() == t2.lower():
        return id2
    return None


def parse_lineups(soup: BeautifulSoup, match_id: Optional[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for block in soup.select("#lineups .lineup"):
        team_a = block.select_one('.box-headline a[href*="/team/"]')
        team_id = parse_id_from_href(team_a.get("href") if team_a else None, "team")
        team_name = text(team_a)

        hltv_a = block.select_one(".teamRanking a.hltv-ranking")
        vrs_a = block.select_one(".teamRanking a.vrs-ranking")
        hltv_rank = extract_rank_number(text(hltv_a))
        hltv_rank_url = absolute_url(hltv_a.get("href") if hltv_a else None)
        hltv_rank_date = ranking_date_from_url(hltv_rank_url)
        vrs_rank = extract_rank_number(text(vrs_a))
        vrs_rank_url = absolute_url(vrs_a.get("href") if vrs_a else None)
        vrs_rank_date = ranking_date_from_url(vrs_rank_url)

        # Players via data-player-id to avoid double-counting portrait + nick rows
        seen_players = set()
        for el in block.select(".players .flagAlign[data-player-id]"):
            player_id = safe_int(el.get("data-player-id"))
            if player_id is None or player_id in seen_players:
                continue
            seen_players.add(player_id)
            nick = text(el.select_one(".text-ellipsis"))
            flag = el.select_one("img.flag")
            nationality = (flag.get("title") or flag.get("alt")) if flag else None
            team_ordinal = safe_int(el.get("data-team-ordinal"))

            # Role badges: find player-image cell for this player
            role_badges: List[str] = []
            player_link = el.find_parent("a") or el.select_one("a")
            # Search role pills under matching player anchors in this block
            for pill in block.select(f'td.player-image a[href*="/player/{player_id}"] .role-pill[title]'):
                title = pill.get("title")
                if title and title not in role_badges:
                    role_badges.append(title)

            full_name = None
            img = block.select_one(f'td.player-image a[href*="/player/{player_id}"] img.player-photo')
            if img:
                full_name = img.get("title") or img.get("alt")

            rows.append({
                "hltv_match_id": match_id,
                "team_id": team_id,
                "team_name": team_name,
                "team_ordinal": team_ordinal,
                "player_id": player_id,
                "nickname": nick,
                "full_name": full_name,
                "nationality": nationality,
                "role_badges": "|".join(role_badges) if role_badges else None,
                "hltv_rank": hltv_rank,
                "hltv_rank_url": hltv_rank_url,
                "hltv_rank_date": hltv_rank_date,
                "vrs_rank": vrs_rank,
                "vrs_rank_url": vrs_rank_url,
                "vrs_rank_date": vrs_rank_date,
            })
    return rows


def parse_player_stats(soup: BeautifulSoup, match_id: Optional[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    root = soup.select_one(".match-page #match-stats") or soup.select_one("#match-stats")
    if root is None:
        return rows

    for panel in root.select(".stats-content"):
        panel_id = panel.get("id") or ""
        scope = panel_id.replace("-content", "")
        map_stats_id = None if scope == "all" else safe_int(scope)

        for table in panel.select("table"):
            classes = set(table.get("class", []))
            if "totalstats" in classes:
                side = "both"
            elif "ctstats" in classes:
                side = "CT"
            elif "tstats" in classes:
                side = "T"
            else:
                continue

            header = table.select_one("tr.header-row")
            if not header:
                continue
            team_a = header.select_one("a.teamName")
            team_id = parse_id_from_href(team_a.get("href") if team_a else None, "team")
            rating_version = text(header.select_one("td.rating .ratingDesc"))

            for row in table.select("tr"):
                if "header-row" in (row.get("class") or []):
                    continue
                player_a = row.select_one("td.players a")
                if not player_a:
                    continue
                player_id = parse_id_from_href(player_a.get("href"), "player")
                nick = text(row.select_one(".player-nick")) or text(player_a)

                kills, deaths = split_kd(text(row.select_one("td.kd.traditional-data")))
                eco_kills, eco_deaths = split_kd(text(row.select_one("td.kd.eco-adjusted-data")))

                swing = safe_float(text(row.select_one("td.roundSwing")))
                adr = safe_float(text(row.select_one("td.adr.traditional-data")))
                eco_adr = safe_float(text(row.select_one("td.adr.eco-adjusted-data")))
                kast = safe_float(text(row.select_one("td.kast.traditional-data")))
                eco_kast = safe_float(text(row.select_one("td.kast.eco-adjusted-data")))
                rating = safe_float(text(row.select_one("td.rating")))

                rows.append({
                    "hltv_match_id": match_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "nickname": nick,
                    "map_stats_id": map_stats_id,
                    "side": side,
                    "kills": kills,
                    "deaths": deaths,
                    "eco_kills": eco_kills,
                    "eco_deaths": eco_deaths,
                    "swing_pct": swing,
                    "adr": adr,
                    "eco_adr": eco_adr,
                    "kast_pct": kast,
                    "eco_kast_pct": eco_kast,
                    "rating": rating,
                    "rating_version": rating_version,
                })
    return rows


def parse_h2h_maps(soup: BeautifulSoup, match_id: Optional[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for tr in soup.select(".head-to-head-listing tr.row"):
        date_a = tr.select_one("td.date a")
        prior_match_id = parse_id_from_href(date_a.get("href") if date_a else None, "matches")
        prior_url = absolute_url(date_a.get("href") if date_a else None)
        unix_el = tr.select_one("td.date [data-unix]")
        prior_time_utc = unix_ms_to_iso(unix_el.get("data-unix") if unix_el else None)

        team1_a = tr.select_one("td.team1 a")
        team2_a = tr.select_one("td.team2 a")
        event_a = tr.select_one("td.event a")
        map_el = tr.select_one("td.map .dynamic-map-name-full")
        result_spans = tr.select("td.result > span")
        score1 = safe_int(text(result_spans[0])) if len(result_spans) > 0 else None
        score2 = safe_int(text(result_spans[1])) if len(result_spans) > 1 else None

        winner_team_id = None
        t1_id = parse_id_from_href(team1_a.get("href") if team1_a else None, "team")
        t2_id = parse_id_from_href(team2_a.get("href") if team2_a else None, "team")
        if "winner" in (tr.select_one("td.team1").get("class") or [] if tr.select_one("td.team1") else []):
            winner_team_id = t1_id
        elif "winner" in (tr.select_one("td.team2").get("class") or [] if tr.select_one("td.team2") else []):
            winner_team_id = t2_id

        rows.append({
            "source_hltv_match_id": match_id,
            "prior_match_id": prior_match_id,
            "prior_match_url": prior_url,
            "prior_time_utc": prior_time_utc,
            "team1_id": t1_id,
            "team1_name": text(team1_a),
            "team1_players": tr.select_one("td.team1").get("title") if tr.select_one("td.team1") else None,
            "team2_id": t2_id,
            "team2_name": text(team2_a),
            "team2_players": tr.select_one("td.team2").get("title") if tr.select_one("td.team2") else None,
            "event_id": parse_id_from_href(event_a.get("href") if event_a else None, "events"),
            "event_name": text(event_a),
            "map_name": text(map_el),
            "team1_score": score1,
            "team2_score": score2,
            "winner_team_id": winner_team_id,
        })
    return rows


def parse_past_match_links(soup: BeautifulSoup, match_id: Optional[int]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for view_name, selector in (
        ("core", ".past-matches-grid[data-past-matches-core]"),
        ("team", ".past-matches-grid[data-past-matches-team]"),
    ):
        grid = soup.select_one(selector)
        if not grid:
            continue
        for box in grid.select(".past-matches-box"):
            subject_a = box.select_one(".past-matches-headline a")
            subject_team_id = parse_id_from_href(subject_a.get("href") if subject_a else None, "team")
            subject_team_name = text(subject_a)
            for tr in box.select(".past-matches-table tr"):
                opp_a = tr.select_one(".past-matches-team .past-matches-teamname a")
                score_a = tr.select_one(".past-matches-score a")
                map_a = tr.select_one(".past-matches-map a")
                prior_url = absolute_url(score_a.get("href") if score_a else None)
                prior_match_id = parse_id_from_href(prior_url, "matches")
                score_text = text(score_a)
                outcome = None
                if score_a:
                    classes = set(score_a.get("class") or [])
                    if "won" in classes:
                        outcome = "won"
                    elif "lost" in classes:
                        outcome = "lost"
                rows.append({
                    "source_hltv_match_id": match_id,
                    "view": view_name,
                    "subject_team_id": subject_team_id,
                    "subject_team_name": subject_team_name,
                    "opponent_team_id": parse_id_from_href(opp_a.get("href") if opp_a else None, "team"),
                    "opponent_team_name": text(opp_a),
                    "recency_text": text(tr.select_one(".past-matches-time-ago")),
                    "format_or_map": text(map_a),
                    "score_text": score_text,
                    "outcome_for_subject": outcome,
                    "prior_match_id": prior_match_id,
                    "prior_match_url": prior_url,
                })
    return rows


def parse_vrs_forecast(soup: BeautifulSoup, match_id: Optional[int]) -> List[Dict[str, Any]]:
    """Capture VRS before (pre-match feature) and result (post-match) columns."""
    container = soup.select_one(".vrs-forecast-container")
    if not container:
        return []

    names = [text(el) for el in container.select(".vrs-forecast-team-row .vrs-forecast-team-name")]
    before_wraps = container.select(".vrs-forecast-left-numbers .vrs-forecast-numbers-wrapper")
    result_wraps = container.select(".vrs-forecast-middle .vrs-forecast-numbers-wrapper")

    rows: List[Dict[str, Any]] = []
    for idx, name in enumerate(names):
        before = before_wraps[idx] if idx < len(before_wraps) else None
        result = result_wraps[idx] if idx < len(result_wraps) else None

        before_points = text(before.select_one(".vrs-forecast-points")) if before else None
        before_rank = text(before.select_one(".vrs-forecast-ranking")) if before else None
        result_points = text(result.select_one(".vrs-forecast-points")) if result else None
        result_rank = text(result.select_one(".vrs-forecast-ranking")) if result else None
        result_move = text(result.select_one(".vrs-forecast-small-points")) if result else None

        def strip_pts(v: Optional[str]) -> Optional[float]:
            if not v:
                return None
            return safe_float(v.replace("pt", "").strip())

        rows.append({
            "hltv_match_id": match_id,
            "team_name": name,
            "before_points": strip_pts(before_points),
            "before_rank": extract_rank_number(before_rank),
            "result_points_delta": strip_pts(result_points),
            "result_rank": extract_rank_number(result_rank),
            "result_rank_delta": safe_int(result_move.replace("+", "") if result_move and result_move.startswith("+") else result_move) if result_move else None,
        })
    return rows


def parse_match_page(soup: BeautifulSoup, capture_time_utc: Optional[str] = None) -> Dict[str, Any]:
    capture_time_utc = capture_time_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    coverage: Dict[str, Any] = {"capture_time_utc": capture_time_utc, "sections": {}}

    series = parse_series(soup, capture_time_utc)
    match_id = series.get("hltv_match_id")
    coverage["hltv_match_id"] = match_id
    coverage["sections"]["series"] = {
        "ok": series.pop("_coverage_ok", True),
        "errors": series.pop("_errors", []),
    }

    try:
        maps = parse_maps(
            soup, match_id, series.get("team1_id"), series.get("team2_id"),
            series.get("team1_name"), series.get("team2_name"),
        )
        coverage["sections"]["maps"] = {"ok": True, "count": len(maps)}
    except Exception as exc:
        maps = []
        coverage["sections"]["maps"] = {"ok": False, "error": str(exc)}

    try:
        veto = parse_veto_steps(
            soup, match_id, series.get("team1_id"), series.get("team2_id"),
            series.get("team1_name"), series.get("team2_name"),
        )
        coverage["sections"]["veto_steps"] = {"ok": True, "count": len(veto)}
    except Exception as exc:
        veto = []
        coverage["sections"]["veto_steps"] = {"ok": False, "error": str(exc)}

    try:
        lineups = parse_lineups(soup, match_id)
        coverage["sections"]["lineups"] = {"ok": True, "count": len(lineups)}
    except Exception as exc:
        lineups = []
        coverage["sections"]["lineups"] = {"ok": False, "error": str(exc)}

    try:
        player_stats = parse_player_stats(soup, match_id)
        coverage["sections"]["player_stats"] = {"ok": True, "count": len(player_stats)}
    except Exception as exc:
        player_stats = []
        coverage["sections"]["player_stats"] = {"ok": False, "error": str(exc)}

    try:
        h2h = parse_h2h_maps(soup, match_id)
        coverage["sections"]["h2h_maps"] = {"ok": True, "count": len(h2h)}
    except Exception as exc:
        h2h = []
        coverage["sections"]["h2h_maps"] = {"ok": False, "error": str(exc)}

    try:
        past = parse_past_match_links(soup, match_id)
        coverage["sections"]["past_match_links"] = {"ok": True, "count": len(past)}
    except Exception as exc:
        past = []
        coverage["sections"]["past_match_links"] = {"ok": False, "error": str(exc)}

    try:
        vrs = parse_vrs_forecast(soup, match_id)
        coverage["sections"]["vrs_forecast"] = {"ok": True, "count": len(vrs)}
    except Exception as exc:
        vrs = []
        coverage["sections"]["vrs_forecast"] = {"ok": False, "error": str(exc)}

    return {
        "series": series,
        "maps": maps,
        "veto_steps": veto,
        "lineups": lineups,
        "player_stats": player_stats,
        "h2h_maps": h2h,
        "past_match_links": past,
        "vrs_forecast": vrs,
        "coverage": coverage,
    }
