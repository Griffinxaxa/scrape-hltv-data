# Reading an HLTV match result page

**Worked fixture:** GamerLegion vs. Luminosity, Stake Ranked Episode 6, November 17, 2026. Luminosity won **2–1** in a best-of-three LAN upper bracket quarter-final. This guide describes the supplied browser-saved HTML. Selectors, row counts and examples refer to this capture. Linked pages have their own schemas and were not included in the inspection.

**How to use the selectors.** Start at `.match-page`. Each table below names a narrower parent when its selectors are relative. “Text” means trimmed element text. Read attributes such as `href`, `title` and `data-unix` directly. Ignore CSS visibility when extracting stats. Mobile headers, spoiler toggles, site sidebars and team-history tabs can produce plausible duplicates if you search the entire document.

## 1. Match identity, schedule and series score

| Field | CSS selector | Extraction and fixture value |
|---|---|---|
| Result URL and match ID | Document head → `link[rel="canonical"]` | Read `href`. The integer after `/matches/` is **2400286**. Fallback is `meta[property="og:url"]` → `content`. |
| Teams in scoreboard order | `.teamsBox .team1-gradient > a`, `.teamsBox .team2-gradient > a` | Read `.teamName` inside each link and the integer after `/team/` in its href. Slot 1 is GamerLegion (9928), slot 2 is Luminosity (6290). Keep these slots for interpreting score pairs. |
| Team region as displayed | `.teamsBox .team > img.team1`, `.teamsBox .team > img.team2` | `title` or `alt`: Europe for both. This describes a team flag. The event venue and player nationalities have separate sources. |
| Series scores and winner | Within each gradient → `:scope > .won, :scope > .lost` | Integer text counts maps won. Left `div.lost` = 1, right `div.won` = 2. Winner team ID = 6290. Preserve missing scores as null for other page states. |
| Scheduled start | `.teamsBox .timeAndEvent .time` | `data-unix="1794936600000"` is milliseconds. Divide by 1,000 before Unix-seconds conversion. UTC is 17:30. The rendered clock says 12:30. |
| Display date | `.teamsBox .timeAndEvent .date` | “17th of November 2026”. Carries the same timestamp and a separate `data-time-format`. No completion timestamp here. |
| Event name and ID | `.teamsBox .timeAndEvent .event a` | Text or title = Stake Ranked Episode 6. Extract 9258 from `/events/9258/stake-ranked-episode-6`. |
| Match status | `.teamsBox .countdown` | “Match over”. Validate alongside final scoreboard. |
| Mobile copy | `.teamsBoxDropdown .score` | The two child spans repeat 1 and 2. Its team links and date also duplicate the main header. Use one header only. |

**Structured metadata trap.** The document’s `script[type="application/ld+json"]` contains a SportsEvent with `url`, `name`, `description`, `competitor[].name/url`, `startDate`, `eventStatus` and `location.name/url`. Its start date, 18:30 at UTC+01:00, agrees with the epoch. Its status says `EventScheduled` and its location says `Online Event`. For status use the visible result header, and for LAN use the format block below. Neither source establishes the city.

## 2. Best-of, LAN, stage and veto

Read `.maps .veto-box .preformatted-text` as multiline text **before** collapsing whitespace. The file contains two nonempty lines:

```text
Best of 3 (LAN)
* Upper bracket quarter-final
```

The first line gives `best_of=3` and `environment=LAN`. A case-insensitive expression such as `Best\s+of\s+(\d+)(?:\s*\(([^)]+)\))?` keeps the environment optional. Preserve the second line after stripping its leading asterisk as `stage_raw`. Here it supports `bracket=upper` and `round=quarter-final`. Keep any additional notes separately. A missing round, bracket or environment should remain unknown. Selecting the first `.veto-box` by index on this file would return the veto actions instead.

For the actual veto, iterate **`.maps .veto-box.real-veto-only > .padding > div`** in DOM order. Each child is one numbered plain-text action. Split off the step number first, then parse the verb. The final line has no acting team.

| Step | Team | Action | Map |
|---:|---|---|---|
| 1 | Luminosity | removed | Anubis |
| 2 | GamerLegion | removed | Inferno |
| 3 | Luminosity | picked | Mirage |
| 4 | GamerLegion | picked | Nuke |
| 5 | GamerLegion | removed | Ancient |
| 6 | Luminosity | removed | Dust2 |
| 7 | — | was left over | Cache |

Match team names against the two header teams and retain the original text on an unrecognized action. On this page the actions have no timestamps. A forecast made before the veto cannot use the observed picks.

## 3. Map scores, halves, overtime and stats links

Iterate `.maps .mapholder` in page order. Inside each card use the following selectors.

| Field | Selector inside card | Read |
|---|---|---|
| Map name | `.mapname` | Text. Map thumbnail `img.minimap` repeats the name in alt/title. |
| Played status | `.results.played` | A completed score block on all three cards. Require score evidence before counting a map as played. |
| Left/right team and rounds | `.results-left .results-teamname`, `.results-left .results-team-score`; same under `.results-right` | Two names and two integer round totals. Join to header slots by team. The right block is a span in this file. |
| Winner and map pick | `.results-left`, `.results-right` | Read class tokens `won`, `lost`, `pick`. A winning team need not be the picker. Neither side has `pick` on Cache. |
| Side and half scores | `.results-center-half-score > span` | Read spans in order. Each regulation pair is left rounds, then right rounds. The numeric spans carry `ct` or `t`. The second pair is the second half. |
| Overtime | `.results-center-half-score` | After the regulation parentheses, read any extra parenthesized pair separately. On Mirage the OT numbers have **no side classes**. Reading only `.ct, .t` drops six rounds. |
| Detailed map stats | `a.results-stats` | Read href; the ID follows `/stats/matches/mapstatsid/`. Join each card to the player-stat map tab using this ID. |

| Order | Map | Pick | Final GL–Luminosity | First half | Second half | OT | Map stats ID |
|---:|---|---|---|---|---|---|---:|
| 1 | Mirage | Luminosity | 14–16 | 5 CT / 7 T | 7 T / 5 CT | 2:4 | 244584 |
| 2 | Nuke | GamerLegion | 13–7 | 7 T / 5 CT | 6 CT / 2 T | — | 244590 |
| 3 | Cache | Decider | 9–13 | 7 T / 5 CT | 2 CT / 8 T | — | 244595 |

**Three ID namespaces.** The public result is match **2400286**. The series-level “Detailed stats” link is `#match-stats .stats-detailed-stats a` → `/stats/matches/133158/gamerlegion-vs-luminosity`. Each map has its own stats ID, as listed above. Store the actual href for each. For saved documents use the canonical origin to resolve relative site links. Browser-saved image paths can point to a local companion directory.

## 4. Lineups, player identities, roles and dated team rankings

Iterate `#lineups .lineup`, one block per team. The following paths are relative to that block.

| Field | Selector | Read |
|---|---|---|
| Owning team | `.box-headline a[href*="/team/"]` | Name and team ID from href. |
| HLTV rank | `.teamRanking a.hltv-ranking` | Text contains a label and `#N`. The dated ranking URL includes the team ID as the last path segment. |
| Valve Regional Standings rank | `.teamRanking a.vrs-ranking` | Extract `#N`. The ranking URL includes the date in the path and team ID in the `teamId` query parameter. |
| Player and team slot | `.players .flagAlign[data-player-id]` | One element per player; attributes `data-player-id` and `data-team-ordinal`. The latter is 1 or 2. Its parent anchor supplies the player URL. |
| Nickname and nationality | Within the same element → `.text-ellipsis` and `img.flag` | Read nickname text and flag title/alt. |
| Full display name | `td.player-image a[href*="/player/"] img.player-photo` | Title or alt includes name and quoted nickname. Match the parent link’s player ID to the nickname row. |
| Role badges | `td.player-image a .role-pill[title]` | Read all titles under a player anchor. This capture labels shalfey and lux “In-game leader”, hypex and afro “Main AWPer”. Preserve an empty list for players without a displayed badge. |

| Team | HLTV rank, Nov 16 | VRS rank, Nov 17 |
|---|---:|---:|
| GamerLegion | 27 | 40 |
| Luminosity | 36 | 46 |

Both sets of ranks are displayed together, but their reference dates differ. Keep each dated href alongside the value. The lineup table has a portrait row and a nickname row pointing to the same players. Counting every player anchor doubles roster size.

## 5. Player statistics for every team, map and side

Start at `#match-stats .stats-content`. The panel ID selects the map scope. `all-content` holds the series aggregate. `244584-content`, `244590-content` and `244595-content` hold Mirage, Nuke and Cache. Map labels are in `.stats-menu-link .dynamic-map-name-full`. Their `id` values are `all` or the numeric map stats ID. The short and full labels reuse IDs. An attribute selector such as `[id="244584-content"]` works without escaping a leading digit.

Within each panel, iterate the tables. Each table belongs to one team and one side filter. Read `tr.header-row a.teamName` for its team ID, then iterate `tr:not(.header-row)` for the five players. `table.totalstats` is both sides, `table.tstats` is T only and `table.ctstats` is CT only. Side tables have a `hidden` class and already contain their data. Join by player ID because row order can change between tables.

| Field | Selector inside a player row | Example, REZ, series/both | Meaning and conversion |
|---|---|---:|---|
| Player identity | `td.players a`; `.player-nick` | 9278, REZ | Parse player ID from href. Desktop `.statsPlayerName.gtSmartphone-only` contains full name; mobile copy contains nickname. Flag title gives country. |
| Kills and deaths | `td.kd.traditional-data` | 48-45 | Split into integer counts. The displayed pair also allows a derived K−D differential or K/D ratio. Guard against zero deaths. |
| Eco-adjusted kills and deaths | `td.kd.eco-adjusted-data` | 47-47 | Split into separate adjusted counts; preserve independently of raw K and D. |
| Round Swing | `td.roundSwing` | +1.27% | Signed percentage measuring average change in round-win chances attributed to the player. Preserve the sign. |
| Average damage per round | `td.adr.traditional-data` | 78.1 | Decimal damage per round. |
| Eco-adjusted ADR | `td.adr.eco-adjusted-data` | 77.1 | Decimal damage per round adjusted for economy. |
| KAST | `td.kast.traditional-data` | 75.0% | Percentage of rounds with a kill, assist, survival or traded death. |
| Eco-adjusted KAST | `td.kast.eco-adjusted-data` | 74.0% | Adjusted percentage. Decide once whether your database stores 74.0 or 0.740. |
| Rating | `td.rating` | 1.18 | Decimal rating. Read its version from the table header’s `td.rating .ratingDesc` (3.0). Styling classes can also indicate positive, neutral or negative. |

The original column names and definitions are in `tr.header-row td[title]`. There are separate eco-adjusted cells for K-D, ADR and KAST. Selecting just `.adr` silently merges two measurements. Swing and Rating each have one cell per row. Side/map aggregates overlap. For a series rating use the series table directly; averaging the map ratings can change the value.

**All ten players, series aggregate, both sides.** This is a reference output for checking the scraper. The identical column selectors work on each map and each side.

| Team | Player (ID) | K-D | eK-eD | Swing | ADR | eADR | KAST | eKAST | Rating 3.0 |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| GamerLegion | REZ (9278) | 48-45 | 47-47 | +1.27% | 78.1 | 77.1 | 75.0% | 74.0% | 1.18 |
| GamerLegion | FL4MUS (20994) | 54-49 | 55-51 | -0.47% | 87.5 | 87.2 | 73.6% | 71.2% | 1.14 |
| GamerLegion | Tauson (20301) | 49-47 | 49-48 | -0.04% | 65.7 | 65.3 | 69.4% | 65.7% | 1.02 |
| GamerLegion | shalfey (17011) | 40-43 | 40-45 | -1.10% | 68.1 | 67.2 | 77.8% | 74.0% | 0.97 |
| GamerLegion | hypex (23766) | 44-39 | 44-42 | -1.17% | 59.8 | 60.2 | 75.0% | 76.5% | 0.95 |
| Luminosity | yxngstxr (22047) | 53-47 | 56-48 | +0.92% | 72.2 | 73.8 | 75.0% | 76.8% | 1.20 |
| Luminosity | Rainwaker (17145) | 44-46 | 46-48 | +1.40% | 81.2 | 82.5 | 69.4% | 64.4% | 1.11 |
| Luminosity | afro (19926) | 45-38 | 43-41 | +3.41% | 57.6 | 56.2 | 69.4% | 62.1% | 1.10 |
| Luminosity | lux (20563) | 45-52 | 44-54 | -2.92% | 72.0 | 70.8 | 68.1% | 67.6% | 0.85 |
| Luminosity | AZUWU (22106) | 36-54 | 37-55 | -1.65% | 59.5 | 60.8 | 69.4% | 71.5% | 0.84 |

**Top player card.** `.potm-container .headline a` identifies yxngstxr (22047), and `.header-info` says “(3 maps)”. For each `.potm-container .potm-chart`, pair `.potm-chart-stat` with `.potm-chart-bar-value`. This gives KPR 0.74, DPR 0.65, KAST 75.0%, MK rating 1.36, Swing +0.92%, ADR 72.2 and Rating 3.0 1.20. KPR and DPR represent kills and deaths per round. Bar heights and the average-line position are presentation coordinates. The card gives MK rating for the featured player only. The standard ten-player tables have no individual columns for assists, headshots, openings, clutches, flash assists or MK rating. For a deeper dataset, the detailed stats links and demo are the entry points; their contents require separate inspection.

## 6. Each team’s past matches

The heading `.past-matches .past-matches-header .headline` labels the window “Matches, past 3 months”. The two grids represent **Core** and **Team**. They are already present together, and Core is the selected tab. Record which view supplied a row. The HTML here gives no formal definition of the core membership rule.

| Item | Selector | Extraction |
|---|---|---|
| Core view | `.past-matches-grid[data-past-matches-core]` | Two `.past-matches-box` children; 25 GamerLegion rows, 20 Luminosity rows. |
| Team view | `.past-matches-grid[data-past-matches-team]` | Same two row counts. Hidden on initial render. |
| Subject team | Within each box → `.past-matches-headline a` | Name plus team ID from href. Sets the perspective of scores in this box. |
| Historical record | Within each box → `.past-matches-table tr` | One listed encounter. Keep its view, owner team and source match ID. |
| Opponent | Within row → `.past-matches-team .past-matches-teamname a` | Name and team ID from href. `.past-matches-team img.flag` gives the listed region. |
| Recency | `.past-matches-time-ago` inside row | Relative text, e.g. “3 weeks ago”. Follow the result link for its exact timestamp. |
| Format or map | `.past-matches-map a` inside row | Text varies. `bo3` and `bo5` describe series; `nuke`, `d2`, `mrg` and `anc` are map labels in this file. Keep the raw token. |
| Historical result link | `.past-matches-score a` inside row | Read href. The format/map link repeats the URL. |
| Score and outcome | Same score anchor | Split text on the spaced hyphen. First value is the **subject team’s** score. Class `won` or `lost` describes that team’s result. Format rows report maps won; map-labelled rows show rounds. |

**Orientation check.** GamerLegion’s first row is HOTU, “3 weeks ago”, bo3, **0–2**, lost, match 2399879. HOTU is first in the URL slug but GamerLegion is first in this box’s score. Match **2397887** illustrates the same rule twice: GamerLegion’s box shows Nuke **13–7**, Luminosity’s box shows **7–13**. A dataset of team perspectives can retain both. A dataset of matches should join by match ID. A window of three months is not a lifetime record, and combining Core and Team blindly counts overlapping results twice.

## 7. Head-to-head history

The summary and detail list are siblings under `.match-page`. Use `.head-to-head-listing` directly for the detail rows. The section gives a map-level history for the displayed pairing.

| Field | Selector | Value or rule |
|---|---|---|
| Summary team identities | `.head-to-head .team1 a.teamName`, `.head-to-head .team2 a.teamName` | GamerLegion and Luminosity, with team URLs. |
| Win/OT summary | `.head-to-head .flexbox-column` | Within each, pair `.bold` with `.description`. In order: left wins **2**, overtimes **0**, right wins **0**. The middle label is “Overtimes”. |
| Prior map | `.head-to-head-listing tr.row` | One row per listed map. Group rows by prior match ID when counting distinct encounters. |
| Prior match ID and time | Within row → `td.date a`, `td.date [data-unix]` | Parse match ID from href, integer milliseconds from span. Keep timestamp as displayed for the H2H row. |
| Team IDs, names, flags | `td.team1 a`, `td.team2 a`, and `img.flag` under each team cell | Read the teams on the row in score order. Mobile logo-only cells repeat them. |
| Listed player names | `td.team1[title]`, `td.team2[title]` | Comma-separated names. The first GamerLegion row lists only Tauson, FL4MUS and hypex. Treat as the supplied names, never a guaranteed historical five-player lineup. |
| Event | `td.event a` | Text and event ID from href. The cell title also spells out the event. |
| Map | `td.map .dynamic-map-name-full` | Full name. Sibling `.dynamic-map-name-short` is the short token. |
| Scores and winner | `td.result > span`; `td.team1.winner, td.team2.winner` | First and second numeric spans align to row teams 1 and 2. Span classes mark won/lost. Compare both scores when deriving the map winner. |

| Display date | Past match ID | data-unix (ms) | Event (ID) | Map | GL–Luminosity |
|---|---:|---:|---|---|---|
| 17/9 26 | 2397887 | 1789640479000 | Logitech G Play Connect 2026 (9266) | Nuke | 13–7 |
| 12/8 26 | 2396560 | 1786527398000 | Esports World Cup 2026 (8261) | Nuke | 13–3 |

The second link’s slug puts Luminosity first, while the row puts GamerLegion first. Recover historical series scores from the linked match headers if needed. The summary here predates the current result: the current Mirage overtime and Luminosity series victory are outside these listed totals.

## 8. VRS points and result-only movement

The `.vrs-forecast-container` stores three aligned lists. Read names from `.vrs-forecast-team-row .vrs-forecast-team-name` in order. Read the “before” records from `.vrs-forecast-left-numbers .vrs-forecast-numbers-wrapper` and the “result” records from `.vrs-forecast-middle .vrs-forecast-numbers-wrapper`. Pair by index **within each list**. The “before” and “result” headers sit between records, so selecting every child by position can misalign the second team.

| Value | Selector within the corresponding wrapper | GamerLegion | Luminosity |
|---|---|---:|---:|
| Before points | Before → `.vrs-forecast-points` | 1314pt | 1282pt |
| Before rank | Before → `.vrs-forecast-ranking` | #40 | #46 |
| Result points **change** | Result → `.vrs-forecast-points` | −11pt | +29pt |
| Result-only rank | Result → `.vrs-forecast-ranking` | #42 | #40 |
| Rank movement | Result → `.vrs-forecast-small-points` | −2 | +6 |
| Direction | Result rank element’s class | falling | rising |

Strip `pt` or `#` as appropriate and retain numeric signs. The explanation in `.tooltip-box.vrs-popup` says final impact can also depend on placement and prize money. A result-cell value of +29 is a change in points. The projected rank of #40 for Luminosity is distinct from its pre-match #46. Use the before column for historical pre-match features.

## 9. Replay resources and page boundaries

| Resource | Selector | What this file supplies |
|---|---|---|
| Demo | `.streams [data-demo-link]` | `data-demo-link="/download/demo/113801"`. The anchor stores the target in a data attribute. The replay bytes and its round events are external. |
| Map VODs | `.streams .stream-box[data-stream-embed]` | URL in the attribute, label under `.flagAlign .spoiler`, region in `img.stream-flag[title]`. Three distinct Kick URLs point to start offsets 25881, 30650 and 34023 seconds. Deduplicate by complete URL, including timestamp, across spoiler copies. |
| Highlights | `iframe.allstargg-iframe` | Day and night copies reference local companion HTML. The parent document alone supplies no play-by-play from inside these frames. |

Keep sidebar live scores, other fixtures, comments and site navigation outside the match dataset. Historical Elo, rolling win rate and map-pool strength are derived features. They have no dedicated value in these result tables.

## 10. Executable check for the player-stat matrix

Run the following JavaScript on the saved document in a browser console. It reads the existing DOM and returns one record per match, team, player, map scope and side. Percentages are stored in percentage points, e.g. 75.0 for 75%. The aggregate panel uses a null map ID. The code deliberately fails on a changed field or unexpected number format so a missing value cannot become zero.

```js
(() => {
  const root = document.querySelector('.match-page #match-stats');
  const matchId = document.querySelector('link[rel="canonical"]')
    .href.match(/\/matches\/(\d+)\//)[1];
  const rows = [];
  const numeric = (value) => {
    const clean = value.trim().replace(/%$/, '').replace('−', '-');
    if (!/^[+-]?\d+(?:\.\d+)?$/.test(clean)) throw Error('Unexpected value ' + value);
    return Number(clean);
  };
  for (const panel of root.querySelectorAll('.stats-content')) {
    const scope = panel.id.replace(/-content$/, '');
    for (const table of panel.querySelectorAll('table')) {
      const header = table.querySelector('.header-row');
      const teamId = header.querySelector('a.teamName').href.match(/\/team\/(\d+)\//)[1];
      const side = { totalstats: 'both', ctstats: 'CT', tstats: 'T' }[
        [...table.classList].find(c => ['totalstats', 'ctstats', 'tstats'].includes(c))
      ];
      if (!side) throw Error('Unknown side filter');
      for (const row of table.querySelectorAll('tr:not(.header-row)')) {
        const read = sel => row.querySelector(sel).textContent.trim();
        const pair = sel => {
          const [, k, d] = read(sel).match(/^(\d+)\s*-\s*(\d+)$/);
          return [Number(k), Number(d)];
        };
        const [kills, deaths] = pair('.kd.traditional-data');
        const [ecoKills, ecoDeaths] = pair('.kd.eco-adjusted-data');
        rows.push({
          matchId, teamId, mapStatsId: scope === 'all' ? null : scope, side,
          playerId: row.querySelector('.players a').href.match(/\/player\/(\d+)\//)[1],
          nick: read('.player-nick'), kills, deaths, ecoKills, ecoDeaths,
          swingPct: numeric(read('.roundSwing')),
          adr: numeric(read('.adr.traditional-data')),
          ecoAdr: numeric(read('.adr.eco-adjusted-data')),
          kastPct: numeric(read('.kast.traditional-data')),
          ecoKastPct: numeric(read('.kast.eco-adjusted-data')),
          rating: numeric(read('.rating')),
          ratingVersion: header.querySelector('.ratingDesc').textContent.trim()
        });
      }
    }
  }
  return rows;
})()
```

**Expected checks for this file.** Four map scopes × two teams × three sides = 24 tables. Five players per table gives 120 distinct stat records, ten of which are series/both. Map scores imply 30 + 20 + 22 = 72 rounds. The left team won one map and the right team won two. The history grids have 90 displayed rows across both views, and the H2H list has two maps.

**Forecasting cutoff.** Store the result, map outcomes, current-match player stats, top-player card and VRS result column as post-match observations. For a model predicting this fixture, compute rolling stats only from previously completed matches. Store ranking reference dates and the time you observed a roster or veto. The result page preserves the displayed lineup and picks but does not establish when they first became available.
