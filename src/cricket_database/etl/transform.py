from __future__ import annotations

from typing import Dict, List, Optional

from .models import MatchModel, InningsModel, BattingEntry, BowlingEntry, FieldingEntry, Delivery


def to_rows(match: MatchModel, source_id: int) -> Dict[str, List[dict]]:
    """Map MatchModel to relational row dicts for bulk upserts.

    Returns a dict of table_name -> list of row dicts in load order.
    """
    rows: Dict[str, List[dict]] = {
        "countries": [],
        "teams": [],
        "players": [],
        "venues": [],
        "series": [],
        "seasons": [],
        "matches": [],
        "match_teams": [],
        "innings": [],
        "batting_innings": [],
        "bowling_innings": [],
        "fielding_innings": [],
        "deliveries": [],
        "team_alias": [],
        "player_alias": [],
        "source_keys": [],
    }

    # Venue
    if match.venue and match.venue.name:
        rows["venues"].append({
            "name": match.venue.name,
            "city": match.venue.city,
            "country_name": match.venue.country,
        })

    # Series/season (optional)
    season_name = match.start_date[:4] if match.start_date else None
    if season_name:
        rows["seasons"].append({"name": season_name, "start_date": match.start_date, "end_date": match.end_date})
    if match.series_name:
        rows["series"].append({"name": match.series_name, "season_name": season_name})

    # Teams + aliases
    for t in match.teams:
        if not t.name:
            continue
        rows["teams"].append({"name": t.name, "country_name": None})
        rows["team_alias"].append({"alias": t.name, "source_id": source_id})

    # Match core
    rows["matches"].append({
        "source_match_key": match.source_match_key,
        "format": match.format or "Unknown",
        "start_date": match.start_date,
        "end_date": match.end_date,
        "result_type": match.result.result_type if match.result else None,
        "winner_team_name": match.result.winner.name if (match.result and match.result.winner) else None,
        "toss_winner_team_name": match.toss.winner.name if (match.toss and match.toss.winner) else None,
        "toss_decision": match.toss.decision if match.toss else None,
        "day_night": bool(match.day_night),
        "follow_on": bool(match.follow_on),
        "dl_method": bool(match.dl_method),
        "venue_name": match.venue.name if match.venue else None,
        "series_name": match.series_name,
    })

    # Match teams
    if match.teams:
        for t in match.teams[:2]:
            if t.name:
                rows["match_teams"].append({"team_name": t.name, "is_home": 0})

    # Innings and components
    for inn in match.innings:
        rows["innings"].append({
            "innings_no": inn.innings_no,
            "batting_team_name": inn.batting_team.name,
            "bowling_team_name": inn.bowling_team.name,
            "runs": inn.runs,
            "wickets": inn.wickets,
            "overs": inn.overs,
            "declared": int(bool(inn.declared)),
            "follow_on_enforced": int(bool(inn.follow_on_enforced)),
        })
        for be in inn.batting:
            rows["players"].append({"full_name": be.player.name, "country_name": None})
            rows["player_alias"].append({"alias": be.player.name, "source_id": source_id})
            rows["batting_innings"].append({
                "player_full_name": be.player.name,
                "position": be.position,
                "runs": be.runs,
                "balls": be.balls,
                "minutes": be.minutes,
                "fours": be.fours,
                "sixes": be.sixes,
                "how_out": be.how_out,
                "bowler_full_name": be.bowler.name if be.bowler else None,
                "fielder_full_name": be.fielder.name if be.fielder else None,
            })
        for bw in inn.bowling:
            rows["players"].append({"full_name": bw.player.name, "country_name": None})
            rows["player_alias"].append({"alias": bw.player.name, "source_id": source_id})
            rows["bowling_innings"].append({
                "player_full_name": bw.player.name,
                "overs": bw.overs,
                "maidens": bw.maidens,
                "runs": bw.runs,
                "wickets": bw.wickets,
                "wides": bw.wides,
                "no_balls": bw.no_balls,
                "econ": bw.econ,
            })
        for d in inn.deliveries:
            rows["players"].extend([
                {"full_name": d.striker.name, "country_name": None},
                {"full_name": d.non_striker.name, "country_name": None},
                {"full_name": d.bowler.name, "country_name": None},
            ])
            if d.dismissal_player:
                rows["players"].append({"full_name": d.dismissal_player.name, "country_name": None})
            rows["deliveries"].append({
                "over_no": d.over_no,
                "ball_no": d.ball_no,
                "striker_full_name": d.striker.name,
                "non_striker_full_name": d.non_striker.name,
                "bowler_full_name": d.bowler.name,
                "runs_off_bat": d.runs_off_bat,
                "extras_bye": d.extras_bye,
                "extras_legbye": d.extras_legbye,
                "extras_wide": d.extras_wide,
                "extras_noball": d.extras_noball,
                "extras_penalty": d.extras_penalty,
                "wicket_type": d.wicket_type,
                "dismissal_full_name": d.dismissal_player.name if d.dismissal_player else None,
            })

    # Source keys
    if match.source_match_key:
        rows["source_keys"].append({
            "entity_type": "match",
            "source_key": match.source_match_key,
        })

    return rows


