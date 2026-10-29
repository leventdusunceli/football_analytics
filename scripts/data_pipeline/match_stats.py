"""
Match-level static data: raw shot/pass/defensive events (for the site's
D3 pitch charts) plus per-player summaries, built once per match.
"""

import pandas as pd

from football_analytics.analytics import (
    get_match_xg_summary,
    get_player_xg_ranking,
    get_team_passing_summary,
)
from football_analytics.data import StatsBombClient
from scripts.data_pipeline import config


def build_match_tables(
    client: StatsBombClient, match_id: int
) -> dict[str, pd.DataFrame]:
    """Fetch every match-level table the web demo needs for match_id,
    keyed by the name it's written to JSON under."""
    shots = client.get_shots(match_id)
    passes = client.get_passes(match_id)
    passing_summary = client.get_player_passing_match(match_id)

    return {
        "shots": shots,
        "passes": passes,
        "defensive_events": client.get_defensive_events_match(match_id),
        "shooting_summary": client.get_player_shooting_match(match_id),
        "passing_summary": passing_summary,
        "defensive_summary": client.get_player_defensive_match(match_id),
        "goals_assists": client.get_player_goals_assists_match(match_id),
        "passing_team_summary": get_team_passing_summary(passing_summary),
        "xg_summary": get_match_xg_summary(shots),
        "xg_ranking": get_player_xg_ranking(shots),
        # Everyone who played, so the site can list players with no events.
        "lineup": client.get_player_minutes_match(match_id),
    }


def build_match_index(client: StatsBombClient) -> pd.DataFrame:
    """Fetch the full fixture list for config.FULL_SEASON_ID, trimmed to
    what the website's team-then-match picker needs."""
    matches = client.get_matches(config.COMPETITION_ID, config.FULL_SEASON_ID)
    return matches[
        [
            "match_id",
            "match_date",
            "match_week",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
        ]
    ].sort_values("match_date")
