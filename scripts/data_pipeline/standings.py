"""
Full standings and xG-vs-actual-position static data for
config.FULL_SEASON_ID, generalizing the demo notebook's ad hoc Section
5.4 calculation (which only covered three teams) to the whole league.
"""

import pandas as pd

from football_analytics.analytics import (
    get_expected_vs_actual,
    get_match_xg_summary,
    get_standings_from_matches,
)
from football_analytics.data import StatsBombClient
from scripts.data_pipeline import config


def build_standings_tables(client: StatsBombClient) -> dict[str, pd.DataFrame]:
    """Full standings and xG-vs-actual position for every team in
    config.FULL_SEASON_ID."""
    matches = client.get_matches(config.COMPETITION_ID, config.FULL_SEASON_ID)
    standings = get_standings_from_matches(matches)

    team_shots = client.get_shots_season(config.COMPETITION_ID, config.FULL_SEASON_ID)
    team_xg = get_match_xg_summary(team_shots)[["team", "total_xg"]]

    return {
        "standings": standings,
        "expected_vs_actual": get_expected_vs_actual(standings, team_xg),
    }
