"""
Team-level static data for the team-comparison panel: goals, xG, assists,
possession, clean sheets, pressures, and fouls won, one row per team in
config.FULL_SEASON_ID.
"""

import pandas as pd

from football_analytics.analytics import get_clean_sheets, get_match_xg_summary
from football_analytics.data import StatsBombClient
from scripts.data_pipeline import config


def build_team_tables(
    client: StatsBombClient, full_roster: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """
    Fetch every team-level table the team-comparison panel needs, keyed
    by the name it's written to JSON under. Reuses the player roster
    already fetched by season_stats.build_full_roster_tables, so this
    only adds the possession/clean-sheets/goals-xG pulls, not a second
    full-season sweep of player data.
    """
    season_id = config.FULL_SEASON_ID
    matches = client.get_matches(config.COMPETITION_ID, season_id)

    team_shots = client.get_shots_season(config.COMPETITION_ID, season_id)
    goals_xg = get_match_xg_summary(team_shots)

    assists = (
        full_roster["goals_assists"]
        .groupby("team")
        .agg(assists=("assists", "sum"))
        .reset_index()
    )

    pressures = (
        full_roster["defensive"]
        .groupby("team")
        .agg(pressures=("pressures", "sum"))
        .reset_index()
    )
    fouls_won = (
        full_roster["ball_carrying"]
        .groupby("team")
        .agg(fouls_won=("fouls_won", "sum"))
        .reset_index()
    )
    pressures_fouls = pressures.merge(fouls_won, on="team", how="outer").fillna(0)

    return {
        "goals_xg": goals_xg,
        "assists": assists,
        "possession": client.get_team_possession_season(
            config.COMPETITION_ID, season_id
        ),
        "clean_sheets": get_clean_sheets(matches),
        "pressures_fouls": pressures_fouls,
    }
