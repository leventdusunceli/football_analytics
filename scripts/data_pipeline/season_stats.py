"""
Season-level static data: two separate pulls.

"Full roster" is every player who appeared in config.FULL_SEASON_ID, used
by the team/player comparison panels. "Trend" is the nine named players
across config.TREND_SEASON_IDS, used only by the season-over-season
trend chart, which needs both seasons for those nine specifically.
"""

import pandas as pd

from football_analytics.analytics import get_per_90_stats
from football_analytics.data import StatsBombClient
from scripts.data_pipeline import config


def _position_group(position: str) -> str:
    """Bucket a StatsBomb position string into forward/midfielder/
    defender/goalkeeper, for the player picker's filter."""
    position = position.lower()
    if "goalkeeper" in position:
        return "goalkeeper"
    if "back" in position:
        return "defender"
    if "midfield" in position:
        return "midfielder"
    return "forward"


def _attach_display_names(
    client: StatsBombClient,
    season_ids: int | list[int],
    tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Merge each player's short display name (e.g. "Lionel Messi") into
    every table that has a player column, so the website never has to
    show a full legal name."""
    names = client.get_player_display_names(config.COMPETITION_ID, season_ids)
    merged = {}
    for name, df in tables.items():
        if "player" not in df.columns:
            merged[name] = df
            continue
        df = df.merge(names, on="player", how="left")
        df["display_name"] = df["display_name"].fillna(df["player"])
        merged[name] = df
    return merged


def build_full_roster_tables(client: StatsBombClient) -> dict[str, pd.DataFrame]:
    """Fetch every season-level table for every player in
    config.FULL_SEASON_ID, keyed by the name it's written to JSON under."""
    season_id = config.FULL_SEASON_ID
    shooting = client.get_player_shooting_season(config.COMPETITION_ID, season_id)
    passing = client.get_player_passing_season(config.COMPETITION_ID, season_id)
    defensive = client.get_player_defensive_season(config.COMPETITION_ID, season_id)
    goals_assists = client.get_player_goals_assists_season(
        config.COMPETITION_ID, season_id
    )
    minutes = client.get_player_minutes_season(config.COMPETITION_ID, season_id)
    ball_carrying = client.get_player_ball_carrying_season(
        config.COMPETITION_ID, season_id
    )

    shooting_per90 = get_per_90_stats(shooting, minutes, ["goals", "total_xg"])
    goals_assists_per90 = get_per_90_stats(goals_assists, minutes, ["goals", "assists"])

    # From minutes (lineups), not shooting: shooting only lists players
    # who took a shot, which left out every goalkeeper.
    roster = minutes[["player", "team", "position"]].drop_duplicates(
        subset=["player", "team"]
    )
    roster = roster.assign(position_group=roster["position"].map(_position_group))

    return _attach_display_names(
        client,
        season_id,
        {
            "shooting": shooting,
            "passing": passing,
            "defensive": defensive,
            "goals_assists": goals_assists,
            "minutes": minutes,
            "ball_carrying": ball_carrying,
            "shooting_per90": shooting_per90,
            "goals_assists_per90": goals_assists_per90,
            "roster": roster,
        },
    )


def build_trend_tables(client: StatsBombClient) -> dict[str, pd.DataFrame]:
    """Fetch the nine named players' stats across both trend seasons,
    keyed by the name it's written to JSON under."""
    shooting = client.get_player_shooting_season(
        config.COMPETITION_ID, config.TREND_SEASON_IDS, players=config.PLAYERS
    )
    goals_assists = client.get_player_goals_assists_season(
        config.COMPETITION_ID, config.TREND_SEASON_IDS, players=config.PLAYERS
    )
    minutes = client.get_player_minutes_season(
        config.COMPETITION_ID, config.TREND_SEASON_IDS, players=config.PLAYERS
    )

    return _attach_display_names(
        client,
        config.TREND_SEASON_IDS,
        {
            "shooting_per90": get_per_90_stats(
                shooting, minutes, ["goals", "total_xg"]
            ),
            "goals_assists_per90": get_per_90_stats(
                goals_assists, minutes, ["goals", "assists"]
            ),
        },
    )
