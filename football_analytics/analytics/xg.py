"""
Expected goals (xG) analytics
Computes xG summaries, over/under performers and player xG stats
from StatsBomb data
"""

import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def get_match_xg_summary(shots: pd.DataFrame) -> pd.DataFrame:
    """
    Summary of xG stats and actual goals per team for a single match

    Args:
        shots: DataFrame returned by StatsBombClient.get_shots


    Returns:
        DataFrame with columns: team, total_shots, shots_on_target,
        total_xg, goals, xg_difference.
        xg_difference is goals minus xG — positive means overperformance.

    Raises:
        DataNotFoundError: If the shots DataFrame is empty.
    """
    if shots.empty:
        raise DataNotFoundError("Shots DataFrame is empty")

    summary = shots.groupby("team").agg(
        total_shots=("shots_statsbomb_xg", "count"),
        shots_on_target=(
            "shot_outcome",
            lambda x: ((x == "Goal") | (x == "Saved")).sum(),
        ),
        total_xg=("shot_statsbomb_xg", "sum"),
        goals=("shot_outcome", lambda x: (x == "Goal").sum()),
    )
    summary["total_xg"] = summary["total_xg"].round(3)
    summary["xg_difference"] = (summary["goals"] - summary["total_xg"]).round(3)
    return summary


def get_player_xg_ranking(shots: pd.DataFrame) -> pd.DataFrame:
    """
    Rank players by their xG for a match or by an aggregation of shots
    in different matches

    Args:
        shots: DataFrame returned by StatsBombClient.get_shots or an
        aggregated shots DataFrame across multiple matches

    Returns:
        DataFrame ranked by total_xg with colummns: player, team,
        shots, goals, total_xg, xg_per_shot.

    Raises:
        DataNotFoundError: If the shots DataFrame is empty.
    """
    if shots.empty:
        raise DataNotFoundError("shots DataFrame is empty.")

    ranking = (
        shots.groupby(["player", "team"])
        .agg(
            shots=("shot_statsbomb_xg", "count"),
            goals=("shot_outcome", lambda x: (x == "Goal").sum()),
            total_xg=("shot_statsbomb_xg", "sum"),
        )
        .reset_index()
    )
    ranking["xg_per_shot"] = (ranking["total_xg"] / ranking["shots"]).round(3)
    ranking["total_xg"] = ranking["total_xg"].round(3)
    return ranking.sort_values("total_xg", ascending=False).reset_index(drop=True)


def get_xg_overperformance(
    shots: pd.DataFrame,
    min_shots: int = 20,
    min_minutes: int | None = None,
):
    """
    Identify players significantly over or underperforming their xG
    across a season or a significant period of games.

    Designed to be used with aggregated shot data across multiple matches
    rather than a single game. Single match samples are too small to draw
    meaningful conclusions from xG overperformance.

    Args:
        shots: Aggregated shots DataFrame across multiple matches.
            Typically built by concatenating get_shots() results
            across a season's worth of matches.
        min_shots: Minimum total shots to filter out small samples.
                    Defaults to 20
        min_minutes: Optional minimum minutes played filter. Requires
                    a 'minutes_played' column in the shots DataFrame.

    Returns:
        DataFrame sorted by xg_difference descending with columns:
        player, team, total_shots, shots_on_target, goals,
        total_xg, xg_difference. Positive xg_difference means the
        player scored more than xG expected (overperforming).

    Raises:
        DataNotFoundError: If the shots DataFrame is empty or no players meet the minimum 
        shots threshold.
    """
    if shots.empty:
        raise DataNotFoundError("shots DataFrame is empty.")

    stats = (
        shots.groupby(["player", "team"])
        .agg(
            total_shots=("shot_statsbomb_xg", "count"),
            shots_on_target=(
                "shot_outcome",
                lambda x: ((x == "Goal") | (x == "Saved")).sum(),
            ),
            goals=("shot_outcome", lambda x: (x == "Goal").sum()),
            total_xg=("shot_statsbomb_xg", "sum"),
        )
        .reset_index()
    )

    stats = stats[stats["total_shots"] >= min_shots].copy()
    if stats.empty:
        raise DataNotFoundError(
            f"No players found with at least {min_shots} shots. "
            "Try lowering the min_shots threshold or using more matches."
        )

    if min_minutes is not None:
        if "minutes_played" not in stats.columns:
            raise DataNotFoundError(
                "Cannot filter by minutes — 'minutes_played' column not "
                "found in the shots DataFrame."
            )
        stats = stats[stats["minutes_played"] >= min_minutes].copy()
        if stats.empty:
            raise DataNotFoundError(
                f"No players found with at least {min_minutes} minutes played."
            )

    stats["total_xg"] = stats["total_xg"].round(3)
    stats["xg_difference"] = (stats["goals"] - stats["total_xg"]).round(3)

    return (
        stats[["player", "team", "total_shots", "shots_on_target",
            "goals", "total_xg", "xg_difference"]]
        .sort_values("xg_difference", ascending=False)
        .reset_index(drop=True)
    )

