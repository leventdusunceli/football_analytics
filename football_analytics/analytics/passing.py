"""
Passing analytics.
Team-level passing summaries built from player-level match stats.
"""

import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def get_team_passing_summary(passing_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-player passing stats into team totals for a match.

    Args:
        passing_stats: DataFrame from StatsBombClient.get_player_passing_match.

    Returns:
        DataFrame with columns: team, passes, passes_completed,
        completion_rate, progressive_passes, line_breaking_passes.

    Raises:
        DataNotFoundError: If passing_stats is empty.
    """
    if passing_stats.empty:
        raise DataNotFoundError("passing_stats is empty.")

    team_stats = (
        passing_stats.groupby("team")
        .agg(
            passes=("passes", "sum"),
            passes_completed=("passes_completed", "sum"),
            progressive_passes=("progressive_passes", "sum"),
            line_breaking_passes=("line_breaking_passes", "sum"),
        )
        .reset_index()
    )
    team_stats["completion_rate"] = (
        (team_stats["passes_completed"] / team_stats["passes"]) * 100
    ).round(1)
    return team_stats[
        [
            "team",
            "passes",
            "passes_completed",
            "completion_rate",
            "progressive_passes",
            "line_breaking_passes",
        ]
    ]
