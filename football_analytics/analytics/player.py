"""
Player performance analytics.
Per-90 normalisation and top-performer rankings over any season-level
stats DataFrame (shooting, passing, defensive, goals/assists).
"""

import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def get_per_90_stats(
    season_stats: pd.DataFrame,
    minutes: pd.DataFrame,
    stat_cols: list[str],
) -> pd.DataFrame:
    """
    Normalise counting stats to a per-90-minutes basis.

    Args:
        season_stats: Any season-level DataFrame with player, team,
            season_id columns plus the columns named in stat_cols, e.g.
            StatsBombClient.get_player_shooting_season output.
        minutes: DataFrame from StatsBombClient.get_player_minutes_season,
            merged on player/team/season_id.
        stat_cols: Columns in season_stats to normalise, e.g.
            ["goals", "total_xg"].

    Returns:
        season_stats merged with minutes_played, plus a "<col>_per90"
        column for each entry in stat_cols. season_stats' own
        matches_played (if present) is replaced with minutes' version,
        since a stat table only has a row per match it recorded that
        stat in (e.g. a match with no shots has no shooting row), while
        minutes counts every match actually played, the correct
        denominator for a per-90 rate.

    Raises:
        DataNotFoundError: If season_stats is empty, or merging against
            minutes produces no rows.
    """
    if season_stats.empty:
        raise DataNotFoundError("season_stats is empty.")

    merge_cols = ["player", "team", "season_id"]
    merged = season_stats.drop(columns=["matches_played"], errors="ignore").merge(
        minutes[merge_cols + ["matches_played", "minutes_played"]],
        on=merge_cols,
        how="inner",
    )
    if merged.empty:
        raise DataNotFoundError(
            "No matching player/team/season_id rows between season_stats "
            "and minutes."
        )

    for col in stat_cols:
        merged[f"{col}_per90"] = (
            (merged[col] / merged["minutes_played"]) * 90
        ).round(2)
    return merged


def get_top_performers(
    season_stats: pd.DataFrame,
    stat_col: str,
    top_n: int = 5,
    ascending: bool = False,
) -> pd.DataFrame:
    """
    Rank players by a single stat column and return the top N rows.

    Args:
        season_stats: Any season-level DataFrame containing stat_col.
        stat_col: Column to rank by.
        top_n: Number of rows to return. Defaults to 5.
        ascending: Sort ascending instead of descending. Defaults to False.

    Returns:
        The top_n rows of season_stats sorted by stat_col.

    Raises:
        DataNotFoundError: If season_stats is empty or stat_col is missing.
    """
    if season_stats.empty:
        raise DataNotFoundError("season_stats is empty.")
    if stat_col not in season_stats.columns:
        raise DataNotFoundError(f"Column '{stat_col}' not found in season_stats.")

    return (
        season_stats.sort_values(stat_col, ascending=ascending)
        .head(top_n)
        .reset_index(drop=True)
    )
