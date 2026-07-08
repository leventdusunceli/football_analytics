"""
Player performance analytics.
Rankings and per-90 minute rankings of player stats
"""

import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def per_90(value: float, minutes: float) -> float:
    """
    Helper functionn to normalise a stat to a per-90-minutes rate.

    Args:
        value: The raw stat total e.g. total goals scored.
        minutes: Total minutes played.

    Returns:
        Stat per 90 minutes rounded to two decimal places.
        Returns 0.0 if minutes is zero to avoid division errors.
    """
    if minutes == 0:
        return 0.0
    return round((value / minutes) * 90, 2)


def get_top_performers(
    player_stats: pd.DataFrame,
    metric: str,
    top_n: int = 10,
    min_threshold: float | None = None,
    threshold_col: str | None = None,
) -> pd.DataFrame:
    """
    Return the top N players ranked by a given metric.

    Args:
        player_stats: Any player stats DataFrame from StatsBombClient
                    e.g. from get_player_shooting_season.
        metric: Column name to rank by e.g. 'total_xg', 'tackles', 'goals'.
        top_n: Number of players to return. Defaults to 10.
        min_threshold: Optional minimum value for threshold_col to filter
                    out small sample sizes e.g. min 5 shots.
        threshold_col: Column to apply min_threshold filter against.
                    Required if min_threshold is provided.

    Returns:
        DataFrame of top N players sorted by metric descending.

    Raises:
        DataNotFoundError: If the metric column does not exist or no players
                        meet the threshold.
    """
    if metric not in player_stats.columns:
        raise DataNotFoundError(
            f"Metric '{metric}' not found in DataFrame. "
            f"Available columns: {list(player_stats.columns)}"
        )

    df = player_stats.copy()

    if min_threshold is not None and threshold_col is not None:
        df = df[df[threshold_col] >= min_threshold]
        if df.empty:
            raise DataNotFoundError(
                f"No players found with {threshold_col} >= {min_threshold}."
            )

    return df.sort_values(metric, ascending=False).head(top_n).reset_index(drop=True)


def add_per_90_columns(
    player_stats: pd.DataFrame,
    stat_cols: list[str],
    minutes_col: str = "minutes_played",
) -> pd.DataFrame:
    """
    Add per-90 normalised columns to a player stats DataFrame.

    Args:
        player_stats: Any player stats DataFrame containing a minutes column.
        stat_cols: List of column names to normalise e.g. ['goals', 'assists'].
        minutes_col: Name of the column containing minutes played.
                    Defaults to 'minutes_played'.

    Returns:
        Original DataFrame with additional '{col}_per_90' columns added.

    Raises:
        DataNotFoundError: If minutes_col is not found in the DataFrame.
    """
    if minutes_col not in player_stats.columns:
        raise DataNotFoundError(
            f"Minutes column '{minutes_col}' not found in DataFrame."
        )

    df = player_stats.copy()
    for col in stat_cols:
        if col in df.columns:
            df[f"{col}_per_90"] = df.apply(
                lambda row: per_90(row[col], row[minutes_col]), axis=1
            )
    return df
