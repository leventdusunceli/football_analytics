"""
Minute-based timeline visualizations.
"""

import matplotlib.pyplot as plt
import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def plot_assist_timeline(assists: pd.DataFrame, ax: plt.Axes | None = None) -> plt.Axes:
    """
    Plot a match's assists along a minute axis, one row per team.

    Args:
        assists: DataFrame from StatsBombClient.get_passes, pre-filtered to
            pass_goal_assist == True. Needs player, team, minute columns.
        ax: Optional existing Axes to draw on. A new Axes is created if
            not provided.

    Returns:
        The Axes the timeline was drawn on.

    Raises:
        DataNotFoundError: If assists is empty.
    """
    if assists.empty:
        raise DataNotFoundError("assists is empty, nothing to plot.")

    if ax is None:
        _, ax = plt.subplots(figsize=(8, 2.5))

    teams = sorted(assists["team"].unique())
    team_y = {team: i for i, team in enumerate(teams)}

    for team, subset in assists.groupby("team"):
        y = [team_y[team]] * len(subset)
        ax.scatter(subset["minute"], y, s=100, zorder=3)
        for _, row in subset.iterrows():
            ax.annotate(
                row["player"],
                (row["minute"], team_y[team]),
                textcoords="offset points",
                xytext=(0, 8),
                ha="center",
                fontsize=8,
            )

    ax.set_yticks(list(team_y.values()))
    ax.set_yticklabels(list(team_y.keys()))
    ax.set_xlabel("Minute")
    ax.set_xlim(0, max(90, assists["minute"].max() + 5))
    ax.set_title("Assist Timeline")
    ax.grid(axis="x", linestyle="--", alpha=0.4)

    return ax
