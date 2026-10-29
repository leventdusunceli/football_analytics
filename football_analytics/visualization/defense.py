"""
Defensive-action visualizations.
Pitch map built from StatsBombClient.get_defensive_events_match output.
"""

import matplotlib.pyplot as plt
import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError
from football_analytics.visualization.match import _build_title, _draw_pitch

DEFAULT_ACTION_COLORS = {
    "tackle": "#2a5599",
    "interception": "#27ae60",
    "clearance": "#e8a33d",
    "block": "#8e44ad",
    "ball_recovery": "#16a085",
    "dribbled_past": "#c0392b",
    "foul_committed": "#7f8c8d",
}


def plot_defensive_actions_map(
    defensive_events: pd.DataFrame,
    colors: dict[str, str] | None = None,
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Draw a match's defensive actions as located markers on a pitch, one
    marker color per action type.

    Args:
        defensive_events: DataFrame from
            StatsBombClient.get_defensive_events_match.
        colors: Optional overrides for any action_type in
            DEFAULT_ACTION_COLORS.
        ax: Optional existing Axes to draw the pitch on. A new pitch
            figure/Axes is created if not provided.

    Returns:
        The Axes the pitch and markers were drawn on.

    Raises:
        DataNotFoundError: If defensive_events is empty.
    """
    if defensive_events.empty:
        raise DataNotFoundError("defensive_events is empty, nothing to plot.")

    colors = {**DEFAULT_ACTION_COLORS, **(colors or {})}
    pitch, ax = _draw_pitch(ax)

    for action_type, subset in defensive_events.groupby("action_type"):
        pitch.scatter(
            subset["location"].str[0],
            subset["location"].str[1],
            color=colors.get(action_type, "#000000"),
            ax=ax,
            label=action_type.replace("_", " ").title(),
            s=80,
            edgecolors="black",
        )

    ax.set_title(_build_title(defensive_events, "Defensive Actions"))
    ax.legend(loc="upper left", fontsize=8, framealpha=0.9)

    return ax
