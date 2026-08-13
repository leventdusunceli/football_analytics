"""
Match-level visualizations.
Pitch maps built from StatsBombClient raw pass/shot event DataFrames.
"""

import matplotlib.pyplot as plt
import pandas as pd
from mplsoccer import Pitch

from football_analytics.utils.exceptions import DataNotFoundError

# StatsBomb's pitch is 120 (length) x 80 (width).
PITCH_LENGTH = 120
PITCH_WIDTH = 80

DEFAULT_ARROW_STYLE = {"width": 2, "headwidth": 6, "headlength": 5}

DEFAULT_PASSING_COLORS = {
    "pass": "#2a5599",
    "progressive": "#e8a33d",
    "line_breaking": "#c0392b",
    "assist": "#27ae60",
}


def _normalize_attacking_direction(
    events: pd.DataFrame, location_cols: list[str]
) -> pd.DataFrame:
    """
    Flip locations from even-numbered periods so a team's play reads in a
    consistent direction across a whole match.

    StatsBomb records event locations in real, un-flipped pitch
    coordinates. Teams switch ends at halftime, so the same team's
    attacking play trends toward x=120 in period 1 but toward x=0 in
    period 2. Left as-is, a full-match pitch map would show a team's
    arrows crossing in both directions, which reads as noise rather than
    a coherent attacking pattern. Mirroring every even period's
    coordinates through the pitch center (since both halves are literal
    mirror images of each other) fixes this without needing to know which
    end a team actually attacked first.

    Args:
        events: A pass or shot DataFrame containing 'period' and the
            given location_cols.
        location_cols: Column names holding [x, y] (or [x, y, z]) lists
            to flip.

    Returns:
        A copy of events with the given location columns mirrored for
        even-numbered periods.
    """
    events = events.copy()

    def _flip_if_even_period(period: int, loc: list[float]) -> list[float]:
        if period % 2 == 0:
            return [PITCH_LENGTH - loc[0], PITCH_WIDTH - loc[1], *loc[2:]]
        return loc

    # Assigning a whole new list back to the column (rather than
    # df.loc[mask, col] = ...) avoids a pandas pitfall: assigning a Series
    # of list-valued cells through .loc makes pandas try to align each
    # list's elements as if they were separate columns, not store the list
    # as a single cell value.
    for col in location_cols:
        events[col] = [
            _flip_if_even_period(period, loc)
            for period, loc in zip(events["period"], events[col], strict=True)
        ]
    return events


def _build_title(events: pd.DataFrame, chart_label: str) -> str:
    """
    Infer a sensible chart title from what's actually in the DataFrame:
    a single player if the data was pre-filtered to one, else a single
    team, else a generic fallback. This is what lets plot_passing_map and
    plot_shot_map serve both a whole-team and a single-player view with
    no extra parameters — the caller controls scope by how they filtered
    StatsBombClient.get_passes()/get_shots(), not by anything passed to
    the plotting function itself.
    """
    if events["player"].nunique() == 1:
        subject = events["player"].iloc[0]
    elif events["team"].nunique() == 1:
        subject = events["team"].iloc[0]
    else:
        subject = "Match"
    return f"{subject} — {chart_label}"


def _draw_pitch(ax: plt.Axes | None) -> tuple[Pitch, plt.Axes]:
    """Internal helper: create/draw a StatsBomb-scaled pitch, reused by
    every chart in this module."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color="white", line_color="black")
    if ax is None:
        _, ax = pitch.draw(figsize=(8, 5.5))
    else:
        pitch.draw(ax=ax)
    return pitch, ax


def plot_passing_map(
    passes: pd.DataFrame,
    colors: dict[str, str] | None = None,
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Draw a passing map on a football pitch, categorizing passes by color.

    Player-level input (a single player) draws every pass plus three
    highlighted categories on top: progressive, line-breaking, assist.
    Team-level input skips the full pass volume and draws only the three
    categories, to avoid clutter. Level is inferred the same way as
    _build_title (single player vs. not) — pass a player-filtered or
    team-filtered StatsBombClient.get_passes() result to control it.

    A pass can belong to more than one category (e.g. progressive and an
    assist). Overlapping arrows are drawn in fixed priority, progressive,
    then line-breaking, then assist. The rarer, more notable category
    stays visible on top.

    Args:
        passes: DataFrame from StatsBombClient.get_passes.
        colors: Optional overrides for any of "pass", "progressive",
            "line_breaking", "assist". Falls back to
            DEFAULT_PASSING_COLORS.
        ax: Optional existing Axes to draw the pitch on. A new pitch
            figure/Axes is created if not provided.

    Returns:
        The Axes the pitch and arrows were drawn on.

    Raises:
        DataNotFoundError: If passes is empty.
    """
    if passes.empty:
        raise DataNotFoundError("passes is empty — nothing to plot.")

    colors = {**DEFAULT_PASSING_COLORS, **(colors or {})}
    passes = _normalize_attacking_direction(passes, ["location", "pass_end_location"])
    pitch, ax = _draw_pitch(ax)

    # fillna(False) alone leaves these object-dtype (NaN/True mix), so ~
    # would do bitwise-not on Python bools (~True == -2) instead of not.
    is_progressive = passes["is_progressive"].fillna(False).astype(bool)
    is_line_breaking = passes["pass_through_ball"].fillna(False).astype(bool)
    is_assist = passes["pass_goal_assist"].fillna(False).astype(bool)
    is_player_level = passes["player"].nunique() == 1

    layers = []
    if is_player_level:
        layers.append((passes, colors["pass"], "Pass"))
    layers += [
        (passes[is_progressive], colors["progressive"], "Progressive pass"),
        (passes[is_line_breaking], colors["line_breaking"], "Line-breaking pass"),
        (passes[is_assist], colors["assist"], "Assist"),
    ]

    for subset, color, label in layers:
        if subset.empty:
            continue
        pitch.arrows(
            subset["location"].str[0],
            subset["location"].str[1],
            subset["pass_end_location"].str[0],
            subset["pass_end_location"].str[1],
            color=color,
            ax=ax,
            label=label,
            **DEFAULT_ARROW_STYLE,
        )

    ax.set_title(_build_title(passes, "Passing Map"))
    ax.legend(loc="upper left", fontsize=9, framealpha=0.9)

    return ax


def plot_shot_map(
    shots: pd.DataFrame,
    goal_color: str = "#2a9d4a",
    default_color: str = "#000000",
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Draw a shot map: arrows from each shot's location to where it ended up
    on a football pitch, colored to highlight goals against every other
    outcome.

    Can be used to map shots by a team or a single player for a given match. Run
    StatsBombClient.get_shots(match_id, team=..., player=...) before
    calling this; same design as plot_passing_map, for the same reason.

    Args:
        shots: DataFrame from StatsBombClient.get_shots, with location,
            shot_end_location, shot_outcome, and period columns.
        goal_color: Arrow color for shots that resulted in a goal.
        default_color: Arrow color for every other shot outcome.
        ax: Optional existing Axes to draw the pitch on. A new pitch
            figure/Axes is created if not provided.

    Returns:
        The Axes the pitch and arrows were drawn on.

    Raises:
        DataNotFoundError: If shots is empty.
    """
    if shots.empty:
        raise DataNotFoundError("shots is empty — nothing to plot.")

    shots = _normalize_attacking_direction(shots, ["location", "shot_end_location"])
    pitch, ax = _draw_pitch(ax)

    is_goal = shots["shot_outcome"] == "Goal"
    for subset, color, label in (
        (shots[~is_goal], default_color, "Shot"),
        (shots[is_goal], goal_color, "Goal"),
    ):
        if subset.empty:
            continue
        pitch.arrows(
            subset["location"].str[0],
            subset["location"].str[1],
            subset["shot_end_location"].str[0],
            subset["shot_end_location"].str[1],
            color=color,
            ax=ax,
            label=label,
            **DEFAULT_ARROW_STYLE,
        )

    ax.set_title(_build_title(shots, "Shot Map"))
    ax.legend(loc="upper left", fontsize=9, framealpha=0.9)

    return ax
