"""
Player-level season visualizations.
Charts built from StatsBombClient season-stat DataFrames.
"""

import matplotlib.pyplot as plt
import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError

# default multiplier turning a small metric (e.g. xg_per_shot ~ 0.1-0.3)
# into a visible bubble area in points^2 — tune per-metric if needed.
DEFAULT_BUBBLE_SCALE = 4000

# assists are small integer counts (single digits to low tens), so they need
# a much smaller multiplier than a fractional metric like xg_per_shot.
DEFAULT_ASSISTS_BUBBLE_SCALE = 40


def _resolve_labels_and_colors(
    stats: pd.DataFrame,
    player_labels: dict[str, str] | None,
    player_colors: dict[str, str] | None,
) -> tuple[pd.Series, dict[str, str]]:
    """
    Internal helper shared by every plotting function in this module:
    relabels players for display and assigns each a consistent color,
    falling back to matplotlib's default color cycle for any player
    without an explicit color.

    Args:
        stats: A season-stat DataFrame containing a 'player' column.
        player_labels: Optional mapping of full StatsBomb player names to
            short display labels.
        player_colors: Optional mapping of (possibly relabeled) player name
            to a matplotlib color.

    Returns:
        Tuple of (display labels aligned to stats' row order, a dict
        mapping every unique label to a color).
    """
    labels = stats["player"].map(player_labels) if player_labels else stats["player"]
    labels = labels.fillna(stats["player"]) if player_labels else labels

    unique_labels = labels.unique()
    colors = {
        label: (player_colors or {}).get(label, f"C{i}")
        for i, label in enumerate(unique_labels)
    }
    return labels, colors


def plot_shooting_profile(
    shooting_stats: pd.DataFrame,
    player_labels: dict[str, str] | None = None,
    player_colors: dict[str, str] | None = None,
    bubble_scale: float = DEFAULT_BUBBLE_SCALE,
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Bubble chart of season shooting output: shots vs. goals, with bubble
    size representing xG per shot.

    Args:
        shooting_stats: DataFrame returned by
            StatsBombClient.get_player_shooting_season.
        player_labels: Optional mapping of full StatsBomb player names to
            short display labels (e.g. {"Lionel Andrés Messi Cuccittini":
            "Messi"}). Purely cosmetic — the input DataFrame is not mutated.
        player_colors: Optional mapping of (possibly relabeled) player name
            to a matplotlib color. Any player not present in the mapping
            falls back to matplotlib's default color cycle.
        bubble_scale: Multiplier applied to xg_per_shot to get bubble area
            in points^2. Tune this if bubbles are too small/large for your
            data's xg_per_shot range.
        ax: Optional existing Axes to draw on, so this can be composed into
            subplots. A new figure/Axes is created if not provided.

    Returns:
        The Axes the chart was drawn on.

    Raises:
        DataNotFoundError: If shooting_stats is empty.
    """
    if shooting_stats.empty:
        raise DataNotFoundError("shooting_stats is empty — nothing to plot.")

    if ax is None:
        _, ax = plt.subplots(figsize=(6, 5))

    labels, colors = _resolve_labels_and_colors(
        shooting_stats, player_labels, player_colors
    )
    # season_ids is often a single season, but get_player_shooting_season
    # can span several — disambiguate annotations if more than one is present.
    multi_season = (
        "season_name" in shooting_stats.columns
        and shooting_stats["season_name"].nunique() > 1
    )

    for (_, row), label in zip(shooting_stats.iterrows(), labels, strict=True):
        ax.scatter(
            row["shots"],
            row["goals"],
            s=row["xg_per_shot"] * bubble_scale,
            color=colors[label],
            alpha=0.85,
            edgecolors="white",
            linewidths=1.5,
            zorder=3,
        )
        label_text = f"{label} ({row['season_name']})" if multi_season else label
        ax.annotate(
            f"{label_text}\n{row['xg_per_shot']:.2f} xG/shot",
            (row["shots"], row["goals"]),
            xytext=(12, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
        )

    title = "Shooting Profile — Bubble Size = xG per Shot"
    if "season_name" in shooting_stats.columns:
        seasons = ", ".join(sorted(shooting_stats["season_name"].unique()))
        title += f"\n{seasons}"

    ax.margins(0.25)
    ax.set_xlabel("Shots (season total)")
    ax.set_ylabel("Goals (season total)")
    ax.set_title(title)
    ax.grid(True, alpha=0.25, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    return ax


def _plot_passing_bubble_chart(
    passing_stats: pd.DataFrame,
    x_col: str,
    x_label: str,
    chart_title: str,
    player_labels: dict[str, str] | None,
    player_colors: dict[str, str] | None,
    bubble_scale: float,
    ax: plt.Axes | None,
) -> plt.Axes:
    """
    Internal helper shared by plot_passing_profile and
    plot_line_breaking_profile — both are the same chart shape (a passing
    volume metric vs. completion rate, bubble size = assists), differing
    only in which column drives the x-axis. Assists come straight from
    passing_stats (StatsBombClient.get_player_passing_season already
    includes them) — no separate goals/assists DataFrame needed.
    """
    if passing_stats.empty:
        raise DataNotFoundError("passing_stats is empty — nothing to plot.")

    if ax is None:
        _, ax = plt.subplots(figsize=(6, 5))

    labels, colors = _resolve_labels_and_colors(
        passing_stats, player_labels, player_colors
    )
    multi_season = (
        "season_name" in passing_stats.columns
        and passing_stats["season_name"].nunique() > 1
    )

    for (_, row), label in zip(passing_stats.iterrows(), labels, strict=True):
        ax.scatter(
            row[x_col],
            row["completion_rate"],
            s=row["assists"] * bubble_scale,
            color=colors[label],
            alpha=0.85,
            edgecolors="white",
            linewidths=1.5,
            zorder=3,
        )
        label_text = f"{label} ({row['season_name']})" if multi_season else label
        ax.annotate(
            f"{label_text}\n{row['assists']} assists",
            (row[x_col], row["completion_rate"]),
            xytext=(12, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
        )

    title = chart_title
    if "season_name" in passing_stats.columns:
        seasons = ", ".join(sorted(passing_stats["season_name"].unique()))
        title += f"\n{seasons}"

    ax.margins(0.25)
    ax.set_xlabel(x_label)
    ax.set_ylabel("Pass Completion Rate (%)")
    ax.set_title(title)
    ax.grid(True, alpha=0.25, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    return ax


def plot_passing_profile(
    passing_stats: pd.DataFrame,
    player_labels: dict[str, str] | None = None,
    player_colors: dict[str, str] | None = None,
    bubble_scale: float = DEFAULT_ASSISTS_BUBBLE_SCALE,
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Bubble chart of progressive passing volume vs. completion rate, with
    bubble size representing assists. Surfaces whether a player's passing
    risk (how often they progress the ball upfield) comes at the cost of
    accuracy, and whether that profile actually converts into created
    goals.

    Args:
        passing_stats: DataFrame from
            StatsBombClient.get_player_passing_season.
        player_labels: Optional mapping of full StatsBomb player names to
            short display labels. Purely cosmetic.
        player_colors: Optional mapping of (possibly relabeled) player
            name to a matplotlib color. Falls back to the default color
            cycle for any player without an explicit color.
        bubble_scale: Multiplier applied to assists to get bubble area in
            points^2.
        ax: Optional existing Axes to draw on, so this can be composed
            into subplots. A new figure/Axes is created if not provided.

    Returns:
        The Axes the chart was drawn on.

    Raises:
        DataNotFoundError: If passing_stats is empty.
    """
    return _plot_passing_bubble_chart(
        passing_stats,
        x_col="progressive_passes",
        x_label="Progressive Passes (season total)",
        chart_title=(
            "Passing Profile — Progression vs. Accuracy — Bubble Size = Assists"
        ),
        player_labels=player_labels,
        player_colors=player_colors,
        bubble_scale=bubble_scale,
        ax=ax,
    )


def plot_line_breaking_profile(
    passing_stats: pd.DataFrame,
    player_labels: dict[str, str] | None = None,
    player_colors: dict[str, str] | None = None,
    bubble_scale: float = DEFAULT_ASSISTS_BUBBLE_SCALE,
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Bubble chart of line-breaking passes (StatsBomb's pass_through_ball)
    vs. completion rate, with bubble size representing assists.

    line_breaking_passes are a much rarer event than progressive_passes
    (single digits to low tens per season vs. hundreds), so this tends to
    read as "chance-creation precision" rather than the passing-volume
    story told by plot_passing_profile.

    Args:
        passing_stats: DataFrame from
            StatsBombClient.get_player_passing_season.
        player_labels: Optional mapping of full StatsBomb player names to
            short display labels. Purely cosmetic.
        player_colors: Optional mapping of (possibly relabeled) player
            name to a matplotlib color. Falls back to the default color
            cycle for any player without an explicit color.
        bubble_scale: Multiplier applied to assists to get bubble area in
            points^2.
        ax: Optional existing Axes to draw on, so this can be composed
            into subplots. A new figure/Axes is created if not provided.

    Returns:
        The Axes the chart was drawn on.

    Raises:
        DataNotFoundError: If passing_stats is empty.
    """
    return _plot_passing_bubble_chart(
        passing_stats,
        x_col="line_breaking_passes",
        x_label="Line-Breaking Passes (season total)",
        chart_title=(
            "Passing Profile — Line-Breaking vs. Accuracy — Bubble Size = Assists"
        ),
        player_labels=player_labels,
        player_colors=player_colors,
        bubble_scale=bubble_scale,
        ax=ax,
    )
