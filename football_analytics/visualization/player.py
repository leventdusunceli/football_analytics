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

# display order + labels for plot_defensive_profile's bars
_DEFENSIVE_STAT_LABELS = {
    "tackles": "Tackles",
    "interceptions": "Interceptions",
    "clearances": "Clearances",
    "blocks": "Blocks",
    "ball_recoveries": "Ball Recoveries",
    "got_dribbled_past": "Dribbled Past",
    "fouls_committed": "Fouls Committed",
    "yellow_cards": "Yellow Cards",
    "red_cards": "Red Cards",
}

# stats where a LOWER per-match rate is the better defensive outcome —
# their bars are drawn from (1 - percentile) so bar length always reads
# as "how favorable", not "how much of this stat".
_LOWER_IS_BETTER = {"got_dribbled_past", "fouls_committed", "yellow_cards", "red_cards"}


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


def plot_defensive_profile(
    defensive_stats: pd.DataFrame,
    players: list[str] | None = None,
    player_labels: dict[str, str] | None = None,
    player_colors: dict[str, str] | None = None,
    ax: plt.Axes | None = None,
) -> plt.Axes:
    """
    Horizontal bar chart of season defensive totals, one bar per stat in
    _DEFENSIVE_STAT_LABELS.

    Args:
        defensive_stats: DataFrame from
            StatsBombClient.get_player_defensive_season.
        players: Optional list of exact player names to draw (bars are
            grouped per stat if more than one — keep this to 2-3 players
            for a readable chart). Defaults to every player in
            defensive_stats.
        player_labels: Optional mapping of full StatsBomb player names to
            short display labels. Purely cosmetic.
        player_colors: Optional mapping of (possibly relabeled) player
            name to a matplotlib color. Falls back to the default color
            cycle for any player without an explicit color.
        ax: Optional existing Axes to draw on. A new figure/Axes is
            created if not provided.

    Returns:
        The Axes the chart was drawn on.

    Raises:
        DataNotFoundError: If defensive_stats is empty, or none of
            `players` are found in it.
    """
    if defensive_stats.empty:
        raise DataNotFoundError("defensive_stats is empty — nothing to plot.")

    subset = (
        defensive_stats
        if players is None
        else defensive_stats[defensive_stats["player"].isin(players)]
    )
    if subset.empty:
        raise DataNotFoundError(f"None of {players} found in defensive_stats.")

    if ax is None:
        _, ax = plt.subplots(figsize=(7, 5))

    stat_cols = list(_DEFENSIVE_STAT_LABELS)
    labels, colors = _resolve_labels_and_colors(subset, player_labels, player_colors)
    n_players = len(subset)
    bar_height = 0.8 / n_players
    y_positions = range(len(stat_cols))

    for i, ((_, row), label) in enumerate(zip(subset.iterrows(), labels, strict=True)):
        offsets = [y - 0.4 + bar_height * (i + 0.5) for y in y_positions]
        ax.barh(
            offsets,
            [row[col] for col in stat_cols],
            height=bar_height,
            color=colors[label],
            label=f"{label} ({row['matches_played']} matches)",
            zorder=3,
        )
        for y, col in zip(offsets, stat_cols, strict=True):
            ax.annotate(
                str(row[col]),
                (row[col], y),
                xytext=(4, 0),
                textcoords="offset points",
                va="center",
                fontsize=8,
            )

    ax.set_yticks(list(y_positions))
    ax.set_yticklabels(
        [
            label + (" (lower better)" if col in _LOWER_IS_BETTER else "")
            for col, label in _DEFENSIVE_STAT_LABELS.items()
        ]
    )
    ax.invert_yaxis()
    ax.margins(x=0.15)
    ax.set_xlabel("Season total")

    title = "Defensive Profile"
    if "season_name" in subset.columns:
        seasons = ", ".join(sorted(subset["season_name"].unique()))
        title += f" — {seasons}"
    ax.set_title(title)

    if n_players > 1:
        ax.legend(loc="lower right", fontsize=9, framealpha=0.9)
    ax.grid(True, axis="x", alpha=0.25, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    return ax
