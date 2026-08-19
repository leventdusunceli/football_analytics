"""Tests for player-level visualizations (visualization/player.py)."""

import matplotlib

matplotlib.use("Agg")  # headless backend, no display needed for tests

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from football_analytics.utils.exceptions import DataNotFoundError
from football_analytics.visualization.player import (
    plot_defensive_profile,
    plot_line_breaking_profile,
    plot_passing_profile,
    plot_shooting_profile,
)


@pytest.fixture
def sample_shooting_stats():
    """Fake DataFrame mimicking get_player_shooting_season() output."""
    return pd.DataFrame(
        {
            "player": ["Messi", "Ronaldo"],
            "team": ["Barcelona", "Real Madrid"],
            "season_id": [27, 27],
            "season_name": ["2015/2016", "2015/2016"],
            "matches_played": [33, 36],
            "position": ["Right Wing", "Left Wing"],
            "shots": [158, 228],
            "shots_on_target": [74, 96],
            "goals": [26, 35],
            "total_xg": [21.264, 31.966],
            "xg_per_shot": [0.135, 0.140],
        }
    )


@pytest.fixture
def sample_passing_stats():
    """Fake DataFrame mimicking get_player_passing_season() output."""
    return pd.DataFrame(
        {
            "player": ["Messi", "Ronaldo"],
            "team": ["Barcelona", "Real Madrid"],
            "season_id": [27, 27],
            "season_name": ["2015/2016", "2015/2016"],
            "matches_played": [33, 36],
            "position": ["Right Wing", "Left Wing"],
            "passes": [1926, 1146],
            "passes_completed": [1544, 878],
            "completion_rate": [80.2, 76.6],
            "progressive_passes": [512, 170],
            "line_breaking_passes": [107, 6],
            "assists": [17, 8],
        }
    )


def test_plot_shooting_profile_returns_axes(sample_shooting_stats):
    ax = plot_shooting_profile(sample_shooting_stats)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_shooting_profile_one_bubble_per_row(sample_shooting_stats):
    ax = plot_shooting_profile(sample_shooting_stats)
    assert len(ax.collections) == len(sample_shooting_stats)
    plt.close(ax.figure)


def test_plot_shooting_profile_empty_df_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_shooting_profile(pd.DataFrame())


def test_plot_shooting_profile_uses_provided_axes(sample_shooting_stats):
    _, ax = plt.subplots()
    returned_ax = plot_shooting_profile(sample_shooting_stats, ax=ax)
    assert returned_ax is ax
    plt.close(ax.figure)


def test_plot_shooting_profile_relabels_players(sample_shooting_stats):
    full_names = sample_shooting_stats.copy()
    full_names["player"] = ["Lionel Messi Cuccittini", "Cristiano Ronaldo Aveiro"]

    ax = plot_shooting_profile(
        full_names,
        player_labels={
            "Lionel Messi Cuccittini": "Messi",
            "Cristiano Ronaldo Aveiro": "Ronaldo",
        },
    )
    annotation_text = " ".join(t.get_text() for t in ax.texts)
    assert "Messi" in annotation_text
    assert "Ronaldo" in annotation_text
    assert "Cuccittini" not in annotation_text
    plt.close(ax.figure)


def test_plot_passing_profile_returns_axes(sample_passing_stats):
    ax = plot_passing_profile(sample_passing_stats)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_passing_profile_one_bubble_per_row(sample_passing_stats):
    ax = plot_passing_profile(sample_passing_stats)
    assert len(ax.collections) == len(sample_passing_stats)
    plt.close(ax.figure)


def test_plot_passing_profile_x_axis_is_progressive_passes(sample_passing_stats):
    ax = plot_passing_profile(sample_passing_stats)
    plotted_x = sorted(c.get_offsets()[0][0] for c in ax.collections)
    assert plotted_x == sorted(sample_passing_stats["progressive_passes"].tolist())
    plt.close(ax.figure)


def test_plot_passing_profile_empty_passing_stats_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_passing_profile(pd.DataFrame())


def test_plot_line_breaking_profile_x_axis_is_line_breaking_passes(
    sample_passing_stats,
):
    ax = plot_line_breaking_profile(sample_passing_stats)
    plotted_x = sorted(c.get_offsets()[0][0] for c in ax.collections)
    assert plotted_x == sorted(sample_passing_stats["line_breaking_passes"].tolist())
    plt.close(ax.figure)


@pytest.fixture
def sample_defensive_stats():
    """Fake DataFrame mimicking get_player_defensive_season() output.
    Busquets outpaces Kante on every "higher is better" stat and trails
    on every "lower is better" one — an unambiguous ranking to assert on."""
    return pd.DataFrame(
        {
            "player": ["Busquets", "Kante"],
            "team": ["Barcelona", "Chelsea"],
            "season_id": [27, 27],
            "season_name": ["2015/2016", "2015/2016"],
            "matches_played": [30, 30],
            "position": ["Center Defensive Midfield", "Center Defensive Midfield"],
            "tackles": [60, 30],
            "interceptions": [40, 20],
            "clearances": [20, 10],
            "blocks": [10, 5],
            "ball_recoveries": [150, 100],
            "got_dribbled_past": [5, 15],
            "fouls_committed": [10, 20],
            "yellow_cards": [2, 6],
            "red_cards": [0, 1],
        }
    )


def test_plot_defensive_profile_returns_axes(sample_defensive_stats):
    ax = plot_defensive_profile(sample_defensive_stats)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_defensive_profile_empty_stats_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_defensive_profile(pd.DataFrame())


def test_plot_defensive_profile_one_bar_group_per_stat(sample_defensive_stats):
    """9 stats x 2 players = 18 bars."""
    ax = plot_defensive_profile(sample_defensive_stats)
    assert len(ax.patches) == 18
    plt.close(ax.figure)


def test_plot_defensive_profile_players_filter(sample_defensive_stats):
    ax = plot_defensive_profile(sample_defensive_stats, players=["Busquets"])
    assert len(ax.patches) == 9
    plt.close(ax.figure)


def test_plot_defensive_profile_unknown_player_raises_error(sample_defensive_stats):
    with pytest.raises(DataNotFoundError):
        plot_defensive_profile(sample_defensive_stats, players=["Nobody"])


def test_plot_defensive_profile_bar_widths_are_raw_season_totals(
    sample_defensive_stats,
):
    """Regression test: bar length must be the raw season total, not a
    per-match rate or percentile — Busquets' 60 tackles must plot as 60,
    not 2.0 (60/30 matches) or a 0-1 percentile."""
    ax = plot_defensive_profile(sample_defensive_stats)
    # tackles is the first stat row (y offsets near 0).
    tackle_bars = sorted(
        (p for p in ax.patches if -0.5 <= p.get_y() <= 0.5), key=lambda p: p.get_y()
    )
    assert len(tackle_bars) == 2
    busquets_bar, kante_bar = tackle_bars
    assert busquets_bar.get_width() == 60
    assert kante_bar.get_width() == 30
    plt.close(ax.figure)


def test_plot_defensive_profile_lower_is_better_not_inverted(sample_defensive_stats):
    """Regression test: "lower is better" stats (e.g. fouls_committed)
    must still plot their raw total as-is, not an inverted/flipped value
    — flipping was a percentile-only concern that no longer applies."""
    ax = plot_defensive_profile(sample_defensive_stats)
    # fouls_committed is the 7th stat (0-indexed row 6).
    fouls_bars = sorted(
        (p for p in ax.patches if 5.5 <= p.get_y() <= 6.5), key=lambda p: p.get_y()
    )
    assert len(fouls_bars) == 2
    busquets_bar, kante_bar = fouls_bars
    assert busquets_bar.get_width() == 10
    assert kante_bar.get_width() == 20
    plt.close(ax.figure)


def test_plot_defensive_profile_legend_includes_matches_played(
    sample_defensive_stats,
):
    ax = plot_defensive_profile(sample_defensive_stats)
    legend_labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert "Busquets (30 matches)" in legend_labels
    assert "Kante (30 matches)" in legend_labels
    plt.close(ax.figure)
