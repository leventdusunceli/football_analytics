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


def test_plot_defensive_profile_higher_stat_gets_longer_bar(sample_defensive_stats):
    """Regression test: for a "higher is better" stat, the player with the
    higher per-match rate must get the longer (more favorable) bar."""
    ax = plot_defensive_profile(sample_defensive_stats)
    # tackles is the first stat row (y offsets near 0); Busquets' bar
    # (2 tackles/match) must be longer than Kante's (1 tackle/match).
    tackle_bars = sorted(
        (p for p in ax.patches if -0.5 <= p.get_y() <= 0.5), key=lambda p: p.get_y()
    )
    assert len(tackle_bars) == 2
    busquets_bar, kante_bar = tackle_bars
    assert busquets_bar.get_width() > kante_bar.get_width()
    plt.close(ax.figure)


def test_plot_defensive_profile_lower_is_better_flips_favorability(
    sample_defensive_stats,
):
    """Regression test: for a "lower is better" stat (fouls_committed),
    the player with the LOWER per-match rate must get the longer
    (more favorable) bar — a naive percentile-of-raw-value bar would get
    this backwards."""
    ax = plot_defensive_profile(sample_defensive_stats)
    # fouls_committed is the 7th stat (0-indexed row 6).
    fouls_bars = sorted(
        (p for p in ax.patches if 5.5 <= p.get_y() <= 6.5), key=lambda p: p.get_y()
    )
    assert len(fouls_bars) == 2
    busquets_bar, kante_bar = fouls_bars
    # Busquets commits fewer fouls/match (10/30) than Kante (20/30), so
    # Busquets' bar should be the more favorable (longer) one.
    assert busquets_bar.get_width() > kante_bar.get_width()
    plt.close(ax.figure)


def test_plot_defensive_profile_bar_lengths_are_percentiles(sample_defensive_stats):
    """With exactly 2 players, favorability bars must be 0.0/1.0 (or a
    0.5/0.5 tie) — anything else means the percentile math is off."""
    ax = plot_defensive_profile(sample_defensive_stats)
    widths = {round(p.get_width(), 3) for p in ax.patches}
    assert widths <= {0.0, 0.5, 1.0}
    plt.close(ax.figure)
