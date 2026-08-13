"""Tests for player-level visualizations (visualization/player.py)."""

import matplotlib

matplotlib.use("Agg")  # headless backend, no display needed for tests

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from football_analytics.utils.exceptions import DataNotFoundError
from football_analytics.visualization.player import (
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
