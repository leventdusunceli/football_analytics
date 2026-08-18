"""Tests for match-level visualizations (visualization/match.py)."""

import matplotlib

matplotlib.use("Agg")  # headless backend, no display needed for tests

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from football_analytics.utils.exceptions import DataNotFoundError
from football_analytics.visualization.match import plot_passing_map, plot_shot_map


@pytest.fixture
def sample_passes():
    """Fake DataFrame mimicking StatsBombClient.get_passes() output: one
    plain pass, one progressive pass, one line-breaking pass (all
    Odegaard), and one assist (Saka, in period 2 to exercise direction
    normalization)."""
    return pd.DataFrame(
        {
            "player": ["Odegaard", "Odegaard", "Odegaard", "Saka"],
            "team": ["Arsenal", "Arsenal", "Arsenal", "Arsenal"],
            "position": [
                "Center Midfield",
                "Center Midfield",
                "Center Midfield",
                "Right Wing",
            ],
            "minute": [10, 20, 30, 55],
            "period": [1, 1, 1, 2],
            "location": [
                [60.0, 40.0],
                [60.0, 40.0],
                [60.0, 40.0],
                [30.0, 20.0],
            ],
            "pass_end_location": [
                [65.0, 42.0],
                [95.0, 40.0],
                [65.0, 45.0],
                [50.0, 30.0],
            ],
            "pass_outcome": [None, None, None, None],
            "pass_through_ball": [None, None, True, None],
            "pass_goal_assist": [None, None, None, True],
            "is_progressive": [False, True, False, False],
        }
    )


@pytest.fixture
def sample_shots():
    """Fake DataFrame mimicking StatsBombClient.get_shots() output."""
    return pd.DataFrame(
        {
            "player": ["Saka", "Havertz"],
            "team": ["Arsenal", "Arsenal"],
            "position": ["Right Wing", "Center Forward"],
            "minute": [30, 60],
            "period": [1, 2],
            "location": [[100.0, 40.0], [95.0, 42.0]],
            "shot_end_location": [[120.0, 40.0], [118.0, 36.0]],
            "shot_statsbomb_xg": [0.3, 0.15],
            "shot_outcome": ["Goal", "Saved"],
        }
    )


def test_plot_passing_map_returns_axes(sample_passes):
    ax = plot_passing_map(sample_passes)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_passing_map_empty_df_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_passing_map(pd.DataFrame())


def test_plot_passing_map_uses_provided_axes(sample_passes):
    _, ax = plt.subplots()
    returned_ax = plot_passing_map(sample_passes, ax=ax)
    assert returned_ax is ax
    plt.close(ax.figure)


def test_plot_passing_map_title_uses_single_player(sample_passes):
    single_player = sample_passes[sample_passes["player"] == "Odegaard"]
    ax = plot_passing_map(single_player)
    assert "Odegaard" in ax.get_title()
    plt.close(ax.figure)


def test_plot_passing_map_title_uses_single_team(sample_passes):
    ax = plot_passing_map(sample_passes)
    assert "Arsenal" in ax.get_title()
    plt.close(ax.figure)


def test_plot_passing_map_player_level_includes_all_passes_layer(sample_passes):
    """Player-level input: plain 'Pass' layer + progressive + line-breaking
    (no assist for Odegaard in the fixture) = 3 arrow layers."""
    single_player = sample_passes[sample_passes["player"] == "Odegaard"]
    ax = plot_passing_map(single_player)
    labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert "Pass" in labels
    assert len(ax.collections) == 3
    plt.close(ax.figure)


def test_plot_passing_map_team_level_excludes_all_passes_layer(sample_passes):
    """Team-level input: no plain 'Pass' layer, only the three highlighted
    categories (progressive, line-breaking, assist) = 3 arrow layers even
    though there are 4 total passes in the fixture."""
    ax = plot_passing_map(sample_passes)
    labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert "Pass" not in labels
    assert len(ax.collections) == 3
    plt.close(ax.figure)


def test_plot_passing_map_color_override(sample_passes):
    ax = plot_passing_map(sample_passes, colors={"assist": "#ffffff"})
    plt.close(ax.figure)


def test_plot_shot_map_returns_axes(sample_shots):
    ax = plot_shot_map(sample_shots)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_shot_map_empty_df_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_shot_map(pd.DataFrame())
