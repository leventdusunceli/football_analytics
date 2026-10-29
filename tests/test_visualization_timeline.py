"""Tests for timeline visualizations (visualization/timeline.py)."""

import matplotlib

matplotlib.use("Agg")  # headless backend, no display needed for tests

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from football_analytics.utils.exceptions import DataNotFoundError
from football_analytics.visualization.timeline import plot_assist_timeline


@pytest.fixture
def sample_assists():
    """Fake DataFrame mimicking StatsBombClient.get_passes output,
    pre-filtered to assists, for two teams."""
    return pd.DataFrame(
        {
            "player": ["Odegaard", "Enzo"],
            "team": ["Arsenal", "Chelsea"],
            "minute": [23, 67],
        }
    )


def test_plot_assist_timeline_returns_axes(sample_assists):
    ax = plot_assist_timeline(sample_assists)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_assist_timeline_one_row_per_team(sample_assists):
    ax = plot_assist_timeline(sample_assists)
    assert [label.get_text() for label in ax.get_yticklabels()] == [
        "Arsenal",
        "Chelsea",
    ]
    plt.close(ax.figure)


def test_plot_assist_timeline_empty_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_assist_timeline(pd.DataFrame())
