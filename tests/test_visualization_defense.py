"""Tests for defensive-action visualizations (visualization/defense.py)."""

import matplotlib

matplotlib.use("Agg")  # headless backend, no display needed for tests

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from football_analytics.utils.exceptions import DataNotFoundError
from football_analytics.visualization.defense import plot_defensive_actions_map


@pytest.fixture
def sample_defensive_events():
    """Fake DataFrame mimicking StatsBombClient.get_defensive_events_match
    output: one tackle, one interception, both from the same team."""
    return pd.DataFrame(
        {
            "player": ["White", "Saliba"],
            "team": ["Arsenal", "Arsenal"],
            "position": ["Right Back", "Center Back"],
            "minute": [10, 20],
            "period": [1, 1],
            "location": [[30.0, 40.0], [25.0, 35.0]],
            "action_type": ["tackle", "interception"],
        }
    )


def test_plot_defensive_actions_map_returns_axes(sample_defensive_events):
    ax = plot_defensive_actions_map(sample_defensive_events)
    assert isinstance(ax, plt.Axes)
    plt.close(ax.figure)


def test_plot_defensive_actions_map_one_legend_entry_per_action_type(
    sample_defensive_events,
):
    ax = plot_defensive_actions_map(sample_defensive_events)
    labels = {t.get_text() for t in ax.get_legend().get_texts()}
    assert labels == {"Tackle", "Interception"}
    plt.close(ax.figure)


def test_plot_defensive_actions_map_empty_raises_error():
    with pytest.raises(DataNotFoundError):
        plot_defensive_actions_map(pd.DataFrame())
