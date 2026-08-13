"""Tests for passing analytics module passing.py"""

import pandas as pd
import pytest

from football_analytics.analytics.passing import get_team_passing_summary
from football_analytics.utils.exceptions import DataNotFoundError


def test_get_team_passing_summary_two_teams(sample_player_passing_stats):
    result = get_team_passing_summary(sample_player_passing_stats)
    assert len(result) == 2


def test_get_team_passing_summary_columns(sample_player_passing_stats):
    result = get_team_passing_summary(sample_player_passing_stats)
    expected_cols = {
        "team",
        "passes",
        "passes_completed",
        "completion_rate",
        "progressive_passes",
        "line_breaking_passes",
    }
    assert set(result.columns) == expected_cols


def test_get_team_passing_summary_sums_correctly(sample_player_passing_stats):
    result = get_team_passing_summary(sample_player_passing_stats)
    arsenal = result[result["team"] == "Arsenal"].iloc[0]
    # Saka + Odegaard + Havertz
    assert arsenal["passes"] == 130
    assert arsenal["passes_completed"] == 107
    assert arsenal["progressive_passes"] == 30
    assert arsenal["line_breaking_passes"] == 7


def test_get_team_passing_summary_completion_rate(sample_player_passing_stats):
    result = get_team_passing_summary(sample_player_passing_stats)
    chelsea = result[result["team"] == "Chelsea"].iloc[0]
    assert chelsea["completion_rate"] == 80.0


def test_get_team_passing_summary_empty_raises_error():
    with pytest.raises(DataNotFoundError):
        get_team_passing_summary(pd.DataFrame())
