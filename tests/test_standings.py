"""Tests for standings.py analytics module"""

import pytest

from football_analytics.analytics.standings import (
    get_clean_standings,
    get_expected_vs_actual,
)
from football_analytics.utils.exceptions import DataNotFoundError


def test_get_clean_standings_correct_columns(sample_raw_standings):
    result = get_clean_standings(sample_raw_standings)
    expected_cols = {
        "position",
        "team",
        "played",
        "won",
        "drawn",
        "lost",
        "goals_for",
        "goals_against",
        "goal_difference",
        "points",
    }
    assert set(result.columns) == expected_cols


def test_get_clean_standings_correct_row_count(sample_raw_standings):
    result = get_clean_standings(sample_raw_standings)
    assert len(result) == 2


def test_get_clean_standings_right_standings(sample_raw_standings):
    result = get_clean_standings(sample_raw_standings)
    assert result.iloc[0]["team"] == "Arsenal"


def test_get_clean_standings_invalidData_raises_DataNotFoundError():
    with pytest.raises(DataNotFoundError):
        get_clean_standings({})


def test_get_expected_vs_actual_returns_correct_columns(
    sample_standings_df, sample_team_xg
):  # noqa: E501
    result = get_expected_vs_actual(sample_standings_df, sample_team_xg)
    expected_cols = {"team", "actual_position", "xg_position", "position_difference"}
    assert set(result.columns) == expected_cols


def test_get_expected_vs_actual_no_matching_team_raises_DatanotFoundError(
    sample_standings_df,
):  # noqa: E501
    import pandas as pd

    bad_xg = pd.DataFrame(
        {
            "team": ["Tottenham", "Man United"],
            "total_xg": [30.0, 28.5],
        }
    )
    with pytest.raises(DataNotFoundError):
        get_expected_vs_actual(sample_standings_df, bad_xg)
