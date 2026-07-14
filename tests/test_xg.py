"""Tests for xG analytics module xg.py"""

import pandas as pd
import pytest

from football_analytics.analytics.xg import (
    get_match_xg_summary,
    get_player_xg_ranking,
    get_xg_overperformance,
)
from football_analytics.utils.exceptions import DataNotFoundError


def test_get_match_xg_summary_return_two_team(sample_shots):
    result = get_match_xg_summary(sample_shots)
    assert len(result) == 2


def test_get_match_xg_summary_columns(sample_shots):
    result = get_match_xg_summary(sample_shots)
    expected_cols = {
        "team",
        "total_shots",
        "shots_on_target",
        "total_xg",
        "goals",
        "xg_difference",
    }
    assert set(result.columns) == expected_cols


def test_get_match_xg_summary_goal_number(sample_shots):
    result = get_match_xg_summary(sample_shots)
    arsenal = result[result["team"] == "Arsenal"].iloc[0]
    assert arsenal["goals"] == 2


def test_get_match_xg_summary_shotsontarget(sample_shots):
    result = get_match_xg_summary(sample_shots)
    chelsea_sot = result[result["team"] == "Chelsea"].iloc[0]
    assert chelsea_sot["shots_on_target"] == 1


def test_get_match_xg_summary_DataNotFoundError():
    with pytest.raises(DataNotFoundError):
        get_match_xg_summary(pd.DataFrame())


def test_get_player_xg_ranking_sorted_descending(sample_shots):
    result = get_player_xg_ranking(sample_shots)
    assert result["total_xg"].is_monotonic_decreasing


def test_get_player_xg_ranking_empty_df_raises_error():
    with pytest.raises(DataNotFoundError):
        get_player_xg_ranking(pd.DataFrame())


def test_get_xg_overperformance_empty_df_raises_error():
    with pytest.raises(DataNotFoundError):
        get_xg_overperformance(pd.DataFrame())


def test_get_xg_overperformance_min_shots_filter(sample_shots):
    # sample_shots has max 3 shots per player so min_shots=10 should
    # raise DataNotFoundError
    with pytest.raises(DataNotFoundError):
        get_xg_overperformance(sample_shots, min_shots=10)


def test_get_xg_overperformance_sorted_descending(sample_shots):
    result = get_xg_overperformance(sample_shots, min_shots=1)
    assert result["xg_difference"].is_monotonic_decreasing
