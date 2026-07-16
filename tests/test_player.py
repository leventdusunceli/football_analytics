"""Test for player analytics (player.py) module"""

import pytest

from football_analytics.analytics.player import (
    add_per_90_columns,
    get_top_performers,
    per_90,
)
from football_analytics.utils.exceptions import DataNotFoundError


def test_per_90_correct_calculation():
    # 18 goals in 1800 minutes = 0.9 per 90
    assert per_90(18, 1800) == 0.9


def test_per_90_zero_minutes_returns_zero():
    assert per_90(10, 0) == 0.0


def test_top_performers_correct_count(sample_player_stats):
    result = get_top_performers(sample_player_stats, metric="goals", top_n=3)
    assert len(result) == 3


def test_get_top_performers_sorted_descending(sample_player_stats):
    result = get_top_performers(sample_player_stats, metric="goals")
    assert result["goals"].is_monotonic_decreasing


def test_get_top_performers_invalid_metric_raises_error(sample_player_stats):
    with pytest.raises(DataNotFoundError):
        get_top_performers(sample_player_stats, metric="non_existent_column")


def test_get_top_performers_min_threshold_filter(sample_player_stats):
    # only Palmer has shots >= 80
    result = get_top_performers(
        sample_player_stats,
        metric="goals",
        min_threshold=80,
        threshold_col="shots",
    )
    assert len(result) == 1
    assert result.iloc[0]["player"] == "Palmer"


def test_add_per_90_columns_adds_correct_columns(sample_player_stats):
    result = add_per_90_columns(
        sample_player_stats,
        stat_cols=["goals", "assists"],
        minutes_col="minutes_played",
    )
    assert "goals_per_90" in result.columns
    assert "assists_per_90" in result.columns


def test_add_per_90_columns_correct_value(sample_player_stats):
    result = add_per_90_columns(
        sample_player_stats,
        stat_cols=["goals"],
        minutes_col="minutes_played",
    )
    # Saka: 15 goals in 1800 minutes = 0.75 per 90
    saka_row = result[result["player"] == "Saka"].iloc[0]
    assert saka_row["goals_per_90"] == 0.75


def test_add_per_90_columns_missing_minutes_raises_error(sample_player_stats):
    with pytest.raises(DataNotFoundError):
        add_per_90_columns(
            sample_player_stats,
            stat_cols=["goals"],
            minutes_col="non_existent_col",
        )
