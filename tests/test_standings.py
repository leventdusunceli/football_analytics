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
