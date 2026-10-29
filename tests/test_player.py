"""Tests for player.py analytics module."""

import pandas as pd
import pytest

from football_analytics.analytics.player import get_per_90_stats, get_top_performers
from football_analytics.utils.exceptions import DataNotFoundError


@pytest.fixture
def sample_season_stats():
    """Season-level stats, shaped like get_player_shooting_season output."""
    return pd.DataFrame(
        {
            "player": ["Messi", "Suárez"],
            "team": ["Barcelona", "Barcelona"],
            "season_id": [27, 27],
            "goals": [10, 20],
            "total_xg": [9.0, 18.0],
        }
    )


@pytest.fixture
def sample_minutes():
    """Minutes-played, shaped like get_player_minutes_season output."""
    return pd.DataFrame(
        {
            "player": ["Messi", "Suárez"],
            "team": ["Barcelona", "Barcelona"],
            "season_id": [27, 27],
            "matches_played": [10, 20],
            "minutes_played": [900.0, 1800.0],
        }
    )


def test_get_per_90_stats_correct_values(sample_season_stats, sample_minutes):
    result = get_per_90_stats(sample_season_stats, sample_minutes, ["goals"])
    messi = result[result["player"] == "Messi"].iloc[0]
    suarez = result[result["player"] == "Suárez"].iloc[0]
    assert messi["goals_per90"] == 1.0
    assert suarez["goals_per90"] == 1.0


def test_get_per_90_stats_multiple_stat_cols(sample_season_stats, sample_minutes):
    result = get_per_90_stats(
        sample_season_stats, sample_minutes, ["goals", "total_xg"]
    )
    assert {"goals_per90", "total_xg_per90"}.issubset(result.columns)


def test_get_per_90_stats_empty_raises_error(sample_minutes):
    with pytest.raises(DataNotFoundError):
        get_per_90_stats(pd.DataFrame(), sample_minutes, ["goals"])


def test_get_per_90_stats_uses_minutes_matches_played(sample_minutes):
    """A stat table's own matches_played only counts matches where that
    stat occurred (e.g. a shooting table has no row for a shotless match),
    so it must not survive the merge alongside minutes' full-season
    matches_played, which is the correct per-90 denominator context."""
    season_stats = pd.DataFrame(
        {
            "player": ["Messi"],
            "team": ["Barcelona"],
            "season_id": [27],
            "matches_played": [1],  # only matches with a recorded goal
            "goals": [1],
        }
    )
    minutes_full_season = pd.DataFrame(
        {
            "player": ["Messi"],
            "team": ["Barcelona"],
            "season_id": [27],
            "matches_played": [10],  # every match actually played
            "minutes_played": [900.0],
        }
    )
    result = get_per_90_stats(season_stats, minutes_full_season, ["goals"])
    assert result.iloc[0]["matches_played"] == 10


def test_get_per_90_stats_no_matching_rows_raises_error(sample_season_stats):
    unrelated_minutes = pd.DataFrame(
        {
            "player": ["Someone Else"],
            "team": ["Other Team"],
            "season_id": [99],
            "matches_played": [5],
            "minutes_played": [500.0],
        }
    )
    with pytest.raises(DataNotFoundError):
        get_per_90_stats(sample_season_stats, unrelated_minutes, ["goals"])


def test_get_top_performers_returns_top_n(sample_season_stats):
    result = get_top_performers(sample_season_stats, "goals", top_n=1)
    assert len(result) == 1
    assert result.iloc[0]["player"] == "Suárez"


def test_get_top_performers_ascending(sample_season_stats):
    result = get_top_performers(sample_season_stats, "goals", top_n=1, ascending=True)
    assert result.iloc[0]["player"] == "Messi"


def test_get_top_performers_empty_raises_error():
    with pytest.raises(DataNotFoundError):
        get_top_performers(pd.DataFrame(), "goals")


def test_get_top_performers_missing_column_raises_error(sample_season_stats):
    with pytest.raises(DataNotFoundError):
        get_top_performers(sample_season_stats, "tackles")
