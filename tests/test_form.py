"""Tests for form.py module"""

import pytest
from football_analytics.analytics.form import (get_recent_form,
                                            get_points_per_game,
                                            get_home_away_split)

from football_analytics.utils.exceptions import DataNotFoundError

def test_get_recent_form_returns_correct_number_of_matches(sample_raw_matches):
    result = get_recent_form(sample_raw_matches, team="Arsenal", last_n=2)
    assert len(result) == 2


def test_get_recent_form_correct_results(sample_raw_matches):
    result = get_recent_form(sample_raw_matches, team="Arsenal", last_n=3)
    assert list(result["result"]) == ["W", "D", "L"]


def test_get_recent_form_correct_points(sample_raw_matches):
    result = get_recent_form(sample_raw_matches, team="Arsenal", last_n=3)
    assert list(result["points"]) == [3, 1, 0]

def test_get_recent_form_unknown_team_raises_error(sample_raw_matches):
    with pytest.raises(DataNotFoundError):
        get_recent_form(sample_raw_matches, team="Tottenham")


def test_get_points_per_game_correct_value(sample_raw_matches):
    # Arsenal: W=3pts, D=1pt, L=0pts → mean = 4/3 = 1.33
    result = get_points_per_game(sample_raw_matches, team="Arsenal")
    assert result == 1.33


def test_get_points_per_game_unknown_team_raises_error(sample_raw_matches):
    with pytest.raises(DataNotFoundError):
        get_points_per_game(sample_raw_matches, team="Tottenham")


def test_get_home_away_split_returns_two_rows(sample_raw_matches):
    result = get_home_away_split(sample_raw_matches, team="Arsenal")
    assert len(result) == 2


def test_get_home_away_split_venues(sample_raw_matches):
    result = get_home_away_split(sample_raw_matches, team="Arsenal")
    assert set(result["venue"]) == {"home", "away"}


def test_get_home_away_split_unknown_team_raises_error(sample_raw_matches):
    with pytest.raises(DataNotFoundError):
        get_home_away_split(sample_raw_matches, team="Tottenham")
        