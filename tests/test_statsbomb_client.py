"""Tests for StatsBombClient."""

from unittest.mock import patch

import pandas as pd
import pytest

from football_analytics.data.statsbomb_client import StatsBombClient
from football_analytics.utils.exceptions import DataNotFoundError


@pytest.fixture
def client():
    """Returns a StatsBombClient instance."""
    return StatsBombClient()


@pytest.fixture
def sample_matches_df():
    """Fake matches DataFrame mimicking statsbombpy output."""
    return pd.DataFrame(
        {
            "match_id": [1, 2, 3],
            "home_team": ["Arsenal", "Barcelona", "Arsenal"],
            "away_team": ["Chelsea", "Arsenal", "Liverpool"],
            "home_score": [2, 1, 0],
            "away_score": [1, 2, 0],
        }
    )


@pytest.fixture
def sample_events_df():
    """Fake events DataFrame mimicking statsbombpy output."""
    return pd.DataFrame(
        {
            "type": ["Shot", "Pass", "Shot", "Tackle", "Shot"],
            "player": ["Saka", "Odegaard", "Havertz", "White", "Palmer"],
            "team": ["Arsenal", "Arsenal", "Arsenal", "Arsenal", "Chelsea"],
            "minute": [12, 23, 45, 67, 88],
            "shot_statsbomb_xg": [0.342, None, 0.521, None, 0.231],
            "shot_outcome": ["Goal", None, "Saved", None, "Missed"],
            "pass_outcome": [None, None, None, None, None],
            "pass_switch": [None, False, None, None, None],
            "pass_goal_assist": [None, False, None, None, None],
        }
    )


# ------------------------------------------------------------------ #
# get_competitions                                                      #
# ------------------------------------------------------------------ #


def test_get_competitions_returns_dataframe(client):
    fake_competitions = pd.DataFrame(
        {
            "competition_id": [11, 16],
            "season_id": [1, 4],
            "competition_name": ["La Liga", "Champions League"],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.competitions",
        return_value=fake_competitions,
    ):
        result = client.get_competitions()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


# ------------------------------------------------------------------ #
# get_matches                                                           #
# ------------------------------------------------------------------ #


def test_get_matches_returns_all_matches(client, sample_matches_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.matches",
        return_value=sample_matches_df,
    ):
        result = client.get_matches(competition_id=11, season_id=1)
        assert len(result) == 3


def test_get_matches_team_filter_returns_correct_matches(client, sample_matches_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.matches",
        return_value=sample_matches_df,
    ):
        result = client.get_matches(competition_id=11, season_id=1, team="Arsenal")
        # Arsenal appear in all 3 matches
        assert len(result) == 3


def test_get_matches_team_filter_unknown_team_raises_error(client, sample_matches_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.matches",
        return_value=sample_matches_df,
    ):
        with pytest.raises(DataNotFoundError):
            client.get_matches(competition_id=11, season_id=1, team="Tottenham")


def test_get_matches_empty_response_raises_error(client):
    with patch(
        "football_analytics.data.statsbomb_client.sb.matches",
        return_value=pd.DataFrame(),
    ):
        with pytest.raises(DataNotFoundError):
            client.get_matches(competition_id=99, season_id=99)


# ------------------------------------------------------------------ #
# get_events                                                            #
# ------------------------------------------------------------------ #


def test_get_events_returns_dataframe(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_events(match_id=1)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5


def test_get_events_empty_response_raises_error(client):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=pd.DataFrame(),
    ):
        with pytest.raises(DataNotFoundError):
            client.get_events(match_id=99)


# ------------------------------------------------------------------ #
# get_shots                                                             #
# ------------------------------------------------------------------ #


def test_get_shots_returns_only_shots(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_shots(match_id=1)
        # sample_events_df has 3 shot events
        assert len(result) == 3


def test_get_shots_correct_columns(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_shots(match_id=1)
        expected_cols = {
            "player",
            "team",
            "minute",
            "shot_statsbomb_xg",
            "shot_outcome",
        }
        assert expected_cols.issubset(set(result.columns))


def test_get_shots_no_shots_raises_error(client):
    """Test that a match with no shot events raises DataNotFoundError."""
    no_shots_df = pd.DataFrame(
        {
            "type": ["Pass", "Tackle"],
            "player": ["Odegaard", "White"],
            "team": ["Arsenal", "Arsenal"],
            "minute": [10, 20],
            "shot_statsbomb_xg": [None, None],
            "shot_outcome": [None, None],
            "pass_outcome": [None, None],
            "pass_switch": [False, None],
            "pass_goal_assist": [False, None],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events", return_value=no_shots_df
    ):
        with pytest.raises(DataNotFoundError):
            client.get_shots(match_id=1)


# ------------------------------------------------------------------ #
# get_player_shooting_match                                            #
# ------------------------------------------------------------------ #


def test_get_player_shooting_match_returns_dataframe(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_player_shooting_match(match_id=1)
        assert isinstance(result, pd.DataFrame)


def test_get_player_shooting_match_correct_columns(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_player_shooting_match(match_id=1)
        expected_cols = {"player", "team", "shots", "goals", "total_xg", "xg_per_shot"}
        assert expected_cols.issubset(set(result.columns))


def test_get_player_shooting_match_passes_match_id(client, sample_events_df):
    """Explicitly verifies match_id is passed through to get_shots."""
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ) as mock_events:
        client.get_player_shooting_match(match_id=42)
        mock_events.assert_called_once_with(match_id=42)


# ------------------------------------------------------------------ #
# get_player_passing_match                                             #
# ------------------------------------------------------------------ #


def test_get_player_passing_match_returns_dataframe(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_player_passing_match(match_id=1)
        assert isinstance(result, pd.DataFrame)


def test_get_player_passing_match_passes_match_id(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ) as mock_events:
        client.get_player_passing_match(match_id=42)
        mock_events.assert_called_once_with(match_id=42)


# ------------------------------------------------------------------ #
# get_player_defensive_match                                           #
# ------------------------------------------------------------------ #


def test_get_player_defensive_match_returns_dataframe(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_player_defensive_match(match_id=1)
        assert isinstance(result, pd.DataFrame)


def test_get_player_defensive_match_passes_match_id(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ) as mock_events:
        client.get_player_defensive_match(match_id=42)
        mock_events.assert_called_once_with(match_id=42)


# ------------------------------------------------------------------ #
# get_player_goals_assists_match                                        #
# ------------------------------------------------------------------ #


def test_get_player_goals_assists_match_returns_dataframe(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_player_goals_assists_match(match_id=1)
        assert isinstance(result, pd.DataFrame)


def test_get_player_goals_assists_match_passes_match_id(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ) as mock_events:
        client.get_player_goals_assists_match(match_id=42)
        mock_events.assert_called_with(match_id=42)
