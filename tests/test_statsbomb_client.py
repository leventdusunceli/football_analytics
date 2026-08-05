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
            "match_week": [1, 2, 3],
        }
    )


@pytest.fixture
def sample_events_df():
    """Fake events DataFrame mimicking statsbombpy output."""
    return pd.DataFrame(
        {
            "type": ["Shot", "Pass", "Shot", "Tackle", "Shot"],
            "player": ["Saka", "Odegaard", "Havertz", "White", "Palmer"],
            "position": [
                "Right Wing",
                "Center Midfield",
                "Center Forward",
                "Right Back",
                "Center Forward",
            ],
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
# Season-level fixtures                                                #
# ------------------------------------------------------------------ #


@pytest.fixture
def sample_competitions_df():
    """Fake competitions DataFrame, used to resolve season_name."""
    return pd.DataFrame(
        {
            "competition_id": [11, 11, 16],
            "season_id": [1, 2, 4],
            "competition_name": ["La Liga", "La Liga", "Champions League"],
            "season_name": ["2020/21", "2021/22", "2021/22"],
        }
    )


@pytest.fixture
def multi_season_matches():
    """Matches DataFrame per season_id, for get_matches' sb.matches side_effect."""
    season_1 = pd.DataFrame(
        {
            "match_id": [101, 102],
            "home_team": ["Arsenal", "Chelsea"],
            "away_team": ["Chelsea", "Arsenal"],
            "home_score": [2, 1],
            "away_score": [1, 1],
            "match_week": [1, 2],
        }
    )
    season_2 = pd.DataFrame(
        {
            "match_id": [201],
            "home_team": ["Arsenal"],
            "away_team": ["Liverpool"],
            "home_score": [3],
            "away_score": [0],
            "match_week": [1],
        }
    )
    return {1: season_1, 2: season_2}


@pytest.fixture
def multi_season_events():
    """sb.events per match_id across two fake seasons. Saka (Arsenal) shoots
    in every match; Havertz (Chelsea) also shoots in match 101 so team
    filtering has something real to exclude."""

    def _shot_row(player, team, xg, outcome, minute=10):
        return {
            "type": "Shot",
            "player": player,
            "position": "Right Wing",
            "team": team,
            "minute": minute,
            "shot_statsbomb_xg": xg,
            "shot_outcome": outcome,
            "pass_outcome": None,
            "pass_switch": None,
            "pass_goal_assist": None,
        }

    return {
        101: pd.DataFrame(
            [
                _shot_row("Saka", "Arsenal", 0.3, "Goal"),
                _shot_row("Havertz", "Chelsea", 0.1, "Missed"),
            ]
        ),
        102: pd.DataFrame([_shot_row("Saka", "Arsenal", 0.2, "Saved")]),
        201: pd.DataFrame([_shot_row("Saka", "Arsenal", 0.5, "Goal")]),
    }


@pytest.fixture
def multi_season_lineups():
    """sb.lineups per match_id — dict keyed by team name, as statsbombpy returns."""
    arsenal_lineup = pd.DataFrame({"player_name": ["Saka", "Odegaard"]})
    chelsea_lineup = pd.DataFrame({"player_name": ["Havertz"]})
    liverpool_lineup = pd.DataFrame({"player_name": ["Salah"]})
    return {
        101: {"Arsenal": arsenal_lineup, "Chelsea": chelsea_lineup},
        102: {"Chelsea": chelsea_lineup, "Arsenal": arsenal_lineup},
        201: {"Arsenal": arsenal_lineup, "Liverpool": liverpool_lineup},
    }


# ------------------------------------------------------------------ #
# get_player_shooting_season (multi-season)                            #
# ------------------------------------------------------------------ #


def test_get_player_shooting_season_one_row_per_season(
    client,
    sample_competitions_df,
    multi_season_matches,
    multi_season_events,
    multi_season_lineups,
):
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.competitions",
            return_value=sample_competitions_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.matches",
            side_effect=lambda competition_id, season_id: multi_season_matches[
                season_id
            ],
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            side_effect=lambda match_id: multi_season_lineups[match_id],
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            side_effect=lambda match_id: multi_season_events[match_id],
        ),
    ):
        result = client.get_player_shooting_season(
            competition_id=11, season_ids=[1, 2], players=["Saka"]
        )

    # one row per player per season — no cross-season stat bleed
    assert len(result) == 2
    assert set(result["season_id"]) == {1, 2}
    assert set(result["season_name"]) == {"2020/21", "2021/22"}

    season_1_row = result[result["season_id"] == 1].iloc[0]
    assert season_1_row["shots"] == 2
    assert season_1_row["goals"] == 1
    assert season_1_row["total_xg"] == pytest.approx(0.5)

    season_2_row = result[result["season_id"] == 2].iloc[0]
    assert season_2_row["shots"] == 1
    assert season_2_row["goals"] == 1
    assert season_2_row["total_xg"] == pytest.approx(0.5)


def test_get_player_shooting_season_accepts_single_int_season_id(
    client,
    sample_competitions_df,
    multi_season_matches,
    multi_season_events,
    multi_season_lineups,
):
    """A bare int season_ids (not a list) should still work — backward compatible."""
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.competitions",
            return_value=sample_competitions_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.matches",
            side_effect=lambda competition_id, season_id: multi_season_matches[
                season_id
            ],
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            side_effect=lambda match_id: multi_season_lineups[match_id],
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            side_effect=lambda match_id: multi_season_events[match_id],
        ),
    ):
        result = client.get_player_shooting_season(
            competition_id=11, season_ids=1, players=["Saka"]
        )

    assert len(result) == 1
    assert result.iloc[0]["season_id"] == 1


def test_get_player_shooting_season_not_found_raises_error(
    client, sample_competitions_df, multi_season_matches, multi_season_lineups
):
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.competitions",
            return_value=sample_competitions_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.matches",
            side_effect=lambda competition_id, season_id: multi_season_matches[
                season_id
            ],
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            side_effect=lambda match_id: multi_season_lineups[match_id],
        ),
    ):
        with pytest.raises(DataNotFoundError):
            client.get_player_shooting_season(
                competition_id=11, season_ids=[1, 2], players=["Nobody"]
            )


# ------------------------------------------------------------------ #
# get_shots_season                                                      #
# ------------------------------------------------------------------ #


def test_get_shots_season_teams_filter_excludes_other_teams(
    client,
    sample_competitions_df,
    multi_season_matches,
    multi_season_events,
):
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.competitions",
            return_value=sample_competitions_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.matches",
            side_effect=lambda competition_id, season_id: multi_season_matches[
                season_id
            ],
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            side_effect=lambda match_id: multi_season_events[match_id],
        ),
    ):
        result = client.get_shots_season(
            competition_id=11, season_ids=[1, 2], teams=["Arsenal"]
        )

    # Havertz's shot for Chelsea in match 101 must be excluded
    assert set(result["team"]) == {"Arsenal"}
    assert len(result) == 3


# ------------------------------------------------------------------ #
# get_player_goals_assists_match — missing pass_goal_assist column     #
# ------------------------------------------------------------------ #


def test_get_player_goals_assists_match_missing_assist_column(client):
    """StatsBomb omits pass_goal_assist entirely when no assist occurred in
    the match — this must not crash, just report 0 assists."""
    events_without_assist_col = pd.DataFrame(
        {
            "type": ["Shot"],
            "player": ["Saka"],
            "position": ["Right Wing"],
            "team": ["Arsenal"],
            "minute": [10],
            "shot_statsbomb_xg": [0.3],
            "shot_outcome": ["Goal"],
            "pass_outcome": [None],
            "pass_switch": [None],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=events_without_assist_col,
    ):
        result = client.get_player_goals_assists_match(match_id=1)

    saka_row = result[result["player"] == "Saka"].iloc[0]
    assert saka_row["goals"] == 1
    assert saka_row["assists"] == 0


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
