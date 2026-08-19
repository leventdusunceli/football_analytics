"""Tests for StatsBombClient."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from football_analytics.data.statsbomb_client import StatsBombClient, _with_retries
from football_analytics.utils.exceptions import DataNotFoundError


@pytest.fixture
def client():
    """Returns a StatsBombClient instance."""
    return StatsBombClient()


# ------------------------------------------------------------------ #
# _with_retries                                                        #
# ------------------------------------------------------------------ #


def test_with_retries_recovers_from_transient_connection_error():
    """A dropped connection followed by success should not fail the call."""
    fn = MagicMock(
        side_effect=[requests.exceptions.ConnectionError("refused"), "result"]
    )
    with patch("football_analytics.data.statsbomb_client.time.sleep"):
        result = _with_retries(fn)
    assert result == "result"
    assert fn.call_count == 2


def test_with_retries_raises_after_exhausting_attempts():
    """A persistent outage must still surface as an error, not silently
    return partial/empty data."""
    fn = MagicMock(side_effect=requests.exceptions.ConnectionError("refused"))
    with patch("football_analytics.data.statsbomb_client.time.sleep"):
        with pytest.raises(requests.exceptions.ConnectionError):
            _with_retries(fn)
    assert fn.call_count == 3


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
            # White's row is a Duel of duel_type "Tackle" — StatsBomb has no
            # standalone "Tackle" event type.
            "type": ["Shot", "Pass", "Shot", "Duel", "Shot"],
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
            "period": [1, 1, 1, 1, 2],
            "shot_statsbomb_xg": [0.342, None, 0.521, None, 0.231],
            "shot_outcome": ["Goal", None, "Saved", None, "Missed"],
            "pass_outcome": [None, None, None, None, None],
            "pass_switch": [None, False, None, None, None],
            "pass_goal_assist": [None, False, None, None, None],
            "duel_type": [None, None, None, "Tackle", None],
            "location": [
                [110.0, 38.0],
                [60.0, 40.0],
                [100.0, 42.0],
                None,
                [95.0, 45.0],
            ],
            "pass_end_location": [None, [90.0, 40.0], None, None, None],
            "shot_end_location": [
                [120.0, 40.0],
                None,
                [118.0, 36.0],
                None,
                [119.0, 44.0],
            ],
            "pass_through_ball": [None, None, None, None, None],
        }
    )


@pytest.fixture
def sample_lineups():
    """sb.lineups matching sample_events_df's players, plus one unused
    substitute (empty `positions`) to verify bench players who never
    entered the match are excluded from goals/assists output."""
    return {
        "Arsenal": pd.DataFrame(
            {
                "player_name": ["Saka", "Odegaard", "Havertz", "White", "Benched Sub"],
                "positions": [
                    [{"position": "Right Wing"}],
                    [{"position": "Center Midfield"}],
                    [{"position": "Center Forward"}],
                    [{"position": "Right Back"}],
                    [],
                ],
            }
        ),
        "Chelsea": pd.DataFrame(
            {
                "player_name": ["Palmer"],
                "positions": [[{"position": "Center Forward"}]],
            }
        ),
    }


@pytest.fixture
def sample_passing_events_df():
    """Fake Pass-only events DataFrame covering a clearly progressive pass,
    a clearly non-progressive pass, a through ball, and an assist, for
    testing get_player_passing_match's progressive/line-breaking/assist
    logic."""
    return pd.DataFrame(
        {
            "type": ["Pass", "Pass", "Pass", "Pass"],
            "player": ["Odegaard", "Odegaard", "Saka", "Havertz"],
            "position": [
                "Center Midfield",
                "Center Midfield",
                "Right Wing",
                "Center Forward",
            ],
            "team": ["Arsenal", "Arsenal", "Arsenal", "Arsenal"],
            "minute": [10, 20, 30, 40],
            "period": [1, 1, 1, 1],
            # opponent goal center is (120, 40) on StatsBomb's 120x80 pitch
            "location": [
                [60.0, 40.0],
                [60.0, 40.0],
                [100.0, 40.0],
                [90.0, 40.0],
            ],
            "pass_end_location": [
                [90.0, 40.0],
                [65.0, 40.0],
                [110.0, 40.0],
                [100.0, 40.0],
            ],
            "pass_outcome": [None, None, None, None],
            "pass_switch": [None, None, None, None],
            "pass_goal_assist": [None, None, None, True],
            "pass_through_ball": [None, None, True, None],
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


def test_get_shots_team_filter(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_shots(match_id=1, team="Chelsea")
        assert set(result["team"]) == {"Chelsea"}


def test_get_shots_player_filter(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_shots(match_id=1, player="Saka")
        assert set(result["player"]) == {"Saka"}


def test_get_shots_filter_no_match_raises_error(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        with pytest.raises(DataNotFoundError):
            client.get_shots(match_id=1, team="Liverpool")


def test_get_shots_includes_location_columns(client, sample_events_df):
    """Regression test: get_shots must expose location/shot_end_location
    for visualization, not just the aggregate stat columns."""
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_shots(match_id=1)
        assert {"location", "shot_end_location", "period"}.issubset(result.columns)


# ------------------------------------------------------------------ #
# get_passes                                                            #
# ------------------------------------------------------------------ #


def test_get_passes_returns_only_passes(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_passes(match_id=1)
        # sample_events_df has 1 pass event
        assert len(result) == 1
        assert result.iloc[0]["player"] == "Odegaard"


def test_get_passes_correct_columns(client, sample_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_passes(match_id=1)
        expected_cols = {
            "player",
            "team",
            "position",
            "minute",
            "period",
            "location",
            "pass_end_location",
            "pass_outcome",
            "pass_through_ball",
            "pass_goal_assist",
            "is_progressive",
        }
        assert expected_cols.issubset(set(result.columns))


def test_get_passes_is_progressive_flag(client, sample_passing_events_df):
    """sample_passing_events_df has one clearly progressive pass (Odegaard's
    first) and one clearly non-progressive one (Odegaard's second)."""
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        result = client.get_passes(match_id=1, player="Odegaard")
        assert result["is_progressive"].tolist() == [True, False]


def test_get_passes_missing_goal_assist_column_defaults_false(client):
    """Same StatsBomb gotcha as pass_through_ball: pass_goal_assist can be
    entirely absent from a match's events, not just null."""
    events_without_assist_col = pd.DataFrame(
        {
            "type": ["Pass"],
            "player": ["Odegaard"],
            "position": ["Center Midfield"],
            "team": ["Arsenal"],
            "minute": [10],
            "period": [1],
            "location": [[20.0, 40.0]],
            "pass_end_location": [[70.0, 40.0]],
            "pass_outcome": [None],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=events_without_assist_col,
    ):
        result = client.get_passes(match_id=1)
        assert result.iloc[0]["pass_goal_assist"] == False  # noqa: E712


def test_get_passes_team_filter(client, sample_passing_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        result = client.get_passes(match_id=1, team="Arsenal")
        assert set(result["team"]) == {"Arsenal"}


def test_get_passes_player_filter(client, sample_passing_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        result = client.get_passes(match_id=1, player="Saka")
        assert set(result["player"]) == {"Saka"}
        assert len(result) == 1


def test_get_passes_filter_no_match_raises_error(client, sample_passing_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        with pytest.raises(DataNotFoundError):
            client.get_passes(match_id=1, player="Nobody")


def test_get_passes_no_passes_raises_error(client):
    no_passes_df = pd.DataFrame(
        {
            "type": ["Tackle"],
            "player": ["White"],
            "team": ["Arsenal"],
            "minute": [20],
            "period": [1],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=no_passes_df,
    ):
        with pytest.raises(DataNotFoundError):
            client.get_passes(match_id=1)


def test_get_passes_missing_through_ball_column_defaults_false(client):
    """Same gotcha as get_player_passing_match: pass_through_ball can be
    entirely absent from a match's events, not just null."""
    events_without_through_ball_col = pd.DataFrame(
        {
            "type": ["Pass"],
            "player": ["Odegaard"],
            "position": ["Center Midfield"],
            "team": ["Arsenal"],
            "minute": [10],
            "period": [1],
            "location": [[20.0, 40.0]],
            "pass_end_location": [[70.0, 40.0]],
            "pass_outcome": [None],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=events_without_through_ball_col,
    ):
        result = client.get_passes(match_id=1)
        assert result.iloc[0]["pass_through_ball"] == False  # noqa: E712


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
            "period": 1,
            "location": [100.0, 40.0],
            "shot_end_location": [120.0, 40.0],
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
    """sb.lineups per match_id — dict keyed by team name, as statsbombpy
    returns. `positions` is non-empty for everyone listed here (they all
    actually appeared) — get_player_goals_assists_match uses an empty
    `positions` list to detect unused substitutes."""

    def _played(names):
        return pd.DataFrame(
            {
                "player_name": names,
                "positions": [[{"position": "Unknown"}]] * len(names),
            }
        )

    arsenal_lineup = _played(["Saka", "Odegaard"])
    chelsea_lineup = _played(["Havertz"])
    liverpool_lineup = _played(["Salah"])
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
    assert season_1_row["matches_played"] == 2

    season_2_row = result[result["season_id"] == 2].iloc[0]
    assert season_2_row["shots"] == 1
    assert season_2_row["goals"] == 1
    assert season_2_row["total_xg"] == pytest.approx(0.5)
    assert season_2_row["matches_played"] == 1


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
# get_player_passing_season                                            #
# ------------------------------------------------------------------ #


def test_get_player_passing_season_includes_assists(
    client, sample_competitions_df, multi_season_matches
):
    """Regression test: assists must be present and summed correctly at
    the season level, sourced from get_player_passing_match's assists
    column."""

    def _pass_row(assist: bool) -> dict:
        return {
            "type": "Pass",
            "player": "Odegaard",
            "position": "Center Midfield",
            "team": "Arsenal",
            "minute": 10,
            "period": 1,
            "location": [60.0, 40.0],
            "pass_end_location": [65.0, 40.0],
            "pass_outcome": None,
            "pass_through_ball": None,
            "pass_goal_assist": True if assist else None,
        }

    events_by_match = {
        101: pd.DataFrame([_pass_row(assist=True)]),
        102: pd.DataFrame([_pass_row(assist=False)]),
    }

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
            side_effect=lambda match_id: events_by_match[match_id],
        ),
    ):
        result = client.get_player_passing_season(competition_id=11, season_ids=1)

    assert result.iloc[0]["assists"] == 1


# ------------------------------------------------------------------ #
# get_player_goals_assists_season (multi-season)                       #
# ------------------------------------------------------------------ #


def test_get_player_goals_assists_season_matches_played_counts_all_appearances(
    client,
    sample_competitions_df,
    multi_season_matches,
    multi_season_events,
    multi_season_lineups,
):
    """Regression test for the matches_played bug: Saka appears in both
    matches of season 1 (multi_season_events: scores in 101, only shoots
    — no goal, no assist — in 102) but must still be counted as having
    played both, not just the one he scored in."""
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
        result = client.get_player_goals_assists_season(
            competition_id=11, season_ids=[1, 2], players=["Saka"]
        )

    season_1_row = result[result["season_id"] == 1].iloc[0]
    assert season_1_row["matches_played"] == 2
    assert season_1_row["goals"] == 1

    season_2_row = result[result["season_id"] == 2].iloc[0]
    assert season_2_row["matches_played"] == 1
    assert season_2_row["goals"] == 1


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
            "period": [1],
            "location": [[100.0, 40.0]],
            "shot_end_location": [[120.0, 40.0]],
            "shot_statsbomb_xg": [0.3],
            "shot_outcome": ["Goal"],
            "pass_outcome": [None],
            "pass_switch": [None],
        }
    )
    saka_lineup = {
        "Arsenal": pd.DataFrame(
            {"player_name": ["Saka"], "positions": [[{"position": "Right Wing"}]]}
        )
    }
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=events_without_assist_col,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=saka_lineup,
        ),
    ):
        result = client.get_player_goals_assists_match(match_id=1)

    saka_row = result[result["player"] == "Saka"].iloc[0]
    assert saka_row["goals"] == 1
    assert saka_row["assists"] == 0


# ------------------------------------------------------------------ #
# get_player_passing_match — missing pass_through_ball column         #
# ------------------------------------------------------------------ #


def test_get_player_passing_match_missing_through_ball_column(client):
    """StatsBomb omits pass_through_ball entirely when no through ball
    occurred in the match — this must not crash, just report 0
    line_breaking_passes."""
    events_without_through_ball_col = pd.DataFrame(
        {
            "type": ["Pass"],
            "player": ["Odegaard"],
            "position": ["Center Midfield"],
            "team": ["Arsenal"],
            "minute": [10],
            "period": [1],
            "location": [[20.0, 40.0]],
            "pass_end_location": [[70.0, 40.0]],
            "pass_outcome": [None],
            "pass_switch": [None],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=events_without_through_ball_col,
    ):
        result = client.get_player_passing_match(match_id=1)

    odegaard_row = result[result["player"] == "Odegaard"].iloc[0]
    assert odegaard_row["line_breaking_passes"] == 0


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


def test_get_player_passing_match_progressive_passes(
    client, sample_passing_events_df
):
    """Regression test: progressive_passes must count actual progression,
    not fall through to 0 (the original bug) or count pass_switch instead."""
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        result = client.get_player_passing_match(match_id=1)

    odegaard = result[result["player"] == "Odegaard"].iloc[0]
    assert odegaard["passes"] == 2
    assert odegaard["progressive_passes"] == 1

    saka = result[result["player"] == "Saka"].iloc[0]
    assert saka["progressive_passes"] == 1


def test_get_player_passing_match_line_breaking_passes(
    client, sample_passing_events_df
):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        result = client.get_player_passing_match(match_id=1)

    odegaard = result[result["player"] == "Odegaard"].iloc[0]
    assert odegaard["line_breaking_passes"] == 0

    saka = result[result["player"] == "Saka"].iloc[0]
    assert saka["line_breaking_passes"] == 1


def test_get_player_passing_match_assists(client, sample_passing_events_df):
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_passing_events_df,
    ):
        result = client.get_player_passing_match(match_id=1)

    havertz = result[result["player"] == "Havertz"].iloc[0]
    assert havertz["assists"] == 1

    odegaard = result[result["player"] == "Odegaard"].iloc[0]
    assert odegaard["assists"] == 0


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


def test_get_player_defensive_match_counts_tackles_from_duel_events(
    client, sample_events_df
):
    """Regression test: StatsBomb has no standalone "Tackle" event type —
    tackles are type == "Duel" with duel_type == "Tackle". sample_events_df
    has exactly one such Duel, for White."""
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=sample_events_df,
    ):
        result = client.get_player_defensive_match(match_id=1)

    white_row = result[result["player"] == "White"].iloc[0]
    assert white_row["tackles"] == 1


def test_get_player_defensive_match_excludes_non_tackle_duels(client):
    """A Duel with duel_type "Aerial Lost" is not a tackle and must not be
    counted as one."""
    events = pd.DataFrame(
        {
            "type": ["Duel", "Interception"],
            "player": ["White", "Saka"],
            "position": ["Right Back", "Right Wing"],
            "team": ["Arsenal", "Arsenal"],
            "minute": [30, 40],
            "period": [1, 1],
            "duel_type": ["Aerial Lost", None],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=events,
    ):
        result = client.get_player_defensive_match(match_id=1)

    assert "White" not in result["player"].values
    saka_row = result[result["player"] == "Saka"].iloc[0]
    assert saka_row["interceptions"] == 1


def test_get_player_defensive_match_missing_duel_type_column(client):
    """StatsBomb omits duel_type entirely when no duel occurred at all in
    the match — this must not crash, just report 0 tackles."""
    events_without_duel_type_col = pd.DataFrame(
        {
            "type": ["Interception"],
            "player": ["Saka"],
            "position": ["Right Wing"],
            "team": ["Arsenal"],
            "minute": [40],
            "period": [1],
        }
    )
    with patch(
        "football_analytics.data.statsbomb_client.sb.events",
        return_value=events_without_duel_type_col,
    ):
        result = client.get_player_defensive_match(match_id=1)

    saka_row = result[result["player"] == "Saka"].iloc[0]
    assert saka_row["tackles"] == 0


# ------------------------------------------------------------------ #
# get_player_goals_assists_match                                        #
# ------------------------------------------------------------------ #


def test_get_player_goals_assists_match_returns_dataframe(
    client, sample_events_df, sample_lineups
):
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=sample_events_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=sample_lineups,
        ),
    ):
        result = client.get_player_goals_assists_match(match_id=1)
        assert isinstance(result, pd.DataFrame)


def test_get_player_goals_assists_match_passes_match_id(
    client, sample_events_df, sample_lineups
):
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=sample_events_df,
        ) as mock_events,
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=sample_lineups,
        ),
    ):
        client.get_player_goals_assists_match(match_id=42)
        mock_events.assert_called_with(match_id=42)


def test_get_player_goals_assists_match_includes_players_with_zero_stats(
    client, sample_events_df, sample_lineups
):
    """Regression test for the matches_played bug: every player who
    appeared in the match must get a row, even with 0 goals and 0
    assists — not just the scorers/assisters. Odegaard neither scores
    nor assists in sample_events_df."""
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=sample_events_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=sample_lineups,
        ),
    ):
        result = client.get_player_goals_assists_match(match_id=1)

    odegaard_row = result[result["player"] == "Odegaard"].iloc[0]
    assert odegaard_row["goals"] == 0
    assert odegaard_row["assists"] == 0


def test_get_player_goals_assists_match_excludes_unused_substitutes(
    client, sample_events_df, sample_lineups
):
    """A player listed in the lineup with an empty `positions` list never
    actually entered the match, and must not be counted as an appearance."""
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=sample_events_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=sample_lineups,
        ),
    ):
        result = client.get_player_goals_assists_match(match_id=1)

    assert "Benched Sub" not in result["player"].values


def test_get_player_goals_assists_match_no_goals_or_assists_does_not_raise(
    client, sample_lineups
):
    """A scoreless match (no goals or assists by anyone) is a legitimate
    result, not missing data — it must not raise DataNotFoundError, and
    every player who played should still get a 0/0 row."""
    events_no_goals = pd.DataFrame(
        {
            "type": ["Shot", "Pass"],
            "player": ["Saka", "Odegaard"],
            "position": ["Right Wing", "Center Midfield"],
            "team": ["Arsenal", "Arsenal"],
            "minute": [10, 20],
            "period": [1, 1],
            "location": [[100.0, 40.0], [60.0, 40.0]],
            "shot_end_location": [[118.0, 42.0], None],
            "shot_statsbomb_xg": [0.1, None],
            "shot_outcome": ["Saved", None],
            "pass_outcome": [None, None],
            "pass_goal_assist": [None, None],
        }
    )
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=events_no_goals,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=sample_lineups,
        ),
    ):
        result = client.get_player_goals_assists_match(match_id=1)

    assert (result["goals"] == 0).all()
    assert (result["assists"] == 0).all()
    saka_row = result[result["player"] == "Saka"].iloc[0]
    assert saka_row["goals"] == 0


def test_get_player_goals_assists_match_raises_when_no_lineup_data(
    client, sample_events_df
):
    """If sb.lineups has no players who actually appeared (e.g. lineup data
    is unavailable for this match), that's a genuine DataNotFoundError —
    distinct from a scoreless-but-fully-played match."""
    empty_lineups = {
        "Arsenal": pd.DataFrame({"player_name": [], "positions": []}),
        "Chelsea": pd.DataFrame({"player_name": [], "positions": []}),
    }
    with (
        patch(
            "football_analytics.data.statsbomb_client.sb.events",
            return_value=sample_events_df,
        ),
        patch(
            "football_analytics.data.statsbomb_client.sb.lineups",
            return_value=empty_lineups,
        ),
    ):
        with pytest.raises(DataNotFoundError):
            client.get_player_goals_assists_match(match_id=1)
