"""Tests for standings.py analytics module"""

import pandas as pd
import pytest

from football_analytics.analytics.standings import (
    get_clean_sheets,
    get_clean_standings,
    get_expected_vs_actual,
    get_standings_from_matches,
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


@pytest.fixture
def sample_statsbomb_matches():
    """StatsBombClient.get_matches-shaped DataFrame: Arsenal beat both
    Chelsea and Liverpool, Chelsea and Liverpool draw with each other."""
    return pd.DataFrame(
        {
            "match_id": [1, 2, 3],
            "home_team": ["Arsenal", "Chelsea", "Arsenal"],
            "away_team": ["Chelsea", "Liverpool", "Liverpool"],
            "home_score": [2, 1, 3],
            "away_score": [1, 1, 0],
            "match_week": [1, 2, 3],
        }
    )


def test_get_standings_from_matches_correct_totals(sample_statsbomb_matches):
    result = get_standings_from_matches(sample_statsbomb_matches)
    arsenal = result[result["team"] == "Arsenal"].iloc[0]
    assert arsenal["played"] == 2
    assert arsenal["won"] == 2
    assert arsenal["points"] == 6
    assert arsenal["goals_for"] == 5
    assert arsenal["goals_against"] == 1


def test_get_standings_from_matches_ranked_by_points(sample_statsbomb_matches):
    result = get_standings_from_matches(sample_statsbomb_matches)
    assert result.iloc[0]["team"] == "Arsenal"
    assert result.iloc[0]["position"] == 1


def test_get_standings_from_matches_teams_filter(sample_statsbomb_matches):
    result = get_standings_from_matches(sample_statsbomb_matches, teams=["Arsenal"])
    assert len(result) == 1
    assert result.iloc[0]["team"] == "Arsenal"


def test_get_standings_from_matches_empty_raises_error():
    with pytest.raises(DataNotFoundError):
        get_standings_from_matches(pd.DataFrame())


def test_get_standings_from_matches_unknown_team_raises_error(
    sample_statsbomb_matches,
):
    with pytest.raises(DataNotFoundError):
        get_standings_from_matches(sample_statsbomb_matches, teams=["Tottenham"])


def test_get_standings_from_matches_accepted_by_get_expected_vs_actual(
    sample_statsbomb_matches, sample_team_xg
):
    """Proves get_standings_from_matches uses the same output contract as
    get_clean_standings, with no adapter needed."""
    standings = get_standings_from_matches(sample_statsbomb_matches)
    result = get_expected_vs_actual(standings, sample_team_xg)
    assert set(result.columns) == {
        "team",
        "actual_position",
        "xg_position",
        "position_difference",
    }


def test_get_clean_sheets_correct_counts(sample_statsbomb_matches):
    # Arsenal 2-1 Chelsea, Chelsea 1-1 Liverpool, Arsenal 3-0 Liverpool:
    # only Arsenal's 3-0 win is a clean sheet.
    result = get_clean_sheets(sample_statsbomb_matches)
    arsenal_row = result[result["team"] == "Arsenal"].iloc[0]
    chelsea_row = result[result["team"] == "Chelsea"].iloc[0]
    assert arsenal_row["clean_sheets"] == 1
    assert arsenal_row["matches_played"] == 2
    assert chelsea_row["clean_sheets"] == 0


def test_get_clean_sheets_sorted_descending(sample_statsbomb_matches):
    result = get_clean_sheets(sample_statsbomb_matches)
    assert result.iloc[0]["team"] == "Arsenal"


def test_get_clean_sheets_teams_filter(sample_statsbomb_matches):
    result = get_clean_sheets(sample_statsbomb_matches, teams=["Chelsea"])
    assert len(result) == 1
    assert result.iloc[0]["team"] == "Chelsea"


def test_get_clean_sheets_empty_raises_error():
    with pytest.raises(DataNotFoundError):
        get_clean_sheets(pd.DataFrame())


def test_get_clean_sheets_unknown_team_raises_error(sample_statsbomb_matches):
    with pytest.raises(DataNotFoundError):
        get_clean_sheets(sample_statsbomb_matches, teams=["Tottenham"])
