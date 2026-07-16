
"""
StatsBomb open data client
Retrieves ....
"""

import pandas as pd
from statsbombpy import sb

from football_analytics.utils.exceptions import DataNotFoundError


class StatsBombClient:
    """
    Wraps the statsbombpy library for access to StatsBomb openn data.
    No API key required for access, statsbombpy library handles connection and authentication
    """

    def get_competitions(self) -> pd.DataFrame:
        """
        List all competitions available in StatsBomb open data.

        Returns:
            DataFrame of available competitions and seasons.
        """
        return sb.competitions()

    def get_matches(
        self,
        competition_id: int,
        season_id: int,
        team: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch all matches for a given competition and season,
        optionally filtered by team name.

        Args:
            competition_id: StatsBomb competition ID.
            season_id: StatsBomb season ID.
            team: Optional team name to filter matches. Must match
            the team name exactly as it appears in StatsBomb data
            e.g. 'Arsenal', 'Barcelona'. Defaults to all teams.

        Returns:
            DataFrame of matches.

        Raises:
            DataNotFoundError: If no matches are found for the given
            competition, season, or team.
        """
        matches = sb.matches(competition_id=competition_id, season_id=season_id)
        if matches.empty:
            raise DataNotFoundError(
                f"No matches found for competition {competition_id},season {season_id}"
            )

        if team:
            matches = matches[
                (matches["home_team"] == team) | (matches["away_team"] == team)
            ].copy()
            if matches.empty:
                raise DataNotFoundError(
                    f"No matches found for team '{team} in competition "
                    f"{competition_id}, season {season_id}. Make sure the "
                    f"team name matches StatsBomb team name exactly"
                )
            return matches

    def get_events(self, match_id: int) -> pd.DataFrame:
        """
        Fetch all on-ball events for a specific match (passes, shots, tackles,
        dribbles, carries, fouls, duels and more). Used as the foundation by
        all other stat methods which filter this data down to specific event types.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame of all events in the match.

        Raises:
            DataNotFoundError: If no events are found for the given match.
        """
        events = sb.events(match_id=match_id)
        if events.empty:
            raise DataNotFoundError(f"No events found for match {match_id}.")
        return events

    def get_shots(self, match_id: int) -> pd.DataFrame:
        """
        Fetch all shot events for a specific match, including xG values.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame of shots with xG values.

        Raises:
            DataNotFoundError: If no shots are found for the given match.
        """
        events = self.get_events(match_id)
        shots = events[events["type"] == "Shot"].copy()
        if shots.empty:
            raise DataNotFoundError(f"No shots found for match {match_id}.")
        return shots[["player", "team", "minute", "shot_statsbomb_xg", "shot_outcome"]]

    # -----------------------------------------------------------------------#
    ## Player stats - match level                                           ##
    # -----------------------------------------------------------------------#

    def get_player_shooting_match(self, match_id):
        """
        Fetch shooting stats per player for a specific match.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, shots, shots_on_target,
            goals, total_xg, xg_per_shot.

        Raises:
            DataNotFoundError: If no shot data is found for the given match.
        """
        shots = self.get_shots()
        stats = (
            shots.groupby(["player", "team"])
            .agg(
                shots=("shot_statsbomb_xg", "count"),
                shots_on_target=(
                    "shot_outcome",
                    lambda x: (x == "Goal").sum() + (x == "Saved").sum(),
                ),
                goals=("shot_outcome", lambda x: (x == "Goal").sum()),
                total_xg=("shot_statsbomb_xg", "sum"),
            )
            .reset_index()
        )
        stats["xg_per_shot"] = (stats["total_xg"] / stats["shots"]).round(3)
        stats["total_xg"] = (stats["total_xg"]).round(3)
        return stats

    def get_player_passing_match(self, match_id: int) -> pd.DataFrame:
        """
        Fetch passing stats per player for a specific match.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, passes, passes_completed,
            completion_rate, progressive_passes.

        Raises:
            DataNotFoundError: If no pass data is found for the given match.
        """
        events = self.get_events(match_id)
        passes = events[events["type"] == "Pass"].copy()
        if passes.empty:
            raise DataNotFoundError(f"No pass data found for match{match_id}")

        stats = (
            passes.groupby(["player", "team"])
            .agg(
                passes=("type", "count"),
                # StatsBomb marks completed passes as NaN in pass_outcome.
                # The column is only populated when a pass fails e.g. Incomplete, Out, Intercepted.  # noqa: E501
                # Therefore isna().sum() correctly counts successful completions.
                passes_completed=("pass outcome", lambda x: x.isna().sum()),
                progressive_passes=(
                    "pass_switch",
                    lambda x: x.sum() if x.dtype == bool else 0,
                ),
            )
            .reset_index()
        )
        stats["completion_rate"] = (
            (stats["passes_completed"] / stats["passes"]) * 100
        ).round(1)
        return stats

    def get_player_defensive_match(self, match_id: int) -> pd.DataFrame:
        """
        Fetch defensive stats per player for a specific match.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, tackles, interceptions,
            clearances.

        Raises:
            DataNotFoundError: If no defensive data found for the given match.
        """
        events = self.get_events(match_id)
        tackles = (
            events[events["type"] == "Tackle"]
            .groupby(["player", "team"])
            .size()
            .reset_index()
        )
        interceptions = (
            events[events["type"] == "Interception"]
            .groupby(["player", "team"])
            .size()
            .reset_index()
        )
        clearances = (
            events[events["type"] == "Clearance"]
            .groupby(["player", "team"])
            .size()
            .reset_index()
        )

        stats = tackles.merge(interceptions, on=["player", "team"], how="outer")
        stats = stats.merge(clearances, on=["player", "team"], how="outer")
        stats = stats.fillna(0)

        for col in ["tackles", "interceptions", "clearances"]:
            stats[col] = stats[col].astype(int)

        if stats.empty:
            raise DataNotFoundError(f"No defensive data found for match {match_id}")
        return stats

    def get_player_goals_assists_match(self, match_id: int) -> pd.DataFrame:
        """
        Fetch goals and assists per player for a specific match.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, goals, assists.

        Raises:
            DataNotFoundError: If no event data found for the given match.
        """
        shots = self.get_shots(match_id)
        events = self.get_events(match_id)

        goals = (
            shots[shots["shot_outcome"] == "Goal"]
            .groupby(["player", "team"])
            .size()
            .reset_index(name="goals")
        )
        assists = (
            events[events["pass_goal_assist"]]
            .groupby(["player", "team"])
            .size()
            .reset_index(name="assists")
        )

        stats = goals.merge(assists, on=["player", "team"], how="outer")
        stats = stats.fillna(0)

        for col in ["goals", "assists"]:
            stats[col] = stats[col].astype(int)

        if stats.empty:
            raise DataNotFoundError(f"No goals or assists found for match {match_id}")
        return stats

    # -----------------------------------------------------------------------#
    ## Player stats - season level                                           ##
    # -----------------------------------------------------------------------#

    def _aggregate_season_stats(
        self, competition_id: int, season_id: int, match_stat_method
    ) -> pd.DataFrame:
        """
        Internal helper that iterates over every match in a season,
        calls the given match-level stat method, and aggregates the results.

        Args:
            competition_id: StatsBomb competition ID.
            season_id: StatsBomb season ID.
            match_stat_method: A bound match-level stat method from this class
            e.g. self.get_player_shooting_match.

        Returns:
            Aggregated DataFrame across all matches in the season.

        Raises:
            DataNotFoundError: If no matches are found for the season.
        """
        matches = self.get_matches(competition_id, season_id)
        all_stats = []
        for match_id in matches["match_id"]:
            try:
                stats = match_stat_method(match_id)
                all_stats.append(stats)
            except DataNotFoundError:
                continue  # continue allows us to compensate for matches that have missing data w/out crashing

        if not all_stats:
            raise DataNotFoundError(
                f"no stats found for competition {competition_id},season {season_id}"
            )
        return pd.concat(all_stats, ignore_index=True)

    def get_player_shooting_season(
        self, competition_id: int, season_id: int
    ) -> pd.DataFrame:
        """
        Fetch aggregated shooting stats per player across a full season.

        Args:
            competition_id: StatsBomb competition ID.
            season_id: StatsBomb season ID.

        Returns:
            DataFrame with columns: player, team, shots, shots_on_target,
            goals, total_xg, xg_per_shot.

        Raises:
            DataNotFoundError: If no data is found for the season.
        """
        raw_data = self._aggregate_season_stats(
            competition_id, season_id, self.get_player_shooting_match
        )
        season_stats = (
            raw_data.groupby(["player", "team"])
            .agg(
                shots=("shots", "sum"),
                shots_on_target=("shots_on_target", "sum"),
                goals=("goals", "sum"),
                total_xg=("total_xg", "sum"),
            )
            .reset_index()
        )
        season_stats["xg_per_shot"] = (
            season_stats["total_xg"] / season_stats["shots"]
        ).round(3)
        season_stats["total_xg"] = season_stats["total_xg"].round(3)
        return season_stats.sort_values("total_xg", ascending=False)

    def get_player_passing_season(
        self, competition_id: int, season_id: int
    ) -> pd.DataFrame:
        """
        Fetch aggregated passing stats per player across a full season.

        Args:
            competition_id: StatsBomb competition ID.
            season_id: StatsBomb season ID.

        Returns:
            DataFrame with columns: player, team, passes, passes_completed,
            completion_rate, progressive_passes.

        Raises:
            DataNotFoundError: If no data is found for the season.
        """
        raw_data = self._aggregate_season_stats(
            competition_id, season_id, self.get_player_passing_match
        )
        season_stats = (
            raw_data.groupby(["player", "team"])
            .agg(
                passes=("passes", "sum"),
                passes_completed=("passes_completed", "sum"),
                progressive_passes=("progressive_passes", "sum"),
            )
            .reset_index()
        )
        season_stats["completion_rate"] = (
            (season_stats["passes_completed"] / season_stats["passes"]) * 100
        ).round(1)
        return season_stats.sort_values("passes", ascending=False)

    def get_player_defensive_season(
        self,
        competition_id: int,
        season_id: int,
    ) -> pd.DataFrame:
        """
        Fetch aggregated defensive stats per player across a full season.

        Args:
            competition_id: StatsBomb competition ID.
            season_id: StatsBomb season ID.

        Returns:
            DataFrame with columns: player, team, tackles, interceptions,
            clearances.

        Raises:
            DataNotFoundError: If no data is found for the season.
        """
        raw_data = self._aggregate_season_stats(
            competition_id, season_id, self.get_player_defensive_match
        )
        season_stats = (
            raw_data.groupby(["player", "team"])
            .agg(
                tackles=("tackles", "sum"),
                interceptions=("interceptions", "sum"),
                clearances=("clearances", "sum"),
            )
            .reset_index()
        )
        return season_stats.sort_values("tackles", ascending=False)

    def get_player_goal_assists_season(
        self,
        competition_id: int,
        season_id: int,
    ) -> pd.DataFrame:
        """
        Fetch aggregated goals and assists per player across a full season.

        Args:
            competition_id: StatsBomb competition ID.
            season_id: StatsBomb season ID.

        Returns:
            DataFrame with columns: player, team, goals, assists.

        Raises:
            DataNotFoundError: If no data is found for the season.
        """
        raw_data = self._aggregate_season_stats(
            competition_id, season_id, self.get_player_goal_assists_match
        )

        season_stats = (
            raw_data.groupby(["player", "team"])
            .agg(
                goals=("goals", "sum"),
                assists=("assists", "sum"),
            )
            .reset_index()
        )
        return season_stats.sort_values("goals", ascending=False)

"""Tests for StatsBombClient."""

import pandas as pd
import pytest
from unittest.mock import patch
from football_analytics.data.statsbomb_client import StatsBombClient
from football_analytics.utils.exceptions import DataNotFoundError


@pytest.fixture
def client():
    """Returns a StatsBombClient instance."""
    return StatsBombClient()


@pytest.fixture
def sample_matches_df():
    """Fake matches DataFrame mimicking statsbombpy output."""
    return pd.DataFrame({
        "match_id": [1, 2, 3],
        "home_team": ["Arsenal", "Barcelona", "Arsenal"],
        "away_team": ["Chelsea", "Arsenal", "Liverpool"],
        "home_score": [2, 1, 0],
        "away_score": [1, 2, 0],
    })


@pytest.fixture
def sample_events_df():
    """Fake events DataFrame mimicking statsbombpy output."""
    return pd.DataFrame({
        "type": ["Shot", "Pass", "Shot", "Tackle", "Shot"],
        "player": ["Saka", "Odegaard", "Havertz", "White", "Palmer"],
        "team": ["Arsenal", "Arsenal", "Arsenal", "Arsenal", "Chelsea"],
        "minute": [12, 23, 45, 67, 88],
        "shot_statsbomb_xg": [0.342, None, 0.521, None, 0.231],
        "shot_outcome": ["Goal", None, "Saved", None, "Missed"],
        "pass_outcome": [None, None, None, None, None],
        "pass_switch": [None, False, None, None, None],
        "pass_goal_assist": [None, False, None, None, None],
    })


# ------------------------------------------------------------------ #
# get_competitions                                                      #
# ------------------------------------------------------------------ #

def test_get_competitions_returns_dataframe(client):
    fake_competitions = pd.DataFrame({
        "competition_id": [11, 16],
        "season_id": [1, 4],
        "competition_name": ["La Liga", "Champions League"],
    })
    with patch("football_analytics.data.statsbomb_client.sb.competitions",
               return_value=fake_competitions):
        result = client.get_competitions()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2


# ------------------------------------------------------------------ #
# get_matches                                                           #
# ------------------------------------------------------------------ #

def test_get_matches_returns_all_matches(client, sample_matches_df):
    with patch("football_analytics.data.statsbomb_client.sb.matches",
               return_value=sample_matches_df):
        result = client.get_matches(competition_id=11, season_id=1)
        assert len(result) == 3


def test_get_matches_team_filter_returns_correct_matches(
    client, sample_matches_df
):
    with patch("football_analytics.data.statsbomb_client.sb.matches",
               return_value=sample_matches_df):
        result = client.get_matches(
            competition_id=11, season_id=1, team="Arsenal"
        )
        # Arsenal appear in all 3 matches
        assert len(result) == 3


def test_get_matches_team_filter_unknown_team_raises_error(
    client, sample_matches_df
):
    with patch("football_analytics.data.statsbomb_client.sb.matches",
               return_value=sample_matches_df):
        with pytest.raises(DataNotFoundError):
            client.get_matches(
                competition_id=11, season_id=1, team="Tottenham"
            )


def test_get_matches_empty_response_raises_error(client):
    with patch("football_analytics.data.statsbomb_client.sb.matches",
               return_value=pd.DataFrame()):
        with pytest.raises(DataNotFoundError):
            client.get_matches(competition_id=99, season_id=99)


# ------------------------------------------------------------------ #
# get_events                                                            #
# ------------------------------------------------------------------ #

def test_get_events_returns_dataframe(client, sample_events_df):
    with patch("football_analytics.data.statsbomb_client.sb.events",
               return_value=sample_events_df):
        result = client.get_events(match_id=1)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5


def test_get_events_empty_response_raises_error(client):
    with patch("football_analytics.data.statsbomb_client.sb.events",
               return_value=pd.DataFrame()):
        with pytest.raises(DataNotFoundError):
            client.get_events(match_id=99)


# ------------------------------------------------------------------ #
# get_shots                                                             #
# ------------------------------------------------------------------ #

def test_get_shots_returns_only_shots(client, sample_events_df):
    with patch("football_analytics.data.statsbomb_client.sb.events",
               return_value=sample_events_df):
        result = client.get_shots(match_id=1)
        # sample_events_df has 3 shot events
        assert len(result) == 3


def test_get_shots_correct_columns(client, sample_events_df):
    with patch("football_analytics.data.statsbomb_client.sb.events",
               return_value=sample_events_df):
        result = client.get_shots(match_id=1)
        expected_cols = {
            "player", "team", "minute",
            "shot_statsbomb_xg", "shot_outcome"
        }
        assert expected_cols.issubset(set(result.columns))


def test_get_shots_no_shots_raises_error(client):
    """Test that a match with no shot events raises DataNotFoundError."""
    no_shots_df = pd.DataFrame({
        "type": ["Pass", "Tackle"],
        "player": ["Odegaard", "White"],
        "team": ["Arsenal", "Arsenal"],
        "minute": [10, 20],
        "shot_statsbomb_xg": [None, None],
        "shot_outcome": [None, None],
        "pass_outcome": [None, None],
        "pass_switch": [False, None],
        "pass_goal_assist": [False, None],
    })
    with patch("football_analytics.data.statsbomb_client.sb.events",
               return_value=no_shots_df):
        with pytest.raises(DataNotFoundError):
            client.get_shots(match_id=1)