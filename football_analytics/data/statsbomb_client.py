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
