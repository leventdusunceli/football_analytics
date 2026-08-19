"""
StatsBomb open data client
Retrieves ....
"""
# TODO: integrate visualizations into the method itself or create a visualization script
# TODO:  add across leagues comparison feature 


import time

import pandas as pd
import requests
from statsbombpy import sb

from football_analytics.utils.exceptions import DataNotFoundError

_NETWORK_RETRY_ATTEMPTS = 3
_NETWORK_RETRY_BACKOFF_SECONDS = 1.0



def _with_retries(fn, *args, **kwargs):
    """
    Call fn, retrying on transient network failures before giving up.

    statsbombpy fetches StatsBomb's open data as static JSON files over
    HTTP for every match/season call, so a single dropped connection can
    otherwise abort an entire multi-match aggregation (e.g. a 380-match
    season) even after most matches already succeeded. A persistent
    outage still raises after all retries are exhausted, rather than
    silently returning partial data as if it were complete.

    Args:
        fn: The statsbombpy function to call (e.g. sb.events).
        *args, **kwargs: Passed through to fn.

    Returns:
        Whatever fn returns.

    Raises:
        requests.exceptions.ConnectionError | requests.exceptions.Timeout:
            If every retry attempt fails.
    """
    last_error = None
    for attempt in range(_NETWORK_RETRY_ATTEMPTS):
        try:
            return fn(*args, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = e
            if attempt < _NETWORK_RETRY_ATTEMPTS - 1:
                time.sleep(_NETWORK_RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise last_error


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
        return _with_retries(sb.competitions)

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
        matches = _with_retries(
            sb.matches, competition_id=competition_id, season_id=season_id
        )
        if matches.empty:
            raise DataNotFoundError(
                f"No matches found for competition {competition_id},season {season_id}"
            )

        if team:
            matches = (
                matches[(matches["home_team"] == team) | (matches["away_team"] == team)]
                .sort_values(by=["match_week"])
                .copy()
            )
            if matches.empty:
                raise DataNotFoundError(
                    f"No matches found for team '{team} in competition "
                    f"{competition_id}, season {season_id}. Make sure the "
                    f"team name matches StatsBomb team name exactly"
                )
        return matches

    def get_events(self, match_id: int) -> pd.DataFrame:
        """
        Fetch all on-ball events for a specific match. (passes, shots, tackles,
        dribbles, carries, fouls, duels and more). Used as the foundation by
        all other stat methods which filter this data down to specific event types

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame of all events in the match, with players shown under their known
            names and their positions where available (e.g. Luis Suarez instead of
            Luis Alberto Suárez Díaz)

        Raises:
            DataNotFoundError: If no events are found for the given match.
        """
        events = _with_retries(sb.events, match_id=match_id)
        if events.empty:
            raise DataNotFoundError(f"No events found for match {match_id}.")

        # fix: use assignment not comparison
        if "player_nickname" in events.columns:
            events["player"] = events["player_nickname"].fillna(events["player"])

        if "position" in events.columns:
            events["position"] = events["position"].fillna("Unknown")

        return events

    def get_shots(
        self,
        match_id: int,
        team: str | None = None,
        player: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch all shot events for a specific match, including xG values
        and start/end locations for visualization.

        Args:
            match_id: StatsBomb match ID.
            team: Optional exact team name to filter to a single team's
                shots.
            player: Optional exact player name to filter to a single
                player's shots.

        Returns:
            DataFrame of shots with columns: player, team, position,
            minute, period, location, shot_end_location,
            shot_statsbomb_xg, shot_outcome. location and
            shot_end_location are [x, y] (shot_end_location is
            sometimes [x, y, z] when the shot's height was tracked).

        Raises:
            DataNotFoundError: If no shots are found for the given match,
                or none match the team/player filter.
        """
        events = self.get_events(match_id)
        shots = events[events["type"] == "Shot"]
        if team:
            shots = shots[shots["team"] == team]
        if player:
            shots = shots[shots["player"] == player]
        shots = shots.copy()

        if shots.empty:
            filter_desc = (
                f" matching team={team!r}, player={player!r}"
                if (team or player)
                else ""
            )
            raise DataNotFoundError(
                f"No shots found for match {match_id}{filter_desc}."
            )

        return shots[
            [
                "player",
                "team",
                "position",
                "minute",
                "period",
                "location",
                "shot_end_location",
                "shot_statsbomb_xg",
                "shot_outcome",
            ]
        ]

    def get_passes(
        self,
        match_id: int,
        team: str | None = None,
        player: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch all pass events for a specific match, with start/end
        locations and pass-classification flags for visualization.

        is_progressive means the pass ends at least 25% closer to the
        center of the opponent's goal than it started — a simplified,
        single-threshold version of the public "progressive pass" concept
        (StatsBomb has no direct tag for this, so it's computed from
        location/pass_end_location).

        Args:
            match_id: StatsBomb match ID.
            team: Optional exact team name to filter to a single team's
                passes.
            player: Optional exact player name to filter to a single
                player's passes.

        Returns:
            DataFrame of passes with columns: player, team, position,
            minute, period, location, pass_end_location, pass_outcome,
            pass_through_ball, pass_goal_assist, is_progressive. location
            and pass_end_location are [x, y].

        Raises:
            DataNotFoundError: If no passes are found for the given
                match, or none match the team/player filter.
        """
        events = self.get_events(match_id)
        passes = events[events["type"] == "Pass"]
        if team:
            passes = passes[passes["team"] == team]
        if player:
            passes = passes[passes["player"] == player]
        passes = passes.copy()

        if passes.empty:
            filter_desc = (
                f" matching team={team!r}, player={player!r}"
                if (team or player)
                else ""
            )
            raise DataNotFoundError(
                f"No passes found for match {match_id}{filter_desc}."
            )

        # StatsBomb omits these sparse flags entirely from a match's events
        # when they never occur in that match, rather than populating them
        # with False.
        for flag_col in ("pass_through_ball", "pass_goal_assist"):
            if flag_col not in passes.columns:
                passes[flag_col] = False

        # opponent's goal is centered at (120, 40) on StatsBomb's 120x80 pitch
        start_x, start_y = passes["location"].str[0], passes["location"].str[1]
        end_x, end_y = (
            passes["pass_end_location"].str[0],
            passes["pass_end_location"].str[1],
        )
        start_dist_to_goal = ((120 - start_x) ** 2 + (40 - start_y) ** 2) ** 0.5
        end_dist_to_goal = ((120 - end_x) ** 2 + (40 - end_y) ** 2) ** 0.5
        passes["is_progressive"] = end_dist_to_goal <= start_dist_to_goal * 0.75

        return passes[
            [
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
            ]
        ]

    # -----------------------------------------------------------------------#
    ## Player stats - match level                                           ##
    # -----------------------------------------------------------------------#

    @staticmethod
    def _primary_positions(
        events: pd.DataFrame, group_cols: list[str] | None = None
    ) -> pd.DataFrame:
        """
        Determine each player's most common position within a set of events.

        StatsBomb records `position` per event, reflecting whatever role a
        player was in at that moment — it can change within a single match
        (tactical shifts, injuries) and across matches in a season. Using it
        directly as a groupby key fragments one player's stats into multiple
        rows. This picks the most frequently recorded position per group as
        a single representative label instead.

        Args:
            events: Any DataFrame containing position and group_cols columns
                (e.g. events, shots, or concatenated match/season stats).
            group_cols: Columns identifying a single player. Defaults to
                ["player", "team"]; pass ["player", "team", "season_id"]
                when aggregating across multiple seasons so position is
                resolved per season rather than across all of them.

        Returns:
            DataFrame with group_cols plus a position column — one row per
            group.
        """
        group_cols = group_cols or ["player", "team"]
        return (
            events.groupby(group_cols)["position"]
            .agg(lambda x: x.mode().iat[0] if not x.mode().empty else "Unknown")
            .reset_index()
        )

    def get_player_shooting_match(self, match_id):
        """
        Fetch shooting stats per player for a specific match.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, position, shots,
            shots_on_target, goals, total_xg, xg_per_shot.

        Raises:
            DataNotFoundError: If no shot data is found for the given match.
        """
        shots = self.get_shots(match_id)
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
        stats = stats.merge(self._primary_positions(shots), on=["player", "team"])
        stats["xg_per_shot"] = (stats["total_xg"] / stats["shots"]).round(3)
        stats["total_xg"] = (stats["total_xg"]).round(3)
        return stats[
            [
                "player",
                "team",
                "position",
                "shots",
                "shots_on_target",
                "goals",
                "total_xg",
                "xg_per_shot",
            ]
        ]

    def get_player_passing_match(self, match_id: int) -> pd.DataFrame:
        """
        Fetch passing stats per player for a specific match.

        progressive_passes counts passes that end at least 25% closer to the
        center of the opponent's goal than they started, a simplified,
        single-threshold version of the "progressive pass" concept used in
        public football analytics. StatsBomb's open data has no direct
        progressive-pass tag, so this is computed from
        location/pass_end_location.

        line_breaking_passes counts passes StatsBomb tags as
        pass_through_ball, a pass threaded through or behind the defensive
        line into space. This is used as-is rather than computed from
        geometry, since detecting a genuine line break requires knowing
        defender positions at the moment of the pass, which isn't
        available in this dataset outside StatsBomb's separate, limited
        360 freeze-frame data.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, position, passes,
            passes_completed, completion_rate, progressive_passes,
            line_breaking_passes, assists.

        Raises:
            DataNotFoundError: If no pass data is found for the given match.
        """
        passes = self.get_passes(match_id)

        stats = (
            passes.groupby(["player", "team"])
            .agg(
                passes=("player", "count"),
                # StatsBomb marks completed passes as NaN in pass_outcome.
                # The column is only populated when a pass fails e.g. Incomplete, Out, Intercepted.  # noqa: E501
                # Therefore isna().sum() correctly counts successful completions.
                passes_completed=("pass_outcome", lambda x: x.isna().sum()),
                progressive_passes=("is_progressive", "sum"),
                # pass_through_ball/pass_goal_assist, like other sparse
                # StatsBomb flags, are only ever True or NaN (never an
                # explicit False), so they must be fillna(False) before
                # summing — dtype is object, not bool.
                line_breaking_passes=(
                    "pass_through_ball",
                    lambda x: x.fillna(False).sum(),
                ),
                assists=("pass_goal_assist", lambda x: x.fillna(False).sum()),
            )
            .reset_index()
        )
        stats = stats.merge(self._primary_positions(passes), on=["player", "team"])
        stats["completion_rate"] = (
            (stats["passes_completed"] / stats["passes"]) * 100
        ).round(1)
        return stats[
            [
                "player",
                "team",
                "position",
                "passes",
                "passes_completed",
                "completion_rate",
                "progressive_passes",
                "line_breaking_passes",
                "assists",
            ]
        ]

    def get_player_defensive_match(self, match_id: int) -> pd.DataFrame:
        """
        Fetch defensive stats per player for a specific match.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, position, tackles,
            interceptions, clearances.

        Raises:
            DataNotFoundError: If no defensive data found for the given match.
        """
        events = self.get_events(match_id)

        def _count(mask: pd.Series, col_name: str) -> pd.DataFrame:
            filtered = events[mask]
            if filtered.empty:
                return pd.DataFrame(columns=["player", "team", col_name])
            return (
                filtered.groupby(["player", "team"]).size().reset_index(name=col_name)
            )

        # Tackles aren't their own event type — they're Duels with
        # duel_type == "Tackle" (as opposed to "Aerial Lost").
        is_tackle = events["type"] == "Duel"
        if "duel_type" in events.columns:
            is_tackle &= events["duel_type"] == "Tackle"
        else:
            is_tackle &= False

        stats = (
            _count(is_tackle, "tackles")
            .merge(
                _count(events["type"] == "Interception", "interceptions"),
                on=["player", "team"],
                how="outer",
            )
            .merge(
                _count(events["type"] == "Clearance", "clearances"),
                on=["player", "team"],
                how="outer",
            )
            .fillna(0)
        )

        for col in ["tackles", "interceptions", "clearances"]:
            stats[col] = stats[col].astype(int)

        if stats.empty:
            raise DataNotFoundError(f"No defensive data found for match {match_id}.")

        stats = stats.merge(self._primary_positions(events), on=["player", "team"])
        return stats[
            ["player", "team", "position", "tackles", "interceptions", "clearances"]
        ]

    def get_player_goals_assists_match(self, match_id: int) -> pd.DataFrame:
        """
        Fetch goals and assists per player for a specific match.

        Every player who actually appeared in the match gets a row, with
        goals/assists zero-filled by default. Unlike shots or passes,
        goals and assists are sparse — most players record zero of both
        in most matches they play — so building this DataFrame only from
        scorers/assisters (the way get_shots/get_passes build theirs from
        shot-takers/passers) would make matches_played in
        get_player_goals_assists_season() count only matches where a
        player scored or assisted, silently undercounting real
        appearances. sb.lineups' per-player `positions` list is the
        appearance signal: an empty list means an unused substitute.

        Args:
            match_id: StatsBomb match ID.

        Returns:
            DataFrame with columns: player, team, position, goals, assists.

        Raises:
            DataNotFoundError: If no lineup data is found for the given match.
        """
        shots = self.get_shots(match_id)
        events = self.get_events(match_id)

        lineups = _with_retries(sb.lineups, match_id=match_id)
        roster = pd.concat(
            [
                pd.DataFrame(
                    {
                        "player": team_lineup.loc[
                            team_lineup["positions"].map(len) > 0, "player_name"
                        ],
                        "team": team,
                    }
                )
                for team, team_lineup in lineups.items()
            ],
            ignore_index=True,
        )

        if roster.empty:
            raise DataNotFoundError(f"No lineup data found for match {match_id}")

        goals = (
            shots[shots["shot_outcome"] == "Goal"]
            .groupby(["player", "team"])
            .size()
            .reset_index(name="goals")
        )
        # StatsBomb omits pass_goal_assist entirely from a match's events when
        # no assist occurred in that match, rather than populating it with False.
        if "pass_goal_assist" in events.columns:
            assists = (
                events[events["pass_goal_assist"].fillna(False) == True]  # noqa: E712
                .groupby(["player", "team"])
                .size()
                .reset_index(name="assists")
            )
        else:
            assists = pd.DataFrame(columns=["player", "team", "assists"])

        stats = roster.merge(goals, on=["player", "team"], how="left")
        stats = stats.merge(assists, on=["player", "team"], how="left")
        stats = stats.fillna(0)

        for col in ["goals", "assists"]:
            stats[col] = stats[col].astype(int)

        stats = stats.merge(
            self._primary_positions(events), on=["player", "team"], how="left"
        )
        stats["position"] = stats["position"].fillna("Unknown")

        return stats[["player", "team", "position", "goals", "assists"]]

    # -----------------------------------------------------------------------#
    ## Player stats - season level                                           ##
    # -----------------------------------------------------------------------#

    def _aggregate_season_stats(
        self,
        competition_id: int,
        season_ids: int | list[int],
        match_stat_method,
        players: list[str] | None = None,
        teams: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Internal helper that iterates over matches across one or more
        seasons, calls the given match-level stat method, and aggregates
        results. Each row is tagged with `season_id` and `season_name` so
        callers can distinguish stats from different seasons.

        When players is provided, only matches where those players appeared
        are processed — significantly reducing the number of API calls.
        When teams is provided, only matches involving those teams are
        processed — a cheaper filter than players since it only needs the
        matches DataFrame, not per-match lineups.

        Args:
            competition_id: StatsBomb competition ID.
            season_ids: A single StatsBomb season ID or a list of them.
            match_stat_method: A bound match-level stat method from this class.
            players: Optional list of exact player names to filter for.
                     When provided only matches containing those players
                     are loaded, skipping all irrelevant matches.
            teams: Optional list of exact team names to filter for. When
                     provided only matches involving those teams are loaded.

        Returns:
            Aggregated DataFrame across relevant matches and seasons, with
            `season_id` and `season_name` columns added.

        Raises:
            DataNotFoundError: If no matches or stats are found across any
            of the requested seasons.
        """
        if isinstance(season_ids, int):
            season_ids = [season_ids]

        season_names = (
            self.get_competitions()
            .query("competition_id == @competition_id")
            .set_index("season_id")["season_name"]
            .to_dict()
        )

        all_stats = []
        for season_id in season_ids:
            matches = self.get_matches(competition_id, season_id)

            if teams:
                matches = matches[
                    matches["home_team"].isin(teams) | matches["away_team"].isin(teams)
                ]

            if players:
                # filter matches to only those where our target players appeared
                # using the matches DataFrame lineup columns to avoid loading all events
                player_match_ids = set()
                for _, match in matches.iterrows():
                    lineups = _with_retries(sb.lineups, match_id=match["match_id"])
                    for team_lineup in lineups.values():
                        if any(
                            p in players for p in team_lineup["player_name"].values
                        ):
                            player_match_ids.add(match["match_id"])
                            break
                matches = matches[matches["match_id"].isin(player_match_ids)]

            for match_id in matches["match_id"]:
                try:
                    stats = match_stat_method(match_id)
                    if players:
                        stats = stats[stats["player"].isin(players)]
                    if teams:
                        stats = stats[stats["team"].isin(teams)]
                    if not stats.empty:
                        stats = stats.copy()
                        stats["season_id"] = season_id
                        stats["season_name"] = season_names.get(season_id, "Unknown")
                        all_stats.append(stats)
                except DataNotFoundError:
                    continue

        if not all_stats:
            raise DataNotFoundError(
                f"No stats found for competition {competition_id}, "
                f"season(s) {season_ids}."
            )
        return pd.concat(all_stats, ignore_index=True)

    def get_player_shooting_season(
        self,
        competition_id: int,
        season_ids: int | list[int],
        players: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch aggregated shooting stats per player across one or more
        seasons. A player who appears in multiple seasons gets one row
        per season.

        Args:
            competition_id: StatsBomb competition ID.
            season_ids: A single StatsBomb season ID or a list of them.
            players: Optional list of exact player names to filter for.
                    Significantly faster than loading all players.

        Returns:
            DataFrame with columns: player, team, season_id, season_name,
            matches_played, position, shots, shots_on_target, goals,
            total_xg, xg_per_shot. matches_played is the number of matches
            in the open dataset that contributed to this row — StatsBomb's
            open data doesn't cover every team's full season for every
            competition/season, so a low matches_played relative to a
            normal ~38-match La Liga season signals partial coverage
            rather than a genuinely quiet season.

        Raises:
            DataNotFoundError: If no data is found for the given season(s).
        """
        group_cols = ["player", "team", "season_id", "season_name"]
        raw_data = self._aggregate_season_stats(
            competition_id, season_ids, self.get_player_shooting_match, players
        )
        season_stats = (
            raw_data.groupby(group_cols)
            .agg(
                matches_played=("player", "count"),
                shots=("shots", "sum"),
                shots_on_target=("shots_on_target", "sum"),
                goals=("goals", "sum"),
                total_xg=("total_xg", "sum"),
            )
            .reset_index()
        )
        season_stats = season_stats.merge(
            self._primary_positions(raw_data, group_cols[:3]),
            on=group_cols[:3],
        )
        season_stats["xg_per_shot"] = (
            season_stats["total_xg"] / season_stats["shots"]
        ).round(3)
        season_stats["total_xg"] = season_stats["total_xg"].round(3)
        season_stats = season_stats[
            [
                "player",
                "team",
                "season_id",
                "season_name",
                "matches_played",
                "position",
                "shots",
                "shots_on_target",
                "goals",
                "total_xg",
                "xg_per_shot",
            ]
        ]
        return season_stats.sort_values(
            ["season_id", "total_xg"], ascending=[True, False]
        )

    def get_player_passing_season(
        self,
        competition_id: int,
        season_ids: int | list[int],
        players: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch aggregated passing stats per player across one or more
        seasons. A player who appears in multiple seasons gets one row
        per season.

        Args:
            competition_id: StatsBomb competition ID.
            season_ids: A single StatsBomb season ID or a list of them.
            players: Optional list of exact player names to filter for.
                     Significantly faster than loading all players.

        Returns:
            DataFrame with columns: player, team, season_id, season_name,
            matches_played, position, passes, passes_completed,
            completion_rate, progressive_passes, line_breaking_passes,
            assists. matches_played is the number of matches in the open
            dataset that contributed to this row — see
            get_player_shooting_season for why that matters.

        Raises:
            DataNotFoundError: If no data is found for the given season(s).
        """
        group_cols = ["player", "team", "season_id", "season_name"]
        raw_data = self._aggregate_season_stats(
            competition_id, season_ids, self.get_player_passing_match, players
        )
        season_stats = (
            raw_data.groupby(group_cols)
            .agg(
                matches_played=("player", "count"),
                passes=("passes", "sum"),
                passes_completed=("passes_completed", "sum"),
                progressive_passes=("progressive_passes", "sum"),
                line_breaking_passes=("line_breaking_passes", "sum"),
                assists=("assists", "sum"),
            )
            .reset_index()
        )
        season_stats = season_stats.merge(
            self._primary_positions(raw_data, group_cols[:3]),
            on=group_cols[:3],
        )
        season_stats["completion_rate"] = (
            (season_stats["passes_completed"] / season_stats["passes"]) * 100
        ).round(1)
        season_stats = season_stats[
            [
                "player",
                "team",
                "season_id",
                "season_name",
                "matches_played",
                "position",
                "passes",
                "passes_completed",
                "completion_rate",
                "progressive_passes",
                "line_breaking_passes",
                "assists",
            ]
        ]
        return season_stats.sort_values(
            ["season_id", "passes"], ascending=[True, False]
        )

    def get_player_defensive_season(
        self,
        competition_id: int,
        season_ids: int | list[int],
        players: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch aggregated defensive stats per player across one or more
        seasons. A player who appears in multiple seasons gets one row
        per season.

        Args:
            competition_id: StatsBomb competition ID.
            season_ids: A single StatsBomb season ID or a list of them.
            players: Optional list of exact player names to filter for.
                     Significantly faster than loading all players.

        Returns:
            DataFrame with columns: player, team, season_id, season_name,
            matches_played, position, tackles, interceptions, clearances.
            matches_played is the number of matches in the open dataset
            that contributed to this row — see get_player_shooting_season
            for why that matters.

        Raises:
            DataNotFoundError: If no data is found for the given season(s).
        """
        group_cols = ["player", "team", "season_id", "season_name"]
        raw_data = self._aggregate_season_stats(
            competition_id, season_ids, self.get_player_defensive_match, players
        )
        season_stats = (
            raw_data.groupby(group_cols)
            .agg(
                matches_played=("player", "count"),
                tackles=("tackles", "sum"),
                interceptions=("interceptions", "sum"),
                clearances=("clearances", "sum"),
            )
            .reset_index()
        )
        season_stats = season_stats.merge(
            self._primary_positions(raw_data, group_cols[:3]),
            on=group_cols[:3],
        )
        season_stats = season_stats[
            [
                "player",
                "team",
                "season_id",
                "season_name",
                "matches_played",
                "position",
                "tackles",
                "interceptions",
                "clearances",
            ]
        ]
        return season_stats.sort_values(
            ["season_id", "tackles"], ascending=[True, False]
        )

    def get_player_goals_assists_season(
        self,
        competition_id: int,
        season_ids: int | list[int],
        players: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch aggregated goals and assists per player across one or more
        seasons. A player who appears in multiple seasons gets one row
        per season.

        Args:
            competition_id: StatsBomb competition ID.
            season_ids: A single StatsBomb season ID or a list of them.
            players: Optional list of exact player names to filter for.
                    Significantly faster than loading all players.

        Returns:
            DataFrame with columns: player, team, season_id, season_name,
            matches_played, position, goals, assists. matches_played is
            the number of matches in the open dataset where the player
            actually appeared (via sb.lineups), not just matches where
            they scored or assisted — goals/assists are too sparse per
            match for "contributed to this row" (get_player_shooting_season's
            caveat) to mean actual appearances the way it does for shots/
            passes/defensive actions.

        Raises:
            DataNotFoundError: If no data is found for the given season(s).
        """
        group_cols = ["player", "team", "season_id", "season_name"]
        raw_data = self._aggregate_season_stats(
            competition_id, season_ids, self.get_player_goals_assists_match, players
        )
        season_stats = (
            raw_data.groupby(group_cols)
            .agg(
                matches_played=("player", "count"),
                goals=("goals", "sum"),
                assists=("assists", "sum"),
            )
            .reset_index()
        )
        season_stats = season_stats.merge(
            self._primary_positions(raw_data, group_cols[:3]),
            on=group_cols[:3],
        )
        season_stats = season_stats[
            [
                "player",
                "team",
                "season_id",
                "season_name",
                "matches_played",
                "position",
                "goals",
                "assists",
            ]
        ]
        return season_stats.sort_values(
            ["season_id", "goals"], ascending=[True, False]
        )

    def get_shots_season(
        self,
        competition_id: int,
        season_ids: int | list[int],
        teams: list[str] | None = None,
        players: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch raw shot-level data across one or more seasons, optionally
        filtered by team and/or player. Unlike the per-player season
        methods, this returns one row per shot rather than aggregating —
        feed it directly into shot-based analytics functions such as
        get_match_xg_summary() for team-level totals, get_player_xg_ranking(),
        or get_xg_overperformance(), which all work on any shots DataFrame
        regardless of how many matches or seasons it spans.

        Args:
            competition_id: StatsBomb competition ID.
            season_ids: A single StatsBomb season ID or a list of them.
            teams: Optional list of exact team names to filter for. Avoids
                     loading shots for irrelevant matches.
            players: Optional list of exact player names to filter for.
                     Avoids loading shots for irrelevant matches.

        Returns:
            DataFrame with columns: player, team, position, minute,
            shot_statsbomb_xg, shot_outcome, season_id, season_name.

        Raises:
            DataNotFoundError: If no shots are found for the given season(s).
        """
        return self._aggregate_season_stats(
            competition_id, season_ids, self.get_shots, players=players, teams=teams
        )
