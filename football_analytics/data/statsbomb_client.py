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

    def get_competitions(self)->pd.DataFrame: 
        """
        List all competitions available in StatsBomb open data.
        
        Returns: 
            DataFrame of available competitions and seasons.
        """
        return sb.competitions()
    
    def get_matches(self,competition_id:int,season_id:int, team:str|None=None,)-> pd.DataFrame:
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
        matches = sb.matches(competition_id=competition_id,season_id=season_id)
        if matches.empty: 
            raise DataNotFoundError(
                f"No matches found for competition {competition_id},"
                f"season {season_id}"
            )
        
        if team: 
            matches = matches[
                (matches['home_team']==team) |
                (matches['away_team']==team)
            ].copy()
            if matches.empty: 
                raise DataNotFoundError(
                    f"No matches found for team '{team} in competition "
                    f"{competition_id}, season {season_id}. Make sure the "
                    f"team name matches StatsBomb team name exactly"
                )
            return matches
        
    def get_events(self,match_id:int)-> pd.DataFrame: 
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

