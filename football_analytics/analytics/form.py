"""
Team form analytics.
Calculates recent form, points per game, and home/away splits
from match data returned by FootballDataClient.
"""

import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def _parse_matches(raw_matches: dict, team: str) -> pd.DataFrame:
    """
    Internal helper that extracts and normalises a team's match results
    from the raw API response into a clean DataFrame.

    Args:
        raw_matches: Raw match dict returned by FootballDataClient.get_matches.
        team: The team name to filter for, must match API data exactly.

    Returns:
        DataFrame with columns: date, venue, goals_for, goals_against, result.

    Raises:
        DataNotFoundError: If no matches are found for the given team.
    """
