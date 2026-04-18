"""
football-data.org REST API client.
Retrieves match results, standings and fixtures for any given season
"""

import os
from datetime import datetime

import requests
from dotenv import load_dotenv

from football_analytics.utils.exceptions import APIError, AuthenticationError

load_dotenv()

BASE_URL = "https://api.football-data.org/v4"

COMPETITIONS = {
    # top 5 european leagues
    "premier_league": "PL",
    "la_liga": "PD",
    "bundesliga": "BL1",
    "serie_a": "SA",
    "ligue_1": "FL1",
    # european club competitions
    "champions_league": "CL",
    "europa_league": "EL",
    "conference_league": "UCL",
}  # https://docs.football-data.org/general/v4/lookup_tables.html for adding more competitions  # noqa: E501


def _current_season() -> int:
    """
    Returns the current season's start year.
    Football seasons start in August, so from August onwards
    the current season year increments.
    """
    now = datetime.now()
    return now.year if now.month >= 8 else now.year - 1


class FootballDataClient:
    """
    Wraps the football-data.org API

    Args:
        api_key: your personalized API key. reads from FOOTBALL_DATA_API_KEY
                environment variable if not provided
    """

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("FOOTBALL_DATA_API_KEY")
        if not self.api_key:
            raise AuthenticationError(
                "No API key provided. Set FOOTBALL_DATA_API_KEY in your .env file"
            )
        self.session = requests.Session()
        self.session.headers.update({"X-Auth-Token": self.api_key})

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """
        Internal method to make a GET request to the API.

        Args:
            endpoint: The API endpoint path (e.g. '/competitions/PL/standings').
            params: Optional query parameters.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            AuthenticationError: if the API key is missing or invalid (401)
            APIError: If the request fails or returns a non-200 status.
        """
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params)

        if response.status_code == 401:
            raise AuthenticationError("Invalid API key.")
        if response.status_code == 404:
            raise APIError(f"Resource not found: {endpoint}")
        if not response.ok:
            raise APIError(
                f"API request failed: {response.status_code} - {response.text}"
            )
        return response.json()

    def get_standings(
        self,
        competition: str,
        season: int = _current_season(),
    ) -> dict:
        """
        Fetch the current standings for a competition.

        Args:
            competition: One of 'premier_league', 'la_liga', 'bundesliga',
                        'serie_a', 'ligue_1', 'champions_league',
                        'europa_league', 'conference_league', 'eredivisie',
                        'primeira_liga', 'scottish_premiership'.
            season: Season start year as a four digit integer (e.g. 2025 for
                    the 2025/26 season). Defaults to the current season.

        Returns:
            Raw standings data as a dictionary.

        Raises:
            AuthenticationError: If the API key is missing or invalid (401).
            APIError: If the competition is invalid or the request fails.
        """
        comp_code = COMPETITIONS.get(competition)
        if not comp_code:
            raise APIError(
                f"Unknown competition '{competition}'."
                f"Currently available competitions: {list(COMPETITIONS.keys())}"
            )
        params: dict = {"season": season}
        return self._get(f"/competitions/{comp_code}/standings", params=params)

    def get_matches(
        self,
        competition: str,
        matchday: int | None = None,
        season: int = _current_season(),
    ) -> dict:
        """
        Fetch matches for a competition, optionally filtered by matchday
        and/or season.

        Args:
            competition: Competition key e.g. 'premier_league', 'la_liga'.
            matchday: Optional matchday number to filter results.
            season: Season start year e.g. 2022 for 2022/23. Defaults to
                    current season

        Returns:
            Raw match data as a dictionary.

        Raises:
            AuthenticationError: If the API key is missing or invalid (401).
            APIError: If the competition is invalid or the request fails.
        """
        comp_code = COMPETITIONS.get(competition)
        if not comp_code:
            raise APIError(
                f"Unknown competition '{competition}'."
                f"Currently available competitions: {list(COMPETITIONS.keys())}"
            )
        params: dict = {"season": season}
        if matchday:
            params["matchday"] = matchday
        return self._get(f"/competitions/{comp_code}/matches", params=params)

    def get_team_matches(
        self,
        team_id: int,
        season: int = _current_season(),
    ) -> dict:
        """
        Fetch all matches for a specific team in a given season.

        Args:
            team_id: football-data.org team ID
            season: Season start year e.g. 2022 for 2022/23. Defaults to
                    current season

            Returns:
                Raw match data as a dictionary.

            Raises:
                AuthenticationError: If the API key is missing or invalid (401).
                APIError: If the competition is invalid or the request fails.
        """
        params = {"season": season}
        return self._get(f"/teams/{team_id}/matches", params=params)

    def get_team(self, team_id: int) -> dict:
        """
        Fetch general club information such as but not limited to full name,
        founding year, home stadium, and a list of their current squad with basic
        player details for a specific team.

        Args:
            team_id: The football-data.org team ID.

        Returns:
            Raw team data as a dictionary.

        Raises:
            AuthenticationError: If the API key is missing or invalid (401).
            APIError: If the team is not found or the request fails.
        """
        return self._get(f"/teams/{team_id}")
