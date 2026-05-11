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

    rows = []
    for match in raw_matches.get("matches", []):
        home = match["homeTeam"]["name"]
        away = match["awayTeam"]["name"]
        score = match.get("score", {}).get("fullTime", {})

        if home == team:
            rows.append(
                {
                    "date": match["utcDate"],
                    "venue": "home",
                    "goals_for": score.get("home"),
                    "goals_against": score.get("away"),
                }
            )
        elif away == team:
            rows.append(
                {
                    "date": match["utcDate"],
                    "venue": "away",
                    "goals_for": score.get("away"),
                    "goals_against": score.get("home"),
                }
            )

    if not rows:
        raise DataNotFoundError(
            f"No matches found for team '{team}'. "
            "Make sure the name matches the API data exactly."
        )
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["goals_for", "goals_against"])
    df["result"] = df.apply(
        lambda row: (
            "W"
            if row["goals_for"] > row["goals_against"]
            else ("D" if row["goals_for"] == row["goal_against"] else "L")
        ),
        axis=1,
    )
