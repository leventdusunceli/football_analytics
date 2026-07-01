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
    return df


def _points_from_result(result: str) -> int:
    """Returns points earned for a given match result"""
    return {"W": 3, "D": 1, "L": 0}.get(result, 0)


def get_recent_form(raw_matches: dict, team: str, last_n: int = 5) -> pd.DataFrame:
    """
    Calculate a team's recent form over their last N matches

    Args:
        raw_matches: Raw match dict returned by FootballDataClient.get_matches.
        team: Team name as it appears in the API data.
        last_n: Number of most recent matches to include. Defaults to 5.

    Returns:
        DataFrame of the last N matches with columns: date, venue,
        goals_for, goals_against, result, points.

    Raises:
        DataNotFoundError: If no matches are found for the given team.
    """

    df = _parse_matches(raw_matches, team)
    recent = df.tail(last_n).copy()
    recent["points"] = recent["result"].apply(_points_from_result)
    return recent.reset_index(drop=True)


def get_points_per_game(raw_matches: dict, team: str) -> float:
    """
    Calculate a team's points per game across all available matches.

    Args:
        raw_matches: Raw match dict returned by FootballDataClient.get_matches.
        team: Team name as it appears in the API data.

    Returns: Points per game as float rounded to two decimal points

    Raises: DataNotFoundError: if no matches are found for the given team.
    """
    df = _parse_matches(raw_matches, team)
    df["points"] = df["result"].apply(_points_from_result)
    return round(df["points"].mean(), 2)


def get_home_away_split(raw_matches: dict, team: str) -> pd.DataFrame:
    """
    Calculate a team's home and away record for a given season

    Args:
        raw_matches: Raw match dict returned by FootballDataClient.get_matches.
        team: Team name as it appears in the API data.
    Returns:
        DataFrame indexed by venue (home/away) with columns:
        played, wins, draws, losses, goals_for, goals_against, points_per_game.

    Raises: DataNotFoundError: if no matches are found for the given team.
    """
    df = _parse_matches(raw_matches, team)
    df["points"] = df["result"].apply(_points_from_result)

    split = (
        df.groupby("venue")
        .agg(
            played=("result", "count"),
            wins=("result", lambda x: (x == "W").sum()),
            draws=("result", lambda x: (x == "D").sum()),
            losses=("result", lambda x: (x == "L").sum()),
            goals_for=("goals_for", "sum"),
            goals_against=("goals_against", "sum"),
            points_per_game=("points", "mean"),
        )
        .reset_index()
    )

    split["points_per_game"] = split["points_per_game"].round(2)
    return split
