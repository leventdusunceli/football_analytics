"""
Standings analytics.
Enriches raw standings data with xG-based position analysis.
"""

import pandas as pd

from football_analytics.utils.exceptions import DataNotFoundError


def get_clean_standings(raw_standings: dict) -> pd.DataFrame:
    """
    Parse raw standings API response into a clean DataFrame.
    ARgs:
        raw_standings: Raw dict returned by FootballDataClient.get_standings
    Returns:
        DataFrame with columns: position, team, played, won, drawn,
        lost, goals_for, goals_against, goal_difference, points.

    Raises:
        DataNotFoundError: If standings data cannot be parsed.
    """
    try:
        table = raw_standings["standings"][0]["table"]
    except (KeyError, IndexError) as e:
        raise DataNotFoundError(f"Could not parse standings data:{e}")

    rows = []
    for entry in table:
        rows.append(
            {
                "position": entry["position"],
                "team": entry["team"]["name"],
                "played": entry["playedGames"],
                "won": entry["won"],
                "drawn": entry["draw"],
                "lost": entry["lost"],
                "goals_for": entry["goalsFor"],
                "goals_against": entry["goalsAgainst"],
                "goal_difference": entry["goalDifference"],
                "points": entry["points"],
            }
        )

    if not rows:
        raise DataNotFoundError("Standings table is empty.")

    return pd.DataFrame(rows)


def get_standings_from_matches(
    matches: pd.DataFrame, teams: list[str] | None = None
) -> pd.DataFrame:
    """
    Build a standings table directly from StatsBomb match results, for
    seasons/teams football-data.org's free tier doesn't cover.

    Args:
        matches: DataFrame from StatsBombClient.get_matches, needs
            home_team, away_team, home_score, away_score.
        teams: Optional list of exact team names to restrict the table to.
            Defaults to every team appearing in matches.

    Returns:
        DataFrame with the same columns as get_clean_standings: position,
        team, played, won, drawn, lost, goals_for, goals_against,
        goal_difference, points. Ranked by points, then goal_difference,
        then goals_for.

    Raises:
        DataNotFoundError: If matches is empty, or none of teams appear
            in it.
    """
    if matches.empty:
        raise DataNotFoundError("matches is empty.")

    target_teams = teams or sorted(
        set(matches["home_team"]) | set(matches["away_team"])
    )

    rows = []
    for team in target_teams:
        home = matches[matches["home_team"] == team]
        away = matches[matches["away_team"] == team]
        played = len(home) + len(away)
        if played == 0:
            continue

        goals_for = home["home_score"].sum() + away["away_score"].sum()
        goals_against = home["away_score"].sum() + away["home_score"].sum()
        won = int(
            (home["home_score"] > home["away_score"]).sum()
            + (away["away_score"] > away["home_score"]).sum()
        )
        drawn = int(
            (home["home_score"] == home["away_score"]).sum()
            + (away["away_score"] == away["home_score"]).sum()
        )

        rows.append(
            {
                "team": team,
                "played": played,
                "won": won,
                "drawn": drawn,
                "lost": played - won - drawn,
                "goals_for": int(goals_for),
                "goals_against": int(goals_against),
                "goal_difference": int(goals_for - goals_against),
                "points": won * 3 + drawn,
            }
        )

    if not rows:
        raise DataNotFoundError("None of the requested teams appear in matches.")

    table = (
        pd.DataFrame(rows)
        .sort_values(["points", "goal_difference", "goals_for"], ascending=False)
        .reset_index(drop=True)
    )
    table.insert(0, "position", table.index + 1)

    return table[
        [
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
        ]
    ]


def get_clean_sheets(
    matches: pd.DataFrame, teams: list[str] | None = None
) -> pd.DataFrame:
    """
    Count clean sheets (matches the opponent scored zero in) per team,
    directly from match results.

    Args:
        matches: DataFrame from StatsBombClient.get_matches, needs
            home_team, away_team, home_score, away_score.
        teams: Optional list of exact team names to restrict to.
            Defaults to every team appearing in matches.

    Returns:
        DataFrame with columns: team, clean_sheets, matches_played,
        sorted by clean_sheets descending.

    Raises:
        DataNotFoundError: If matches is empty, or none of teams appear
            in it.
    """
    if matches.empty:
        raise DataNotFoundError("matches is empty.")

    target_teams = teams or sorted(
        set(matches["home_team"]) | set(matches["away_team"])
    )

    rows = []
    for team in target_teams:
        home = matches[matches["home_team"] == team]
        away = matches[matches["away_team"] == team]
        played = len(home) + len(away)
        if played == 0:
            continue
        clean_sheets = int(
            (home["away_score"] == 0).sum() + (away["home_score"] == 0).sum()
        )
        rows.append(
            {"team": team, "clean_sheets": clean_sheets, "matches_played": played}
        )

    if not rows:
        raise DataNotFoundError("None of the requested teams appear in matches.")

    return (
        pd.DataFrame(rows)
        .sort_values("clean_sheets", ascending=False)
        .reset_index(drop=True)
    )


def get_expected_vs_actual(
    standings: pd.DataFrame,
    team_xg: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compare a team's actual league position against their xG-implied
    expected position.

    Args:
        standings: DataFrame returned by get_clean_standings.
        team_xg: DataFrame with columns 'team' and 'total_xg', typically aggregated
        from StatsBomb shot data.

    Returns:
        DataFrame with columns: team, actual_position, xg_position,
        position_difference. Positive position_difference means the team
        is finishing higher than their xG suggests they should.

    Raises:
        DataNotFoundError: If the merge produces no matching teams.
    """
    merged = standings[["position", "team"]].merge(
        team_xg[["team", "total_xg"]], on="team", how="inner"
    )

    if merged.empty:
        raise DataNotFoundError(
            "No teams matched between standings and xG data. "
            "Check that team names are consistent between both sources."
        )

    merged = merged.sort_values("total_xg", ascending=False).reset_index(drop=True)
    merged["xg_position"] = merged.index + 1
    merged = merged.sort_values("position").reset_index(drop=True)
    merged["position_difference"] = merged["xg_position"] - merged["position"]

    return merged.rename(columns={"position": "actual_position"})[
        ["team", "actual_position", "xg_position", "position_difference"]
    ]
