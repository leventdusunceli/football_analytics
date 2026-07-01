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
    merged["xg_ranking"] = merged.index + 1
    merged = merged.sort_values("position").reset_index(drop=True)
    merged["position_difference"] = merged["xg_ranking"] - merged["position"]

    return merged.rename(columns={"position": "season_standing"})[
        ["team", "actual_position", "xg_ranking", "position_difference"]
    ]
