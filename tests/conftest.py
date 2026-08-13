"""
Shared pytest fixtures for football-analytics tests.
Fixtures provide fake data to be used by test modules.
"""

import pandas as pd
import pytest


@pytest.fixture
def sample_shots():
    """Fake shots data mimicking get_shots() output"""
    return pd.DataFrame(
        {
            "player": ["Saka", "Odegaard", "Havertz", "Palmer", "Jackson"],
            "team": ["Arsenal", "Arsenal", "Arsenal", "Chelsea", "Chelsea"],
            "minute": [12, 34, 67, 23, 88],
            "shot_statsbomb_xg": [0.342, 0.087, 0.521, 0.412, 0.231],
            "shot_outcome": ["Goal", "Missed", "Goal", "Saved", "Missed"],
        }
    )


@pytest.fixture
def sample_player_passing_stats():
    """Fake passing data mimicking get_player_passing_match() output"""
    return pd.DataFrame(
        {
            "player": ["Saka", "Odegaard", "Havertz", "Palmer"],
            "team": ["Arsenal", "Arsenal", "Arsenal", "Chelsea"],
            "passes": [40, 60, 30, 50],
            "passes_completed": [32, 54, 21, 40],
            "completion_rate": [80.0, 90.0, 70.0, 80.0],
            "progressive_passes": [10, 15, 5, 12],
            "line_breaking_passes": [2, 4, 1, 3],
        }
    )


@pytest.fixture
def sample_raw_matches():
    """Fake row matches mimicking football-data.org API response"""
    return {
        "matches": [
            {
                "utcDate": "2024-01-01T15:00:00Z",
                "homeTeam": {"name": "Arsenal"},
                "awayTeam": {"name": "Chelsea"},
                "score": {"fullTime": {"home": 2, "away": 1}},
            },
            {
                "utcDate": "2024-01-08T15:00:00Z",
                "homeTeam": {"name": "Liverpool"},
                "awayTeam": {"name": "Arsenal"},
                "score": {"fullTime": {"home": 1, "away": 1}},
            },
            {
                "utcDate": "2024-01-15T15:00:00Z",
                "homeTeam": {"name": "Arsenal"},
                "awayTeam": {"name": "Man City"},
                "score": {"fullTime": {"home": 0, "away": 2}},
            },
        ]
    }


@pytest.fixture
def sample_standings_df():
    """Fake clean standings DataFrame."""
    return pd.DataFrame(
        {
            "position": [1, 2, 3],
            "team": ["Arsenal", "Chelsea", "Liverpool"],
            "played": [20, 20, 20],
            "won": [14, 12, 10],
            "drawn": [3, 4, 5],
            "lost": [3, 4, 5],
            "goals_for": [42, 38, 35],
            "goals_against": [18, 22, 25],
            "goal_difference": [24, 16, 10],
            "points": [45, 40, 35],
        }
    )


@pytest.fixture
def sample_team_xg():
    """Fake team xG DataFrame for standings enrichment tests."""
    return pd.DataFrame(
        {
            "team": ["Arsenal", "Chelsea", "Liverpool"],
            "total_xg": [38.4, 41.2, 35.1],
        }
    )


@pytest.fixture
def sample_raw_standings():
    """Fake raw standing mimicking the output from football-org API."""
    return {
        "standings": [
            {
                "table": [
                    {
                        "position": 1,
                        "team": {"name": "Arsenal"},
                        "playedGames": 20,
                        "won": 14,
                        "draw": 3,
                        "lost": 3,
                        "goalsFor": 42,
                        "goalsAgainst": 18,
                        "goalDifference": 24,
                        "points": 45,
                    },
                    {
                        "position": 2,
                        "team": {"name": "Chelsea"},
                        "playedGames": 20,
                        "won": 12,
                        "draw": 4,
                        "lost": 4,
                        "goalsFor": 38,
                        "goalsAgainst": 22,
                        "goalDifference": 16,
                        "points": 40,
                    },
                ]
            }
        ]
    }


@pytest.fixture
def sample_player_stats():
    """Fake player stats DataFrame for player analytics tests."""
    return pd.DataFrame(
        {
            "player": ["Saka", "Odegaard", "Havertz", "Palmer", "Jackson"],
            "team": ["Arsenal", "Arsenal", "Arsenal", "Chelsea", "Chelsea"],
            "position": [
                "Right Wing",
                "Center Midfield",
                "Center Forward",
                "Right Back",
                "Center Forward",
            ],
            "goals": [15, 8, 12, 18, 6],
            "assists": [10, 12, 5, 7, 3],
            "total_xg": [12.3, 7.1, 10.8, 14.2, 5.9],
            "shots": [72, 45, 60, 88, 38],
            "minutes_played": [1800, 1650, 1710, 1890, 1200],
        }
    )
