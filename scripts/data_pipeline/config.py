"""
Scope of the web demo's precomputed data: which StatsBomb competition,
seasons, players and match the pipeline builds static JSON for.
"""

COMPETITION_ID = 11  # La Liga

# The one season with full, every-team, every-match coverage. Used for
# the full player roster, the full standings, the team-comparison stats,
# and the full match index/all match-level data.
FULL_SEASON_ID = 27  # 2015/2016

# 26 = 2014/2015 (Barcelona-heavy, partial coverage for other clubs).
# Only used together with FULL_SEASON_ID for the nine-player trend chart,
# see TREND_ELIGIBLE_PLAYERS below.
TREND_SEASON_IDS = [FULL_SEASON_ID, 26]

FORWARDS = [
    "Lionel Andrés Messi Cuccittini",
    "Luis Alberto Suárez Díaz",
    "Cristiano Ronaldo dos Santos Aveiro",
]
MIDFIELDERS = [
    "Luka Modrić",
    "Sergio Busquets i Burgos",
    "Andrés Iniesta Luján",
]
DEFENDERS = [
    "Raphaël Varane",
    "Javier Alejandro Mascherano",
    "Diego Roberto Godín Leal",
]
PLAYERS = FORWARDS + MIDFIELDERS + DEFENDERS

# Players with real multi-match coverage in BOTH seasons, verified against
# the open dataset directly (get_player_shooting_season) while planning
# this pipeline. The other four players have only 1-2 matches in season
# 26, not enough for a meaningful two-season trend line, so the site
# shows them as a single labeled data point instead of a connected trend.
TREND_ELIGIBLE_PLAYERS = [
    "Lionel Andrés Messi Cuccittini",
    "Luis Alberto Suárez Díaz",
    "Andrés Iniesta Luján",
    "Sergio Busquets i Burgos",
    "Javier Alejandro Mascherano",
]

DATA_DIR = "webapp/public/data"
CACHE_DIR = ".cache/statsbomb"
