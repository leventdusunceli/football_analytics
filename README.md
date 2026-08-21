# football-analytics

[![CI](https://github.com/leventdusunceli/football_analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/leventdusunceli/football_analytics/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A Python package for retrieving, analyzing, and visualizing football data.

In modern football, statistical analysis is central to how clubs scout players, prepare
for matches, and evaluate performance. `football-analytics` pulls data from two
complementary sources, StatsBomb's open event data and the football-data.org REST API,
normalizes it into pandas DataFrames, and layers analytics and visualization tools on
top. It allows us to visualize data from match and season level. 

This is a portfolio project: the goal is to demonstrate practical package design,
testing discipline, and data engineering judgment.

## Features

Package contains the following features:

- **Match-level analysis**: shots, expected goals (xG), passing (including progressive and
  line-breaking passes), defensive actions (tackles, interceptions, blocks, cards, and
  more), goals and assists, all per player or per team for a single match.
- **Season-level analysis**: the same statistics aggregated across a season, with
  automatic per-season match filtering by player or team to avoid loading irrelevant
  matches.
- **Team-level analysis**: recent form, points per game, home/away splits, clean
  standings, and actual-vs-xG-expected league position.
- **Visualizations**: shot maps and passing maps on a pitch, season bubble-chart
  profiles (shooting, passing), and a defensive-stats bar chart profile, all built on
  matplotlib.
- **Custom exception hierarchy** so callers can catch `DataNotFoundError`,
  `AuthenticationError`, or `APIError` instead of provider-specific errors leaking
  through.
- **Tested and linted on every push**: `pytest` and `ruff` run in CI on every push and
  pull request against `main`.

## Installation

Requires Python 3.10+ (developed and tested on 3.11). Not published to PyPI, so install
directly from a clone:

```bash
git clone https://github.com/leventdusunceli/football_analytics.git
cd football_analytics
pip install -e ".[dev]"    # editable install with test/lint tooling
# or: pip install -e .     # runtime dependencies only
```

## Configuration

StatsBomb's open data requires no credentials. football-data.org requires a free API
key:

1. Register for one at <https://www.football-data.org/client/register>.
2. Copy `.env.example` to `.env` and set `FOOTBALL_DATA_API_KEY`.

`FootballDataClient` reads this automatically via `python-dotenv`; `StatsBombClient`
needs nothing.

Note that football-data.org's free tier only serves recent seasons (older seasons
return a 403), while StatsBomb's open data is exclusively historical, so combining both
sources for the same season isn't always possible. See the demo notebook's Section 5
for how this is handled in practice.

## Quick start

```python
from football_analytics.data import StatsBombClient
from football_analytics.analytics import get_match_xg_summary

client = StatsBombClient()
shots = client.get_shots(match_id=3773457)
get_match_xg_summary(shots)
```

```python
from football_analytics.data import FootballDataClient
from football_analytics.analytics import get_clean_standings

client = FootballDataClient()  # reads FOOTBALL_DATA_API_KEY from .env
raw_standings = client.get_standings(competition="la_liga", season=2024)
get_clean_standings(raw_standings)
```

## Project structure

```text
football-analytics/
├── football_analytics/               # the installable package
│   ├── __init__.py                   # package version
│   │
│   ├── data/                         # Layer 1: raw data access (the only layer that talks to the data sources)
│   │   │                             
│   │   ├── statsbomb_client.py       # StatsBombClient: wraps statsbombpy data for match and season events 
│   │   │                             # (attacking and defending stats)
│   │   └── football_data_client.py   # FootballDataClient: wraps the football-data.org REST API for standings, matches, and fixtures
│   │                                 
│   │
│   ├── analytics/                    # Layer 2: pure pandas transforms, no network I/O
│   │   ├── xg.py                     # xG summaries, player rankings, over/under-performance, built from data/ shot output
│   │   │                             
│   │   ├── passing.py                # team-level passing summaries, built from data/ per-player passing output
│   │   │                             
│   │   ├── form.py                   # recent form, points per game, home/away splits, built from data/ match output
│   │   │                             
│   │   └── standings.py              # clean standings + xG-vs-actual position, combines data/ standings with analytics/xg.py
│   │                                 
│   │
│   ├── visualization/                # Layer 3: matplotlib charts built on data/ and analytics/ output
│   │   │                            
│   │   ├── match.py                  # single-match pitch maps (shots, passes)
│   │   └── player.py                 # season-level profile charts (shooting, passing, defensive bar chart)
│   │                                 
│   │
│   ├── models/                       # reserved for a future Pydantic layer, e.g. if
│   │                                 # a REST API or database is built on this
│   │                                 # package; pandas DataFrames are the data model
│   │                                 # for now
│   │
│   └── utils/
│       └── exceptions.py             # shared exception hierarchy. Every layer above raises these 
│                                     # instead of a raw requests/pandas error
│
├── tests/                            # one test file per module above, pytest + unittest.mock (no real network calls)
│   │                                 
│   └── conftest.py                   # shared test fixtures
│
├── notebooks/
│   └── football_analytics_demo.ipynb # the main portfolio artifact: match-level,
│                                     # season-level (player), and season-level
│                                     # (team) walkthroughs using every module above
│
├── .github/workflows/ci.yml          # ruff + pytest on every push/PR to main
├── pyproject.toml                    # package metadata, dependencies, tool config
└── README.md
```

Data flows strictly downward through the three layers in `football_analytics/`:

1. `data/` fetches and lightly cleans raw API responses into DataFrames or dicts
2. `analytics/` takes those DataFrames and computes statistics, with no awareness of
where the data came from
3. `visualization/` takes `analytics/`'s (or `data/`'s) output
and draws it.

Nothing in `data/` imports from `analytics/` or `visualization/`, and
nothing in `analytics/` imports from `visualization/`, so each layer can be tested and
reused independently. `utils/exceptions.py` is the one module every layer depends on.

## Testing

```bash
pytest              # run the test suite
ruff check .         # lint (also runs in CI)
```

Tests mock every external call, `requests.Session.get` for `FootballDataClient` and
`statsbombpy`'s functions for `StatsBombClient`, so the suite runs fully offline.

## Demo notebook

`notebooks/football_analytics_demo.ipynb` exercises the package end to end against
real StatsBomb and football-data.org data: exploring available StatsBomb competitions,
match-level shot/passing/defensive analysis with pitch-map visualizations,
season-level player comparisons (forwards, midfielders, defenders) with profile
charts, and team-level season analysis (form, standings, expected-vs-actual position).

## License

MIT, see [LICENSE](LICENSE).
