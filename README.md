# football-analytics

[![CI](https://github.com/leventdusunceli/football_analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/leventdusunceli/football_analytics/actions/workflows/ci.yml)
[![Deploy web demo](https://github.com/leventdusunceli/football_analytics/actions/workflows/deploy-pages.yml/badge.svg)](https://github.com/leventdusunceli/football_analytics/actions/workflows/deploy-pages.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A Python package for retrieving, analyzing, and visualizing football data,
pulling from StatsBomb's open event data and the football-data.org REST
API, normalizing both into pandas DataFrames.

**Live demo: [LaLiga 2015/16 In Depth](https://leventdusunceli.github.io/football_analytics/)**,
an interactive site built entirely on this package's output (see
[Web demo](#web-demo)).

This is a portfolio project: the goal is to demonstrate practical package
design, testing discipline, and data engineering judgment.

## Features

- **Match and season-level analysis**: shots, xG, passing (progressive and
  line-breaking), defensive actions (tackles, interceptions, pressures,
  and more), dribbling, fouls won, minutes played, goals and assists, per
  player or per team, with automatic per-season match filtering to avoid
  loading irrelevant matches.
- **Team-level analysis**: standings (from football-data.org or built
  directly from StatsBomb matches), possession, clean sheets, recent form,
  points per game, home/away splits, and actual-vs-xG-expected position.
- **Visualizations**: shot/passing/defensive-action pitch maps, season
  profile charts, all built on matplotlib.
- **Custom exception hierarchy** (`DataNotFoundError`, `AuthenticationError`,
  `APIError`) instead of provider-specific errors leaking through.
- **Tested and linted on every push**: `pytest` and `ruff` run in CI.

## Installation

Requires Python 3.10+ (developed and tested on 3.11). Not published to PyPI:

```bash
git clone https://github.com/leventdusunceli/football_analytics.git
cd football_analytics
pip install -e ".[dev]"    # editable install with test/lint tooling
# or: pip install -e .     # runtime dependencies only
```

## Configuration

StatsBomb's open data needs no credentials. For football-data.org,
register for a free key at
<https://www.football-data.org/client/register>, copy `.env.example` to
`.env`, and set `FOOTBALL_DATA_API_KEY` (`FootballDataClient` reads it
automatically; `StatsBombClient` needs nothing). The free tier only
covers recent seasons, see `FootballDataClient`'s docstrings for detail.

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
│   ├── data/                         # raw data access: StatsBombClient, FootballDataClient
│   ├── analytics/                    # pure pandas transforms (xg, passing, form, standings, player)
│   ├── visualization/                # matplotlib charts (match, player, defense, timeline)
│   ├── models/                       # reserved for a future Pydantic layer
│   └── utils/exceptions.py           # shared exception hierarchy
│
├── tests/                            # one test file per module, pytest + unittest.mock
├── notebooks/football_analytics_demo.ipynb   # main portfolio artifact
├── scripts/data_pipeline/            # build tooling: fetches data, writes JSON for webapp/
├── webapp/                           # the web demo (static HTML/CSS/JS + Plotly/D3)
├── .github/workflows/                # ci.yml (test+lint), deploy-pages.yml (web demo)
└── pyproject.toml
```

Data flows one way: `data/` fetches and cleans raw responses,
`analytics/` computes statistics from them with no awareness of where
they came from, `visualization/` draws `analytics/`'s or `data/`'s
output. Each layer can be tested and reused independently.

## Testing

```bash
pytest              # run the test suite (package + data pipeline)
ruff check .        # lint (also runs in CI)
```

Tests mock every external call, so the suite runs fully offline.

## Demo notebook

`notebooks/football_analytics_demo.ipynb` exercises the package end to
end against real StatsBomb and football-data.org data: match-level
analysis with pitch maps, season-level player comparisons, and
team-level season analysis.

## Web demo

**[Open the live site](https://leventdusunceli.github.io/football_analytics/)**:
"LaLiga 2015/16 In Depth", a second portfolio artifact covering the full
2015/16 season (every player, full standings, all 380 matches), built
entirely from precomputed StatsBomb data. It has three views:

- **Team comparison**: goals vs. xG, possession, shots on target, assists
  and pressing intensity for any set of teams, with a summary table.
- **Player comparison**: any two players side by side, from games played
  and minutes to scoring, passing, dribbling and defending, plus a
  season-over-season trend for nine notable players.
- **Match view**: shot, passing and defensive-action maps for any of the
  380 matches, for a whole team or a single player.

Every view can be shared by link, works on phones, and explains each
advanced statistic on hover or tap.

How it's built: `scripts/data_pipeline/` calls this package to compute
every table and writes them as static JSON, and `webapp/` (plain
HTML/CSS/JS with Plotly and D3, no build step) reads them in the browser.
On every push to `main`, [deploy-pages.yml](.github/workflows/deploy-pages.yml)
rebuilds the data and publishes the site to GitHub Pages, so the live site
always matches the code.

`FootballDataClient` is intentionally excluded from the site, to keep it
free of secrets and external API calls. To build and preview it locally:

```bash
python -m scripts.data_pipeline.build   # first run ~15-20 min, cached afterwards
python -m scripts.serve_webapp          # then open http://localhost:8000
```

See [webapp/README.md](webapp/README.md) for details.

## License

MIT, see [LICENSE](LICENSE).
