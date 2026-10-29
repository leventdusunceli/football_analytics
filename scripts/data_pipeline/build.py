"""
Builds every static JSON file the web demo reads. Run from the repo root:

    python -m scripts.data_pipeline.build
"""

import datetime
import json
from pathlib import Path

from football_analytics.data import StatsBombClient
from scripts.data_pipeline import config, match_stats, season_stats, team_stats
from scripts.data_pipeline import standings as standings_pipeline
from scripts.data_pipeline.cache import install_disk_cache
from scripts.data_pipeline.write import write_table


def _write_meta(data_dir: Path, teams: list[str]) -> None:
    meta = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "competition_id": config.COMPETITION_ID,
        "full_season_id": config.FULL_SEASON_ID,
        "trend_season_ids": config.TREND_SEASON_IDS,
        "trend_eligible_players": config.TREND_ELIGIBLE_PLAYERS,
        "teams": sorted(teams),
        "default_team": "Barcelona",
    }
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / "meta.json").open("w") as f:
        json.dump(meta, f, indent=2)


def main() -> None:
    install_disk_cache(Path(config.CACHE_DIR))
    client = StatsBombClient()
    data_dir = Path(config.DATA_DIR)

    # Full roster first: this is the one expensive, full-season pull, and
    # every other table below either reuses its already-cached match
    # events or is cheap on its own.
    full_roster = season_stats.build_full_roster_tables(client)
    for name, df in full_roster.items():
        write_table(df, data_dir / "players" / "season-27" / f"{name}.json")

    for name, df in season_stats.build_trend_tables(client).items():
        write_table(df, data_dir / "players" / "trend" / f"{name}.json")

    for name, df in team_stats.build_team_tables(client, full_roster).items():
        write_table(df, data_dir / "teams" / "season-27" / f"{name}.json")

    for name, df in standings_pipeline.build_standings_tables(client).items():
        write_table(df, data_dir / "standings" / f"{name}.json")

    match_index = match_stats.build_match_index(client)
    write_table(match_index, data_dir / "match" / "index.json")

    for match_id in match_index["match_id"]:
        match_dir = data_dir / "match" / str(match_id)
        for name, df in match_stats.build_match_tables(client, match_id).items():
            write_table(df, match_dir / f"{name}.json")

    _write_meta(data_dir, teams=full_roster["roster"]["team"].unique().tolist())
    print(f"Wrote static data to {data_dir}")


if __name__ == "__main__":
    main()
