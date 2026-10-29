"""
Pipeline-local disk cache for StatsBomb network calls, so repeated local
`python -m scripts.data_pipeline.build` runs during development don't
re-fetch the same data from StatsBomb's open-data host. StatsBombClient
itself only caches in-memory, for one process's lifetime (see
football_analytics/data/statsbomb_client.py); this adds cross-run
persistence, scoped to this one pipeline rather than the installable
package, since StatsBomb's open data never changes once published, so
there is no invalidation logic to get right.
"""

import pickle
from pathlib import Path
from typing import Callable

from football_analytics.data import statsbomb_client

_CACHED_FUNCTIONS = ["events", "lineups", "matches", "competitions"]


def _cache_key(kwargs: dict) -> str:
    return "_".join(f"{k}-{v}" for k, v in sorted(kwargs.items())) or "default"


def _cached(subdir: Path, fn: Callable) -> Callable:
    """Wrap fn so a call's result is pickled under subdir, keyed by its
    kwargs, and reused on the next call with the same kwargs."""
    subdir.mkdir(parents=True, exist_ok=True)

    def wrapped(**kwargs):
        path = subdir / f"{_cache_key(kwargs)}.pkl"
        if path.exists():
            with path.open("rb") as f:
                return pickle.load(f)
        result = fn(**kwargs)
        with path.open("wb") as f:
            pickle.dump(result, f)
        return result

    return wrapped


def install_disk_cache(cache_dir: Path) -> None:
    """
    Wrap StatsBombClient's sb.events/sb.lineups/sb.matches/sb.competitions
    with disk-cached versions, for this process's lifetime. Call once,
    before building any StatsBombClient instance.
    """
    for name in _CACHED_FUNCTIONS:
        original = getattr(statsbomb_client.sb, name)
        setattr(statsbomb_client.sb, name, _cached(cache_dir / name, original))
