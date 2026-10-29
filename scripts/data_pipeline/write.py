"""
Writes a DataFrame to the {"columns": [...], "rows": [...]} JSON shape the
web demo's JS reads directly into Plotly/D3.
"""

import json
import math
from pathlib import Path

import pandas as pd


def _to_native(value):
    """Convert a numpy/pandas scalar to a plain, JSON-serializable Python
    type. Lists (StatsBomb's location columns) pass through unchanged."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def write_table(df: pd.DataFrame, path: Path) -> None:
    """
    Write df as {"columns": [...], "rows": [...]} JSON at path, creating
    parent directories as needed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        [_to_native(v) for v in row] for row in df.itertuples(index=False, name=None)
    ]
    payload = {"columns": df.columns.tolist(), "rows": rows}
    with path.open("w") as f:
        json.dump(payload, f)
