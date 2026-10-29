"""Tests for write.py's DataFrame-to-JSON shape."""

import json
import math

import pandas as pd

from scripts.data_pipeline.write import write_table


def test_write_table_shape(tmp_path):
    df = pd.DataFrame({"player": ["Messi", "Suárez"], "goals": [43, 16]})
    path = tmp_path / "out" / "shooting.json"

    write_table(df, path)

    with path.open() as f:
        payload = json.load(f)
    assert payload["columns"] == ["player", "goals"]
    assert payload["rows"] == [["Messi", 43], ["Suárez", 16]]


def test_write_table_nan_becomes_null(tmp_path):
    df = pd.DataFrame({"player": ["Messi"], "xg_per_shot": [math.nan]})
    path = tmp_path / "shooting.json"

    write_table(df, path)

    with path.open() as f:
        payload = json.load(f)
    assert payload["rows"] == [["Messi", None]]


def test_write_table_preserves_location_lists(tmp_path):
    df = pd.DataFrame({"player": ["Messi"], "location": [[60.0, 40.0]]})
    path = tmp_path / "shots.json"

    write_table(df, path)

    with path.open() as f:
        payload = json.load(f)
    assert payload["rows"] == [["Messi", [60.0, 40.0]]]
