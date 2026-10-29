"""Tests for cache.py's disk-cache hit/miss behavior."""

from unittest.mock import MagicMock, patch

from scripts.data_pipeline.cache import install_disk_cache


def test_install_disk_cache_reuses_result_on_second_call(tmp_path):
    fake_events = MagicMock(return_value={"fake": "events"})
    with (
        patch("football_analytics.data.statsbomb_client.sb.events", fake_events),
        patch("football_analytics.data.statsbomb_client.sb.lineups", MagicMock()),
        patch("football_analytics.data.statsbomb_client.sb.matches", MagicMock()),
        patch(
            "football_analytics.data.statsbomb_client.sb.competitions", MagicMock()
        ),
    ):
        install_disk_cache(tmp_path)
        from football_analytics.data import statsbomb_client

        result_1 = statsbomb_client.sb.events(match_id=1)
        result_2 = statsbomb_client.sb.events(match_id=1)

        assert result_1 == {"fake": "events"}
        assert result_2 == {"fake": "events"}
        fake_events.assert_called_once_with(match_id=1)


def test_install_disk_cache_is_per_argument_key(tmp_path):
    fake_events = MagicMock(side_effect=lambda match_id: {"match_id": match_id})
    with (
        patch("football_analytics.data.statsbomb_client.sb.events", fake_events),
        patch("football_analytics.data.statsbomb_client.sb.lineups", MagicMock()),
        patch("football_analytics.data.statsbomb_client.sb.matches", MagicMock()),
        patch(
            "football_analytics.data.statsbomb_client.sb.competitions", MagicMock()
        ),
    ):
        install_disk_cache(tmp_path)
        from football_analytics.data import statsbomb_client

        statsbomb_client.sb.events(match_id=1)
        statsbomb_client.sb.events(match_id=2)

        assert fake_events.call_count == 2
