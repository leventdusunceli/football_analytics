"""Tests for football-data.org API (football__data_client.py)"""

from unittest.mock import MagicMock, patch

import pytest

from football_analytics.data.football_data_client import FootballDataClient
from football_analytics.utils.exceptions import APIError, AuthenticationError


def test_missing_api_key_raises_AuthenticationError():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(AuthenticationError):
            FootballDataClient(api_key=None)


def test_invalid_comp_raises_APIError():
    client = FootballDataClient(api_key="fake_key")
    with pytest.raises(APIError):
        client.get_standings("fake_league")


def test_get_standings_calls_correct_endpoint():
    client = FootballDataClient(api_key="fake_key")
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.json.return_value = {"standings": []}

    with patch.object(client.session, "get", return_value=mock_response) as mock_get:
        client.get_standings("premier_league")
        called_url = mock_get.call_args[0][0]
        assert "PL/standings" in called_url


def test_401_response_raises_authentication_error():
    client = FootballDataClient(api_key="fake_key")
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.ok = False

    with patch.object(client.session, "get", return_value=mock_response):
        with pytest.raises(AuthenticationError):
            client.get_standings("premier_league")


def test_500_response_raises_api_error():
    client = FootballDataClient(api_key="fake_key")
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.ok = False

    with patch.object(client.session, "get", return_value=mock_response):
        with pytest.raises(APIError):
            client.get_standings("premier_league")
