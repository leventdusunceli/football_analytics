class FootballAnalyticsError(Exception):
    """Base exception for the football-analytics package"""

    pass


class APIError(FootballAnalyticsError):
    """Raised when an API request fails"""

    pass


class AuthenticationError(FootballAnalyticsError):
    """Raised when API key is not balid or missing"""

    pass


class DataNotFoundError(FootballAnalyticsError):
    """Raised whe the requested data doesnn't exist"""

    pass
