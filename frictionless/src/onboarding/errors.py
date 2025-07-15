class RetryableError(Exception):
    """Raised when an API call failed but should be retried (e.g. 500 'Please wait')."""


class FatalApiError(Exception):
    """Raised for non-recoverable API errors."""
