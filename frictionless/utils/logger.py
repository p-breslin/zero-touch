import logging
import sys


class CustomFormatter(logging.Formatter):
    """A log formatter that adjusts output formatting based on the log level.

    INFO-level logs use a concise format, while DEBUG and higher levels use a detailed format including timestamp, logger name, and line number.
    """

    # Define a detailed format for DEBUG and higher-level warnings/errors
    detailed_format = (
        "| %(levelname)-8s | %(asctime)s | %(name)s:%(lineno)d | %(message)s"
    )

    # Define a concise format for INFO messages
    concise_format = "| %(levelname)-8s | %(asctime)s | %(message)s"

    def __init__(self):
        super().__init__(fmt="%(levelno)d: %(msg)s", datefmt="%H:%M:%S")
        self.formatters = {
            logging.INFO: logging.Formatter(self.concise_format, datefmt=self.datefmt),
            logging.DEBUG: logging.Formatter(
                self.detailed_format, datefmt=self.datefmt
            ),
            logging.WARNING: logging.Formatter(
                self.detailed_format, datefmt=self.datefmt
            ),
            logging.ERROR: logging.Formatter(
                self.detailed_format, datefmt=self.datefmt
            ),
            logging.CRITICAL: logging.Formatter(
                self.detailed_format, datefmt=self.datefmt
            ),
        }

    def format(self, record: logging.LogRecord) -> str:
        """Selects the appropriate log format dynamically based on log level.

        Args:
            record (LogRecord): The log record being processed.

        Returns:
            str: The formatted log string.
        """
        formatter = self.formatters.get(record.levelno)
        return formatter.format(record)


def setup_logging(
    level: int = logging.INFO,
    stream=None,
) -> None:
    """Initializes the root logger with a custom formatter and optional output stream.

    Applies different formats based on severity, suppresses noisy third-party logs,
    and clears preexisting handlers to avoid duplication.

    Args:
        level (int): Minimum log level to process (default: INFO).
        stream (Optional[Any]): If provided, logs are written to sys.stdout. Otherwise, no default stream handler is added.
    """
    # Get the root logger
    root_logger = logging.getLogger()

    # Clear any existing handlers to prevent duplicate logs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Create a handler to output to the console
    handler = logging.StreamHandler(sys.stdout if stream else None)

    # Set our custom formatter on the handler
    handler.setFormatter(CustomFormatter())

    # Add the configured handler to the root logger
    root_logger.addHandler(handler)

    # Set the minimum level of logs to process
    root_logger.setLevel(level)

    # Quieten noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Use a special logger name to avoid the verbose format for this one message
    logging.getLogger("setup_logger").info(
        f"Logging configured with level: {logging.getLevelName(level)}"
    )
