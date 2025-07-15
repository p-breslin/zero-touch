import datetime
import logging
import sys

from scripts.paths import DATA_DIR


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
    stream: bool = True,
    label: str = None,
) -> None:
    """Configures the root logger with a custom formatter and optional file output.

    Args:
        level (int): Minimum log level to process (default: INFO).
        stream (bool): If True, logs are written to stdout.
        label: (str, ptional): If provided, save logs to file using label as prefix.
    """
    root_logger = logging.getLogger()

    # Clear any existing handlers to prevent duplicate logs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Custom formatter to be set for the handler
    formatter = CustomFormatter()

    # Stream handler to console (stdout)
    if stream:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    # Save logs to file
    if not stream or label:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        if label:
            log_name = f"{label}_{timestamp}.log"
        else:
            log_name = f"logs_{timestamp}.log"

        savepath = DATA_DIR / "logs" / log_name
        savepath.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(savepath, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Minimum level of logs to process
    root_logger.setLevel(level)

    # Quieten noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # Use a special logger name to avoid the verbose format for this one message
    logging.getLogger("setup_logger").info(
        f"Logging configured with level: {logging.getLevelName(level)}"
    )
