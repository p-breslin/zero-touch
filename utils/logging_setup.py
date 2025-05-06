import sys
import logging


def setup_logging(
    level: int = logging.INFO,
    stream=None,
) -> None:
    """
    Configures the root logger.

    Args:
        level: default overall log level (INFO by default)
        stream: if truthy, log to sys.stdout; otherwise caller adds own handlers
    """
    stdout = sys.stdout if stream else None

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=stdout,
    )

    # Quieten noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    print(f"Logging configured with level: {logging.getLevelName(level)}")
