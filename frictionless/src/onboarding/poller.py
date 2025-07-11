import logging
import sys
import time

log = logging.getLogger(__name__)


class PollResult:
    """Represents the outcome of a single polling iteration.

    Attributes:
        done (bool): True if polling should stop.
        value: The value to return when done is True.
        info: Optional metadata or status info from predicate_fn.
    """

    def __init__(self, done: bool, value=None, info=None):
        self.done = done
        self.value = value
        self.info = info


def wait_for(
    predicate_fn, interval: float, timeout: float, on_retry=None, on_timeout=None
):
    """Polls a specified API over a time-range.

    Repeatedly calls `predicate_fn` until it returns a PollResult with done=True or
    until the timeout is reached.

    Args:
        predicate_fn (callable[[], PollResult]): A zero-argument function that returns a PollResult.
        interval (float): Seconds to wait between calls.
        timeout (float): Maximum total seconds to poll before aborting.
        on_retry (callable[[PollResult], None], optional): Called after each   unsuccessful PollResult, before sleeping
        on_timeout (callable[[float], None], optional): Called once with elapsed time
        if timeout is exceeded.

    Returns:
        The `.value` attribute from the successful PollResult.

    Exits with sys.exit(1) if the timeout is exceeded.
    """
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            if on_timeout:
                on_timeout(elapsed)
            log.error("Polling timed out after %.1f seconds", elapsed)
            sys.exit(1)

        result = predicate_fn()
        if result.done:
            return result.value

        if on_retry:
            on_retry(result)

        time.sleep(interval)
