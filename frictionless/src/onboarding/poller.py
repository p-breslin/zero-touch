import logging
import sys
import time

log = logging.getLogger(__name__)


class PollResult:
    """Represents the outcome of a single polling iteration.

    Attributes:
        done (bool): True if polling should stop.
        value: The value to return when done is True.
        info: Optional metadata or status info from the predicate.
    """

    def __init__(self, done: bool, value=None, info=None):
        self.done = done
        self.value = value
        self.info = info


def wait_for(
    poll_status,
    *poll_args,
    interval: float,
    timeout: float,
    on_retry=None,
    on_timeout=None,
):
    """Generic polling loop.

    Repeatedly calls `poll_status(*poll_args)` until it returns a PollResult with done=True or until the timeout is reached.

    Args:
        poll_status (callable): Function returning PollResult.
        *poll_args: Positional args to pass into poll_status on each call.
        interval (float): Seconds to wait between calls.
        timeout (float): Maximum total seconds to poll before aborting.
        on_retry (callable[[PollResult], None], optional):
            Called with the last PollResult before each sleep.
        on_timeout (callable[[float], None], optional):
            Called with elapsed seconds if timeout is exceeded.

    Returns:
        The `.value` attribute from the successful PollResult.

    Exits via sys.exit(1) if the timeout is exceeded.
    """
    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            if on_timeout:
                on_timeout(elapsed)
            log.error("Polling timed out after %.1f seconds", elapsed)
            sys.exit(1)

        result = poll_status(*poll_args)
        if result.done:
            return result.value

        if on_retry:
            on_retry(result)

        time.sleep(interval)
