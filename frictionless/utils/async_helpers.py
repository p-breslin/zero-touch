import asyncio
from concurrent.futures import ThreadPoolExecutor

from src.onboarding.file_uploader import upload_and_wait

_executor = ThreadPoolExecutor(max_workers=3)


async def file_upload_wrapper(client, file_info, base_path, interval, timeout):
    """Asynchronously runs the blocking `upload_and_wait` in a thread pool.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.
        file_info (dict): Dict with keys 'file', 'filetype', and 'description'.
        base_path (str): Filesystem path prefix where files live.
        interval (float): Seconds between poll attempts.
        timeout (float): Maximum seconds to wait before aborting.

    Returns:
        dict: The final status entry dict returned by `upload_and_wait` when processing is complete.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _executor,
        upload_and_wait,
        client,
        file_info,
        base_path,
        interval,
        timeout,
    )
