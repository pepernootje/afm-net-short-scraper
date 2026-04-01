import logging
from io import StringIO

import requests
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_exponential,
)

from scraper.config import (
    AFM_ARCHIVE_CSV_URL,
    AFM_CURRENT_CSV_URL,
    BACKOFF_WAIT_MAX,
    BACKOFF_WAIT_MIN,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    USER_AGENT,
)

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/csv,text/plain,*/*",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}


@retry(
    retry=__import__("tenacity").retry_if_exception_type(
        (requests.ConnectionError, requests.Timeout)
    ),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(min=BACKOFF_WAIT_MIN, max=BACKOFF_WAIT_MAX),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _download(url: str) -> StringIO:
    logger.info("Downloading %s", url)
    response = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
    response.encoding = response.apparent_encoding or "utf-8-sig"
    response.raise_for_status()
    return StringIO(response.text)


def fetch_current() -> StringIO:
    return _download(AFM_CURRENT_CSV_URL)


def fetch_archive() -> StringIO:
    return _download(AFM_ARCHIVE_CSV_URL)
