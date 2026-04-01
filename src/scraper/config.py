from pathlib import Path

AFM_CURRENT_CSV_URL = (
    "https://www.afm.nl/export.aspx"
    "?type=8a46a4ef-f196-4467-a7ab-1ae1cb58f0e7&format=csv"
)
AFM_ARCHIVE_CSV_URL = (
    "https://www.afm.nl/export.aspx"
    "?type=3ca31b3d-23d9-4fa2-b846-29c7e3f0e5ff&format=csv"
)

BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = BASE_DIR / "short_positions.db"
LOG_PATH = BASE_DIR / "scraper.log"

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
BACKOFF_WAIT_MIN = 2
BACKOFF_WAIT_MAX = 30

USER_AGENT = (
    "afm-net-short-scraper/0.1.0 "
    "(https://github.com/user/afm-net-short-scraper; educational use)"
)

SCHEDULE_HOUR = 18
SCHEDULE_MINUTE = 30
SCHEDULE_TIMEZONE = "Europe/Amsterdam"

COLUMN_MAP = {
    "Positie houder": "position_holder",
    "Naam van de emittent": "issuer_name",
    "ISIN": "isin",
    "Netto Shortpositie": "net_short_position",
    "Positiedatum": "position_date",
}

DUTCH_MONTHS = {
    "jan": 1, "feb": 2, "mrt": 3, "apr": 4,
    "mei": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "okt": 10, "nov": 11, "dec": 12,
}
