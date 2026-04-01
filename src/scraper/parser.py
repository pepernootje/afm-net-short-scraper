import logging
from datetime import date
from io import StringIO

import pandas as pd

from scraper.config import COLUMN_MAP, DUTCH_MONTHS

logger = logging.getLogger(__name__)

_EXPECTED_INTERNAL_COLS = set(COLUMN_MAP.values())


def _parse_dutch_date(value) -> date | None:
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    # ISO format: "2026-03-30" or "2026-03-30 00:00:00"
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    # Dutch format: "30 mrt 2026"
    parts = value.split()
    if len(parts) != 3:
        return None
    try:
        day = int(parts[0])
        month = DUTCH_MONTHS.get(parts[1].lower())
        year = int(parts[2])
        if month is None:
            return None
        return date(year, month, day)
    except (ValueError, TypeError):
        return None


def parse(csv_io: StringIO, source: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_io, sep=";", dtype=str)
    except Exception:
        csv_io.seek(0)
        df = pd.read_csv(csv_io, sep=",", dtype=str)

    df.columns = [c.strip() for c in df.columns]

    rename_map = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    missing = _EXPECTED_INTERNAL_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns after rename: {missing}")

    df = df[list(_EXPECTED_INTERNAL_COLS)].copy()

    for col in ("position_holder", "issuer_name", "isin"):
        df[col] = df[col].str.strip()

    df["net_short_position"] = pd.to_numeric(
        df["net_short_position"].str.replace(",", ".", regex=False),
        errors="coerce",
    )

    df["position_date"] = df["position_date"].apply(_parse_dutch_date)

    before = len(df)
    df = df.dropna(subset=["isin", "position_holder", "position_date"])
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows with missing key fields", dropped)

    df["source"] = source

    df = df.drop_duplicates(subset=["isin", "position_holder", "position_date"])

    return df.reset_index(drop=True)


def merge(current_df: pd.DataFrame, archive_df: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([current_df, archive_df], ignore_index=True)
    return combined.drop_duplicates(
        subset=["isin", "position_holder", "position_date"],
        keep="first",
    ).reset_index(drop=True)
