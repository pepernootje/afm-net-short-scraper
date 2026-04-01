import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from scraper.config import DATA_DIR, DB_PATH

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS positions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    position_holder     TEXT    NOT NULL,
    issuer_name         TEXT    NOT NULL,
    isin                TEXT    NOT NULL,
    net_short_position  REAL    NOT NULL,
    position_date       TEXT    NOT NULL,
    source              TEXT    NOT NULL,
    scraped_at          TEXT    NOT NULL,
    UNIQUE (isin, position_holder, position_date)
);

CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT    NOT NULL,
    rows_fetched    INTEGER NOT NULL,
    rows_inserted   INTEGER NOT NULL,
    rows_updated    INTEGER NOT NULL,
    status          TEXT    NOT NULL,
    message         TEXT
);

CREATE TABLE IF NOT EXISTS daily_position_trader (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    position_holder     TEXT    NOT NULL,
    issuer_name         TEXT    NOT NULL,
    isin                TEXT    NOT NULL,
    date                TEXT    NOT NULL,
    net_short_position  REAL    NOT NULL,
    is_filled           INTEGER NOT NULL DEFAULT 0,
    UNIQUE (isin, position_holder, date)
);

CREATE TABLE IF NOT EXISTS daily_position_issuer (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer_name         TEXT    NOT NULL,
    isin                TEXT    NOT NULL,
    date                TEXT    NOT NULL,
    total_net_short     REAL    NOT NULL,
    active_holders      INTEGER NOT NULL,
    UNIQUE (isin, date)
);

CREATE INDEX IF NOT EXISTS idx_positions_isin   ON positions (isin);
CREATE INDEX IF NOT EXISTS idx_positions_date   ON positions (position_date);
CREATE INDEX IF NOT EXISTS idx_positions_holder ON positions (position_holder);
CREATE INDEX IF NOT EXISTS idx_dpt_isin_date    ON daily_position_trader (isin, date);
CREATE INDEX IF NOT EXISTS idx_dpi_isin_date    ON daily_position_issuer (isin, date);
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(_DDL)
    logger.info("Database initialised at %s", db_path)


def upsert(df: pd.DataFrame, db_path: Path = DB_PATH) -> dict[str, int]:
    scraped_at = datetime.now(timezone.utc).isoformat()
    inserted = 0
    updated = 0

    with get_connection(db_path) as conn:
        for _, row in df.iterrows():
            pos_date = (
                row["position_date"].isoformat()
                if isinstance(row["position_date"], date)
                else str(row["position_date"])
            )
            existing = conn.execute(
                "SELECT id, net_short_position FROM positions "
                "WHERE isin = ? AND position_holder = ? AND position_date = ?",
                (row["isin"], row["position_holder"], pos_date),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """INSERT INTO positions
                       (position_holder, issuer_name, isin, net_short_position,
                        position_date, source, scraped_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["position_holder"],
                        row["issuer_name"],
                        row["isin"],
                        float(row["net_short_position"]),
                        pos_date,
                        row["source"],
                        scraped_at,
                    ),
                )
                inserted += 1
            elif abs(float(existing["net_short_position"]) - float(row["net_short_position"])) > 1e-6:
                conn.execute(
                    """UPDATE positions
                       SET net_short_position = ?, source = ?, scraped_at = ?
                       WHERE id = ?""",
                    (
                        float(row["net_short_position"]),
                        row["source"],
                        scraped_at,
                        existing["id"],
                    ),
                )
                updated += 1

    logger.info("Upsert complete: inserted=%d updated=%d", inserted, updated)
    return {"inserted": inserted, "updated": updated}


def log_run(
    rows_fetched: int,
    rows_inserted: int,
    rows_updated: int,
    status: str,
    message: str | None = None,
    db_path: Path = DB_PATH,
) -> None:
    run_at = datetime.now(timezone.utc).isoformat()
    with get_connection(db_path) as conn:
        conn.execute(
            """INSERT INTO run_log
               (run_at, rows_fetched, rows_inserted, rows_updated, status, message)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_at, rows_fetched, rows_inserted, rows_updated, status, message),
        )


def write_csv_snapshot(df: pd.DataFrame, snapshot_date: date | None = None) -> Path:
    if snapshot_date is None:
        snapshot_date = date.today()
    path = DATA_DIR / f"{snapshot_date.isoformat()}.csv"
    out = df.copy()
    out["position_date"] = out["position_date"].apply(
        lambda d: d.isoformat() if isinstance(d, date) else d
    )
    out.to_csv(path, index=False)
    logger.info("CSV snapshot written to %s (%d rows)", path, len(df))
    return path


def query_latest(db_path: Path = DB_PATH) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        return pd.read_sql_query(
            "SELECT * FROM positions ORDER BY position_date DESC, position_holder",
            conn,
        )
