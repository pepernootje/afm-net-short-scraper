"""
Aggregates raw positions into daily time series per trader and per issuer.

Pipeline
--------
1. Load raw positions from `positions` table.
2. Build a Mon–Fri business-day calendar from first observation to today.
3. For each (position_holder, isin) group:
   - Snap any weekend dates to the following Monday.
   - Reindex to the full calendar.
   - Forward-fill, but stop propagating once a terminal row (< 0.5%) is seen.
4. Write expanded rows to `daily_position_trader`.
5. Aggregate to `daily_position_issuer`.
"""

import logging
from datetime import date

import numpy as np
import pandas as pd

from scraper.config import DB_PATH
from scraper.storage import get_connection, init_db

logger = logging.getLogger(__name__)

_THRESHOLD = 0.5  # AFM reporting threshold


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_positions(conn) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT position_holder, issuer_name, isin, net_short_position, position_date "
        "FROM positions ORDER BY isin, position_holder, position_date",
        conn,
    )
    df["position_date"] = pd.to_datetime(df["position_date"]).dt.date
    return df


def _build_calendar(start: date, end: date) -> np.ndarray:
    return pd.bdate_range(start=start, end=end, freq="B").date


def _snap_to_monday(d: date) -> date:
    """Move weekend dates to the following Monday."""
    weekday = d.weekday()
    if weekday == 5:  # Saturday
        return date.fromordinal(d.toordinal() + 2)
    if weekday == 6:  # Sunday
        return date.fromordinal(d.toordinal() + 1)
    return d


def _expand_group(group: pd.DataFrame, calendar: np.ndarray) -> pd.DataFrame:
    """
    Expand one (position_holder, isin) group to the full business-day calendar.

    Forward-fill strategy
    ---------------------
    - Normal rows (>= 0.5%): fill forward indefinitely across gaps.
    - Terminal rows (< 0.5%): keep the row itself, but do NOT propagate the
      value to subsequent filled days (position dropped below the threshold).
    - Filled rows that inherit a terminal flag are dropped.
    - Rows before the first real report are dropped (no value to fill from).
    """
    meta = {
        "position_holder": group["position_holder"].iloc[0],
        "issuer_name": group["issuer_name"].iloc[0],
        "isin": group["isin"].iloc[0],
    }

    # Snap weekend dates to Monday before pivoting
    group = group.copy()
    group["position_date"] = group["position_date"].apply(_snap_to_monday)
    # If snapping caused duplicates (two rows land on same Monday), keep last
    group = group.sort_values("position_date").drop_duplicates(
        subset=["position_date"], keep="last"
    )

    s = group.set_index("position_date")["net_short_position"]
    s = s.reindex(calendar)

    # Identify which rows were originally reported vs filled
    was_reported = s.notna()

    # Mark terminal reports: a reported row with value < threshold
    is_terminal = pd.Series(False, index=s.index)
    is_terminal[was_reported] = s[was_reported] < _THRESHOLD

    # Forward-fill position values across gaps
    s_filled = s.ffill()

    # Forward-fill the terminal flag so gaps after a terminal row inherit it
    terminal_filled = is_terminal.copy()
    # Only propagate True into NaN gaps (not into subsequent reported rows)
    # We achieve this by ffill on the terminal flag, then mask reported rows
    # back to their actual terminal status.
    terminal_propagated = terminal_filled.replace(False, np.nan).ffill().fillna(False).astype(bool)
    terminal_propagated[was_reported] = is_terminal[was_reported]

    is_filled_col = (~was_reported).astype(int)

    # Build the result series
    result = pd.DataFrame({
        "net_short_position": s_filled,
        "is_filled": is_filled_col,
        "is_terminal_propagated": terminal_propagated,
    })

    # Drop rows with no value (before first report)
    result = result.dropna(subset=["net_short_position"])

    # Drop filled rows that carry a terminal flag (position gone below 0.5%)
    kill = (result["is_filled"] == 1) & result["is_terminal_propagated"]
    result = result[~kill]

    result = result.drop(columns=["is_terminal_propagated"])
    result.index.name = "date"
    result = result.reset_index()

    for col, val in meta.items():
        result[col] = val

    return result


def _write_trader(df: pd.DataFrame, conn) -> int:
    conn.execute("DELETE FROM daily_position_trader")
    rows = 0
    for _, row in df.iterrows():
        conn.execute(
            """INSERT OR REPLACE INTO daily_position_trader
               (position_holder, issuer_name, isin, date, net_short_position, is_filled)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                row["position_holder"],
                row["issuer_name"],
                row["isin"],
                str(row["date"]),
                float(row["net_short_position"]),
                int(row["is_filled"]),
            ),
        )
        rows += 1
    return rows


def _write_issuer(df: pd.DataFrame, conn) -> int:
    conn.execute("DELETE FROM daily_position_issuer")
    rows = 0
    for _, row in df.iterrows():
        conn.execute(
            """INSERT OR REPLACE INTO daily_position_issuer
               (issuer_name, isin, date, total_net_short, active_holders)
               VALUES (?, ?, ?, ?, ?)""",
            (
                row["issuer_name"],
                row["isin"],
                str(row["date"]),
                float(row["total_net_short"]),
                int(row["active_holders"]),
            ),
        )
        rows += 1
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_aggregation(db_path=DB_PATH) -> dict[str, int]:
    """
    Full pipeline: load → expand → ffill → aggregate → write.
    Returns {"trader_rows": N, "issuer_rows": N}.
    """
    init_db(db_path)

    with get_connection(db_path) as conn:
        raw = _load_positions(conn)

    if raw.empty:
        logger.warning("No positions found — skipping aggregation")
        return {"trader_rows": 0, "issuer_rows": 0}

    calendar = _build_calendar(raw["position_date"].min(), date.today())
    logger.info(
        "Aggregating %d raw rows over %d business days (%s → %s)",
        len(raw), len(calendar), calendar[0], calendar[-1],
    )

    groups = []
    for (holder, isin), grp in raw.groupby(["position_holder", "isin"], sort=False):
        expanded = _expand_group(grp.reset_index(drop=True), calendar)
        if not expanded.empty:
            groups.append(expanded)
        else:
            logger.debug("Empty expansion for %s / %s — skipped", holder, isin)

    if not groups:
        logger.warning("All groups expanded to empty — nothing to write")
        return {"trader_rows": 0, "issuer_rows": 0}

    trader_df = pd.concat(groups, ignore_index=True)

    issuer_df = (
        trader_df
        .groupby(["isin", "issuer_name", "date"], as_index=False)
        .agg(
            total_net_short=("net_short_position", "sum"),
            active_holders=("net_short_position", lambda s: (s > 0).sum()),
        )
    )

    with get_connection(db_path) as conn:
        trader_rows = _write_trader(trader_df, conn)
        issuer_rows = _write_issuer(issuer_df, conn)

    logger.info(
        "Aggregation complete: trader_rows=%d issuer_rows=%d",
        trader_rows, issuer_rows,
    )
    return {"trader_rows": trader_rows, "issuer_rows": issuer_rows}
