from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from scraper.config import DB_PATH
from scraper.storage import get_connection


@st.cache_data(ttl=3600)
def get_overview(db_path: Path = DB_PATH) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                i.issuer_name,
                i.isin,
                i.date           AS latest_date,
                i.total_net_short,
                i.active_holders,
                p.peak_short
            FROM daily_position_issuer i
            JOIN (
                SELECT isin, MAX(total_net_short) AS peak_short
                FROM daily_position_issuer
                GROUP BY isin
            ) p ON i.isin = p.isin
            WHERE i.date = (
                SELECT MAX(date) FROM daily_position_issuer d2
                WHERE d2.isin = i.isin
            )
            ORDER BY i.total_net_short DESC
            """,
            conn,
        )
    df["latest_date"] = pd.to_datetime(df["latest_date"]).dt.date
    return df


@st.cache_data(ttl=3600)
def get_issuer_history(
    isin: str, start_date: date, end_date: date, db_path: Path = DB_PATH
) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT date, total_net_short, active_holders
            FROM daily_position_issuer
            WHERE isin = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            conn,
            params=(isin, start_date.isoformat(), end_date.isoformat()),
        )
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=3600)
def get_trader_history(
    isin: str, start_date: date, end_date: date, db_path: Path = DB_PATH
) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT date, position_holder, net_short_position, is_filled
            FROM daily_position_trader
            WHERE isin = ?
              AND date BETWEEN ? AND ?
            ORDER BY date, position_holder
            """,
            conn,
            params=(isin, start_date.isoformat(), end_date.isoformat()),
        )
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=3600)
def get_current_holders(isin: str, db_path: Path = DB_PATH) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        return pd.read_sql_query(
            """
            SELECT position_holder, net_short_position, is_filled, date
            FROM daily_position_trader
            WHERE isin = ?
              AND date = (
                  SELECT MAX(date) FROM daily_position_trader WHERE isin = ?
              )
            ORDER BY net_short_position DESC
            """,
            conn,
            params=(isin, isin),
        )


@st.cache_data(ttl=3600)
def get_all_issuers(db_path: Path = DB_PATH) -> list[tuple[str, str]]:
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT issuer_name, isin
            FROM daily_position_issuer
            ORDER BY issuer_name
            """,
            conn,
        )
    return list(df.itertuples(index=False, name=None))


@st.cache_data(ttl=3600)
def get_date_bounds(db_path: Path = DB_PATH) -> tuple[date, date]:
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT MIN(date), MAX(date) FROM daily_position_issuer"
        ).fetchone()
    return (
        date.fromisoformat(row[0]),
        date.fromisoformat(row[1]),
    )
