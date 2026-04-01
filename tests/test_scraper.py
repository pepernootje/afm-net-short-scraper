import sqlite3
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest

from analytics import aggregator
from scraper import parser, storage
from scraper.parser import _parse_dutch_date

_SAMPLE_CSV = (
    "Positie houder;Naam van de emittent;ISIN;Netto Shortpositie;Positiedatum\n"
    "Marshall Wace LLP;Signify N.V.;NL0011821392;1.87;30 mrt 2026\n"
    "AQR Capital Management, LLC;Adyen N.V.;NL0012969182;0.90;30 mrt 2026\n"
    "Marshall Wace LLP;Signify N.V.;NL0011821392;1.87;30 mrt 2026\n"
)


def _sample_io() -> StringIO:
    return StringIO(_SAMPLE_CSV)


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

def test_parse_dutch_date_valid():
    assert _parse_dutch_date("30 mrt 2026") == date(2026, 3, 30)
    assert _parse_dutch_date("01 jan 2024") == date(2024, 1, 1)


def test_parse_dutch_date_invalid():
    assert _parse_dutch_date("not a date") is None
    assert _parse_dutch_date("") is None
    assert _parse_dutch_date(None) is None


def test_parse_deduplicates():
    df = parser.parse(_sample_io(), source="current")
    assert len(df) == 2


def test_parse_types():
    df = parser.parse(_sample_io(), source="current")
    assert pd.api.types.is_float_dtype(df["net_short_position"])
    assert all(isinstance(v, date) for v in df["position_date"])


def test_parse_source_label():
    df = parser.parse(_sample_io(), source="archive")
    assert all(df["source"] == "archive")


def test_merge_prefers_current():
    current = parser.parse(_sample_io(), source="current")
    archive = parser.parse(_sample_io(), source="archive")
    merged = parser.merge(current, archive)
    assert all(merged["source"] == "current")


# ---------------------------------------------------------------------------
# Storage tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.db"
    storage.init_db(path)
    return path


def test_init_db_creates_tables(db_path: Path):
    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert "positions" in tables
    assert "run_log" in tables
    conn.close()


def test_upsert_inserts(db_path: Path):
    df = parser.parse(_sample_io(), source="current")
    result = storage.upsert(df, db_path)
    assert result["inserted"] == 2
    assert result["updated"] == 0


def test_upsert_idempotent(db_path: Path):
    df = parser.parse(_sample_io(), source="current")
    storage.upsert(df, db_path)
    result = storage.upsert(df, db_path)
    assert result["inserted"] == 0
    assert result["updated"] == 0


def test_upsert_detects_update(db_path: Path):
    df = parser.parse(_sample_io(), source="current")
    storage.upsert(df, db_path)

    modified = df.copy()
    modified.loc[0, "net_short_position"] = 2.50
    result = storage.upsert(modified, db_path)
    assert result["updated"] == 1


def test_query_latest(db_path: Path):
    df = parser.parse(_sample_io(), source="current")
    storage.upsert(df, db_path)
    result = storage.query_latest(db_path)
    assert len(result) == 2


def test_log_run(db_path: Path):
    storage.log_run(100, 50, 5, "ok", None, db_path)
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT * FROM run_log ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    conn.close()


# ---------------------------------------------------------------------------
# Aggregator tests
# ---------------------------------------------------------------------------

# Three holders on the same ISIN over Mon–Wed 2026-03-23..25:
#   Holder A: 1.50 Mon, 1.80 Wed  → Tue should be filled at 1.50
#   Holder B: 0.80 Mon, 0.40 Wed  → Wed is terminal; Thu must NOT be filled
#   Holder C: 0.60 Mon only        → Tue+ should be filled (open position)
_SAMPLE_POSITIONS = [
    ("Holder A", "Issuer X", "NL0001", 1.50, date(2026, 3, 23)),
    ("Holder A", "Issuer X", "NL0001", 1.80, date(2026, 3, 25)),
    ("Holder B", "Issuer X", "NL0001", 0.80, date(2026, 3, 23)),
    ("Holder B", "Issuer X", "NL0001", 0.40, date(2026, 3, 25)),
    ("Holder C", "Issuer X", "NL0001", 0.60, date(2026, 3, 23)),
]


@pytest.fixture()
def agg_db(tmp_path: Path) -> Path:
    path = tmp_path / "agg_test.db"
    storage.init_db(path)
    scraped_at = datetime.now(timezone.utc).isoformat()
    with storage.get_connection(path) as conn:
        for holder, issuer, isin, pos, pos_date in _SAMPLE_POSITIONS:
            conn.execute(
                """INSERT INTO positions
                   (position_holder, issuer_name, isin, net_short_position,
                    position_date, source, scraped_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (holder, issuer, isin, pos, pos_date.isoformat(), "test", scraped_at),
            )
    aggregator.run_aggregation(db_path=path)
    return path


def _trader(db_path: Path) -> pd.DataFrame:
    with storage.get_connection(db_path) as conn:
        return pd.read_sql_query("SELECT * FROM daily_position_trader", conn)


def _issuer(db_path: Path) -> pd.DataFrame:
    with storage.get_connection(db_path) as conn:
        return pd.read_sql_query("SELECT * FROM daily_position_issuer", conn)


def _row(df: pd.DataFrame, **kwargs) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for col, val in kwargs.items():
        mask &= df[col] == val
    rows = df[mask]
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)} for {kwargs}"
    return rows.iloc[0]


def test_agg_tables_created(agg_db: Path):
    conn = sqlite3.connect(agg_db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "daily_position_trader" in tables
    assert "daily_position_issuer" in tables
    conn.close()


def test_holder_a_tuesday_filled(agg_db: Path):
    df = _trader(agg_db)
    row = _row(df, position_holder="Holder A", date="2026-03-24")
    assert row["net_short_position"] == pytest.approx(1.50)
    assert row["is_filled"] == 1


def test_holder_a_wednesday_reported(agg_db: Path):
    df = _trader(agg_db)
    row = _row(df, position_holder="Holder A", date="2026-03-25")
    assert row["net_short_position"] == pytest.approx(1.80)
    assert row["is_filled"] == 0


def test_holder_b_wednesday_terminal_kept(agg_db: Path):
    df = _trader(agg_db)
    row = _row(df, position_holder="Holder B", date="2026-03-25")
    assert row["net_short_position"] == pytest.approx(0.40)
    assert row["is_filled"] == 0


def test_holder_b_thursday_absent(agg_db: Path):
    df = _trader(agg_db)
    rows = df[(df["position_holder"] == "Holder B") & (df["date"] == "2026-03-26")]
    assert len(rows) == 0, "Holder B should have no row on Thu (terminal on Wed)"


def test_holder_c_tuesday_filled(agg_db: Path):
    df = _trader(agg_db)
    row = _row(df, position_holder="Holder C", date="2026-03-24")
    assert row["is_filled"] == 1


def test_issuer_monday_total(agg_db: Path):
    df = _issuer(agg_db)
    row = _row(df, isin="NL0001", date="2026-03-23")
    assert row["total_net_short"] == pytest.approx(1.50 + 0.80 + 0.60)
    assert row["active_holders"] == 3


def test_issuer_wednesday_total(agg_db: Path):
    df = _issuer(agg_db)
    row = _row(df, isin="NL0001", date="2026-03-25")
    # Holder A: 1.80, Holder B: 0.40 (own reporting date), Holder C: ffill 0.60
    assert row["total_net_short"] == pytest.approx(1.80 + 0.40 + 0.60)
    assert row["active_holders"] == 3


def test_issuer_thursday_holder_b_absent(agg_db: Path):
    df = _issuer(agg_db)
    row = _row(df, isin="NL0001", date="2026-03-26")
    # Only Holder A (1.80 ffilled) + Holder C (0.60 ffilled)
    assert row["total_net_short"] == pytest.approx(1.80 + 0.60)
    assert row["active_holders"] == 2
