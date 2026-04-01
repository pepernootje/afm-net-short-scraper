# afm-net-short-scraper

Downloads and stores all net short position reports published by the Dutch financial
regulator [AFM](https://www.afm.nl). No HTML scraping is needed — AFM exposes direct
CSV export endpoints that return clean, semicolon-delimited data.

## Setup

```bash
uv sync
uv run python -m scraper.main --init-only
```

## Usage

```bash
# One-shot: fetch both current and archive registers
uv run python -m scraper.main

# Fetch only the current register (~1 000 rows)
uv run python -m scraper.main --current

# Fetch only the archive register (~20 000 rows)
uv run python -m scraper.main --archive

# Initialise DB schema only, then exit
uv run python -m scraper.main --init-only

# Run as a daemon (fires daily at 18:30 Amsterdam time)
uv run python -m scraper.scheduler
```

## App

A two-page Streamlit app that reads from `short_positions.db` and visualises the aggregated data.

**Page 1 — Market overview:** sortable table of all issuers with current total short, holder count, and all-time peak. Click any row to drill in.

**Page 2 — Issuer detail:** four Altair charts (total short over time, stacked area by holder, individual holder lines, current holders bar) with a date range filter (preset buttons + custom picker). Navigate back via the sidebar.

```bash
uv run streamlit run app/main.py
```

Opens at `http://localhost:8501`. Use the sidebar dropdown to jump directly to any issuer, or click a row in the overview table.

### Skipping aggregation during development

```bash
# Scrape only, skip the aggregation step
uv run python -m scraper.main --skip-aggregation

# Re-run aggregation on its own (reads existing positions table)
uv run python -c "from analytics.aggregator import run_aggregation; run_aggregation()"
```

## Tests

```bash
uv run pytest -v
uv run pytest --cov=scraper --cov-report=term-missing
```

## Database schema

### `positions`

| Column               | Type    | Notes                          |
|----------------------|---------|--------------------------------|
| `id`                 | INTEGER | Primary key                    |
| `position_holder`    | TEXT    | Fund / firm name               |
| `issuer_name`        | TEXT    | Company being shorted          |
| `isin`               | TEXT    | ISIN code                      |
| `net_short_position` | REAL    | Position as % of share capital |
| `position_date`      | TEXT    | YYYY-MM-DD                     |
| `source`             | TEXT    | `current` or `archive`         |
| `scraped_at`         | TEXT    | UTC ISO datetime               |

Unique constraint: `(isin, position_holder, position_date)`.

### `run_log`

| Column          | Type    | Notes                  |
|-----------------|---------|------------------------|
| `id`            | INTEGER | Primary key            |
| `run_at`        | TEXT    | UTC ISO datetime       |
| `rows_fetched`  | INTEGER |                        |
| `rows_inserted` | INTEGER |                        |
| `rows_updated`  | INTEGER |                        |
| `status`        | TEXT    | `ok` or `error`        |
| `message`       | TEXT    | Error message if any   |

## Deployment

**System cron** (runs at 18:30 Amsterdam time, Mon–Fri):

```cron
30 17 * * 1-5  cd /path/to/afm-net-short-scraper && uv run python -m scraper.main
```

**GitHub Actions** — the included workflow (`.github/workflows/scrape.yml`) runs
automatically on weekdays at 17:30 UTC, commits the updated `short_positions.db` and
any new daily CSV snapshot back to the repository. Trigger it manually via
`workflow_dispatch` as needed.
