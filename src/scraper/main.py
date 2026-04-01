import argparse
import logging
import sys

from analytics import aggregator
from scraper import fetcher, parser, storage
from scraper.config import DB_PATH
from scraper.logging_setup import configure_logging

logger = logging.getLogger(__name__)


def run(fetch_current: bool = True, fetch_archive: bool = True, skip_aggregation: bool = False) -> None:
    storage.init_db(DB_PATH)

    current_df = None
    archive_df = None

    if fetch_current:
        try:
            logger.info("Fetching current register…")
            csv_io = fetcher.fetch_current()
            current_df = parser.parse(csv_io, source="current")
            logger.info("Current register: %d rows parsed", len(current_df))
        except Exception as exc:
            logger.error("Failed to fetch/parse current register: %s", exc)
            storage.log_run(0, 0, 0, "error", str(exc), DB_PATH)
            if not fetch_archive:
                sys.exit(1)

    if fetch_archive:
        try:
            logger.info("Fetching archive register…")
            csv_io = fetcher.fetch_archive()
            archive_df = parser.parse(csv_io, source="archive")
            logger.info("Archive register: %d rows parsed", len(archive_df))
        except Exception as exc:
            logger.error("Failed to fetch/parse archive register: %s", exc)
            storage.log_run(0, 0, 0, "error", str(exc), DB_PATH)
            if current_df is None:
                sys.exit(1)

    if current_df is not None and archive_df is not None:
        merged = parser.merge(current_df, archive_df)
    elif current_df is not None:
        merged = current_df
    elif archive_df is not None:
        merged = archive_df
    else:
        logger.error("No data available — aborting")
        sys.exit(1)

    rows_fetched = len(merged)
    result = storage.upsert(merged, DB_PATH)
    storage.write_csv_snapshot(merged)
    storage.log_run(
        rows_fetched,
        result["inserted"],
        result["updated"],
        "ok",
        None,
        DB_PATH,
    )
    logger.info(
        "Run complete: fetched=%d inserted=%d updated=%d",
        rows_fetched,
        result["inserted"],
        result["updated"],
    )

    if not skip_aggregation:
        agg_result = aggregator.run_aggregation(DB_PATH)
        logger.info(
            "Aggregation done: trader_rows=%d issuer_rows=%d",
            agg_result["trader_rows"],
            agg_result["issuer_rows"],
        )


def main() -> None:
    configure_logging()

    ap = argparse.ArgumentParser(description="AFM net short positions scraper")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--current", action="store_true", help="Fetch current register only")
    group.add_argument("--archive", action="store_true", help="Fetch archive register only")
    group.add_argument("--init-only", action="store_true", help="Initialise DB and exit")
    ap.add_argument("--skip-aggregation", action="store_true", help="Skip aggregation step after scrape")
    args = ap.parse_args()

    if args.init_only:
        storage.init_db(DB_PATH)
        logger.info("DB initialised — exiting")
        return

    run(
        fetch_current=not args.archive,
        fetch_archive=not args.current,
        skip_aggregation=args.skip_aggregation,
    )


if __name__ == "__main__":
    main()
