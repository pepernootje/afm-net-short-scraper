import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from scraper.config import SCHEDULE_HOUR, SCHEDULE_MINUTE, SCHEDULE_TIMEZONE
from scraper.logging_setup import configure_logging
from scraper.main import run

logger = logging.getLogger(__name__)


def start() -> None:
    configure_logging()
    scheduler = BlockingScheduler()
    trigger = CronTrigger(
        hour=SCHEDULE_HOUR,
        minute=SCHEDULE_MINUTE,
        timezone=SCHEDULE_TIMEZONE,
    )
    scheduler.add_job(
        func=run,
        trigger=trigger,
        misfire_grace_time=3600,
        coalesce=True,
    )
    logger.info(
        "Scheduler started — will run daily at %02d:%02d %s",
        SCHEDULE_HOUR,
        SCHEDULE_MINUTE,
        SCHEDULE_TIMEZONE,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    start()
