import sys
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.notifier.telegram import send_telegram_message
from app.reports.daily_summary import generate_daily_summary_text
from app.scheduler.run_once import run_pipeline_once

TIMEZONE = "Australia/Brisbane"


def send_daily_summary():
    """Send the daily paper betting summary to Telegram."""
    summary_text = generate_daily_summary_text()
    send_telegram_message(summary_text)


def main():
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    scheduler.add_job(
        run_pipeline_once,
        trigger="cron",
        minute="*",
        id="minute_pipeline",
        replace_existing=True,
    )
    scheduler.add_job(
        send_daily_summary,
        trigger="cron",
        hour=18,
        minute=10,
        id="daily_summary",
        replace_existing=True,
    )

    print("Scheduler started.")
    print("Minute pipeline job: every minute")
    print("Daily summary job: 18:10 Australia/Brisbane")
    scheduler.start()


if __name__ == "__main__":
    main()
