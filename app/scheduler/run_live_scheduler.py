import sys
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.scheduler.run_once import (
    BRISBANE_TZ,
    _timestamp,
    run_fast_pipeline_once,
    run_slow_refresh_once,
    run_ultra_fast_late_pipeline_once,
)
from app.notifier.telegram import send_telegram_message
from app.reports.daily_summary import generate_daily_summary_text


def send_daily_summary() -> None:
    print(f"[{_timestamp()}] Daily summary job started")
    try:
        summary_text = generate_daily_summary_text()
        sent = send_telegram_message(summary_text)
        print(f"[{_timestamp()}] Daily summary sent={sent}")
    except Exception as exc:
        print(f"[{_timestamp()}] Daily summary job failed | {exc}")


def main() -> None:
    scheduler = BlockingScheduler(timezone=BRISBANE_TZ)

    scheduler.add_job(
        run_fast_pipeline_once,
        CronTrigger(day_of_week="mon-sun", hour="9-17", minute="*/2", timezone=BRISBANE_TZ),
        id="racing_fast_pipeline_daytime",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_fast_pipeline_once,
        CronTrigger(day_of_week="mon-sun", hour="18", minute="0", timezone=BRISBANE_TZ),
        id="racing_fast_pipeline_close",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_ultra_fast_late_pipeline_once,
        CronTrigger(day_of_week="mon-sun", hour="9-17", minute="*", timezone=BRISBANE_TZ),
        id="racing_ultra_fast_late_pipeline_daytime",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_slow_refresh_once,
        CronTrigger(day_of_week="mon-sun", hour="9-17", minute="*/30", timezone=BRISBANE_TZ),
        id="racing_slow_refresh_half_hourly",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_slow_refresh_once,
        CronTrigger(day_of_week="mon-sun", hour="6", minute="15", timezone=BRISBANE_TZ),
        id="racing_slow_refresh_morning",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        send_daily_summary,
        CronTrigger(day_of_week="mon-sun", hour="18", minute="10", timezone=BRISBANE_TZ),
        id="daily_telegram_summary",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    print(
        f"[{_timestamp()}] Live scheduler started | "
        "Timezone=Australia/Brisbane | "
        "Fast pipeline=Every 2 minutes from 09:00 to 18:00, Monday-Sunday | "
        "Ultra fast late pipeline=Every minute from 09:00 to 18:00, Monday-Sunday | "
        "Slow refresh=Every 30 minutes from 09:00 to 17:30 plus 06:15 daily | "
        "Daily summary=18:10 Australia/Brisbane"
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print(f"[{_timestamp()}] Live scheduler stopped")


if __name__ == "__main__":
    main()
