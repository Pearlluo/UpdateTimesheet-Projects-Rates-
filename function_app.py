"""
function_app.py
===============
Azure Functions v2 entry point — Timer Trigger
Triggers every Tuesday at 02:00 Perth time (AWST)
which is Monday 18:00 UTC.

Cron schedule: "0 0 18 * * 1"
  second minute hour day month weekday(1=Monday)
  UTC Monday 18:00 = Perth Tuesday 02:00 AWST
"""

import logging
import azure.functions as func
import main as pipeline

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 18 * * 1",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def timesheet_pipeline(timer: func.TimerRequest) -> None:
    """Runs every Tuesday 02:00 AWST (Monday 18:00 UTC)"""
    if timer.past_due:
        logging.warning("Timer is past due — running now to catch up")

    logging.info("Timesheet pipeline triggered by Azure Functions")

    try:
        pipeline.main()
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
