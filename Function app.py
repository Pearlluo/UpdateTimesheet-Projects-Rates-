"""
function_app.py
===============
Azure Functions v2 入口 — Timer Trigger
每周二 Perth 时间 02:00 自动触发 (UTC 周一 18:00)

Cron 表达式说明:
  "0 0 18 * * 1"
  秒 分 时 日 月 星期(1=周一)
  UTC 周一 18:00 = Perth 周二 02:00 AWST
"""

import logging
import azure.functions as func
import main as pipeline

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 0 18 * * 1",   # UTC 周一 18:00 = Perth 周二 02:00 AWST
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def timesheet_pipeline(timer: func.TimerRequest) -> None:
    """每周二 Perth 02:00 AWST 触发"""
    if timer.past_due:
        logging.warning("⚠️  Timer is past due — running now to catch up")

    logging.info("🚀 Timesheet pipeline triggered by Azure Functions")

    try:
        pipeline.main()
        logging.info("✅ Pipeline completed successfully")
    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}")
        raise