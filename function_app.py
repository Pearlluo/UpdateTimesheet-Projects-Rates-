import logging
import importlib
import azure.functions as func

app = func.FunctionApp()

@app.schedule(
    schedule="0 0 18 * * 1",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def timesheet_pipeline(myTimer: func.TimerRequest) -> None:
    logging.info("Timesheet pipeline triggered")

    try:
        import main
        importlib.reload(main)
        main.main()
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.exception(f"Pipeline failed: {e}")
        raise
