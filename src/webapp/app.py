from __future__ import annotations
import logging
import threading
import os
import uuid
from typing import Dict, Any, Optional, List, Tuple

from fastapi import FastAPI, Form, HTTPException, Request, Depends
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request as StarletteRequest
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel

from src.config.settings import Settings, AppConfig
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter

logger = logging.getLogger(__name__)

app = FastAPI()

try:
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.abspath(os.path.join(current_file_dir, ".."))
    static_dir_abs = os.path.abspath(os.path.join(src_dir, "webapp", "static"))

    logger.debug(f"Attempting to mount static files from absolute path: {static_dir_abs}")

    if not os.path.isdir(static_dir_abs):
        logger.error(f"Static directory NOT FOUND at: {static_dir_abs}")
    else:
        logger.info(f"Static directory found at: {static_dir_abs}")
        if not os.path.exists(os.path.join(static_dir_abs, "style.css")):
            logger.error(f"style.css NOT FOUND in static directory: {static_dir_abs}")
        else:
            logger.info(f"style.css FOUND in static directory: {static_dir_abs}")

    app.mount("/static", StaticFiles(directory=static_dir_abs), name="static")
    logger.info(f"Static files mounted successfully from: {static_dir_abs}")

except Exception as e:
    logger.error(f"Failed to mount static files: {e}", exc_info=True)

try:
    templates = Jinja2Templates(directory="src/webapp/templates")
    logger.info("Jinja2 templates configured successfully.")
except Exception as e:
    logger.error(f"Failed to configure Jinja2 templates: {e}", exc_info=True)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

settings = Settings()
app_config = settings.app_config

logging.getLogger().setLevel(app_config.log_level.upper())


class TaskStatus(BaseModel):
    task_id: str
    status: str
    progress: str = "Initializing..."
    result_file: Optional[str] = None
    error: Optional[str] = None
    email: Optional[str] = None
    source_info: Dict[str, str] = {}
    statistics: Dict[str, Any] = {}
    detailed_results: List[Dict[str, Any]] = []


active_tasks: Dict[str, TaskStatus] = {}

RESULTS_DIR_NAME = 'results'
results_base_path = settings.app_config.writer.output_dir
results_path = os.path.join(str(settings.project_root), results_base_path)

os.makedirs(results_path, exist_ok=True)
logger.info(f"Results will be saved in: {results_path}")


def send_notification_email(email_address: str, task: TaskStatus):
    logger.info(
        f"Placeholder: Sending notification email to {email_address} for task {task.task_id} (Status: {task.status}).")
    if task.status == 'COMPLETED' and task.result_file:
        logger.info(f"  Report file: {task.result_file}")
    elif task.error:
        logger.error(f"  Task failed with error: {task.error}")
    pass


def run_parser_task(parser_class, url: str, task_id: str, proxy_server: Optional[str] = None,
                    user_email: Optional[str] = None, output_filename: str = "report.csv",
                    company_name: str = "", company_site: str = "", source: str = "") -> None:
    active_tasks[task_id] = TaskStatus(
        task_id=task_id,
        status='RUNNING',
        progress='Initializing parser...',
        email=user_email,
        source_info={'company_name': company_name, 'company_site': company_site, 'source': source}
    )
    driver = None
    writer = None

    try:
        driver = SeleniumDriver(settings=settings, proxy=proxy_server)
        driver.start()

        parser_instance = parser_class(driver=driver, settings=settings)

        parsed_output = parser_instance.parse(url=url)

        aggregated_info = parsed_output.get('aggregated_info', {})
        card_data_list = parsed_output.get('cards_data', [])

        if card_data_list:
            active_tasks[task_id].detailed_results = card_data_list
            active_tasks[task_id].statistics = aggregated_info

            writer = CSVWriter(settings=app_config)
            writer.set_file_path(os.path.join(results_path, output_filename))

            with writer:
                if isinstance(card_data_list, list):
                    for record in card_data_list:
                        writer.write(record)
                logger.info(f"Task {task_id}: Wrote {writer._wrote_count} records to CSV.")
        else:
            logger.warning(f"Task {task_id}: Parser returned no data or an empty structure.")
            active_tasks[task_id].status = 'COMPLETED'
            active_tasks[task_id].progress = 'Parsing finished, but no data found.'
            if user_email:
                send_notification_email(user_email, active_tasks[task_id])
            return

        active_tasks[task_id].status = 'COMPLETED'
        active_tasks[task_id].progress = 'Parsing finished successfully.'
        active_tasks[task_id].result_file = os.path.basename(writer._file_path)

        if user_email:
            send_notification_email(user_email, active_tasks[task_id])

    except Exception as e:
        logger.error(f"Error in parser task {task_id}: {e}", exc_info=True)
        active_tasks[task_id].status = 'FAILED'
        active_tasks[task_id].error = str(e)
        active_tasks[task_id].progress = 'An error occurred during parsing.'
        if user_email:
            send_notification_email(user_email, active_tasks[task_id])
    finally:
        if driver and driver._is_running:
            driver.stop()
            logger.info(f"Driver stopped for task {task_id}.")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    logger.info("Received GET request for root ('/').")
    try:
        error_msg = request.query_params.get('error')
        success_msg = request.query_params.get('success')

        logger.info(f"Attempting to render index.html. Error: '{error_msg}', Success: '{success_msg}'.")
        logger.debug(f"Request object: {request}")

        return templates.TemplateResponse("index.html",
                                          {"request": request, "error": error_msg, "success": success_msg})
    except Exception as e:
        logger.error(f"Error rendering index.html: {e}", exc_info=True)
        raise


@app.post("/start_parsing")
@limiter.limit("10/minute")
async def start_parsing(
        request: Request,
        company_name: str = Form(...),
        company_site: str = Form(...),
        source: str = Form(...),
        email: str = Form(...),
        output_filename: str = Form("report.csv")
):
    if not company_name or not company_site or not source or not email:
        return RedirectResponse(url="/?error=Missing+required+fields.+Please+fill+in+all+fields.", status_code=302)

    search_query_encoded = company_name.replace(" ", "+")
    target_url = ""
    parser_class = None

    if source == '2gis':
        target_url = f"https://2gis.ru/search/{search_query_encoded}?search_source=main&company_website={company_site}"
        parser_class = GisParser
    elif source == 'yandex':
        target_url = f"https://yandex.ru/maps/?text={search_query_encoded},{company_site}"
        parser_class = YandexParser
    else:
        return RedirectResponse(url="/?error=Invalid+source+specified.+Please+choose+2gis+or+yandex.", status_code=302)

    if not target_url or not parser_class:
        return RedirectResponse(url="/?error=Failed+to+determine+parser+or+URL.", status_code=302)

    task_id = str(uuid.uuid4())

    proxy_server = os.environ.get("PROXY_SERVER")
    if not proxy_server:
        proxy_server = getattr(settings.chrome, 'proxy_server', None)

    active_tasks[task_id] = TaskStatus(
        task_id=task_id,
        status='PENDING',
        progress='Task submitted, waiting to start...',
        email=email,
        source_info={'company_name': company_name, 'company_site': company_site, 'source': source}
    )
    logger.info(f"Submitted task {task_id} for {source} (URL: {target_url}) for user {email}.")

    thread = threading.Thread(
        target=run_parser_task,
        args=(parser_class, target_url, task_id, proxy_server, email, output_filename,
              company_name, company_site, source)
    )
    thread.daemon = True
    thread.start()

    return RedirectResponse(url=f"/task_status/{task_id}", status_code=302)


@app.get("/task_status/{task_id}")
async def task_status_page(request: Request, task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return templates.TemplateResponse("task_status.html", {"request": request, "task": task.dict()})


@app.get("/task_status_api/{task_id}")
async def task_status_api(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task.dict()


@app.get("/results/{filename}")
async def download_results(filename: str):
    file_path = os.path.join(results_path, filename)

    if os.path.exists(file_path):
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    else:
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")


@app.get("/generate_report/{task_id}")
async def generate_report(task_id: str):
    task = active_tasks.get(task_id)
    if not task or task.status != 'COMPLETED':
        raise HTTPException(status_code=404, detail="Task not found or not completed.")

    logger.info(f"Placeholder: Generating PDF report for task {task_id}")

    pdf_filename = f"report_{task_id}.pdf"
    pdf_path = os.path.join(results_path, pdf_filename)

    try:
        with open(pdf_path, "w") as f:
            f.write(f"Dummy PDF Report for Task: {task_id}\n")
            f.write(f"Status: {task.status}\n")
            f.write(f"Source: {task.source_info.get('source')} Company: {task.source_info.get('company_name')}\n")
            f.write(f"Parsed file: {task.result_file}\n")

            if task.statistics:
                f.write("\n--- Aggregated Statistics ---\n")
                for key, value in task.statistics.items():
                    f.write(f"{key}: {value}\n")

            if task.detailed_results:
                f.write(f"\n--- {len(task.detailed_results)} Detailed Results ---\n")
                for i, detail in enumerate(task.detailed_results[:5]):
                    f.write(
                        f"{i + 1}. Card: {detail.get('card_name', 'N/A')}, Rating: {detail.get('card_rating', 'N/A')}\n")
                    if len(task.detailed_results) > 5:
                        f.write("...\n")

        logger.info(f"Dummy PDF report created at: {pdf_path}")
    except Exception as e:
        logger.error(f"Failed to create dummy PDF report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error generating PDF report.")

    return JSONResponse({
        "message": "PDF report generation requested.",
        "report_filename": pdf_filename,
        "report_url": f"/download_report/{pdf_filename}"
    })


@app.get("/download_report/{filename}")
async def download_report(filename: str):
    report_path = os.path.join(results_path, filename)
    if os.path.exists(report_path):
        return FileResponse(
            path=report_path,
            filename=filename,
            media_type='application/pdf',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    else:
        raise HTTPException(status_code=404, detail="Report file not found.")


if __name__ == "__main__":
    try:
        import uvicorn

        uvicorn.run(
            "src.webapp.app:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level=app_config.log_level.lower()
        )
    except Exception as e:
        logger.error(f"Uvicorn server failed to start: {e}", exc_info=True)
