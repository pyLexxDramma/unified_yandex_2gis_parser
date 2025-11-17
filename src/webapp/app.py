from __future__ import annotations
import logging
import threading
import os
import uuid
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import quote, urljoin

from fastapi import FastAPI, Form, HTTPException, Request, Depends
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request as StarletteRequest
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel, Field
import urllib.parse

from src.config.settings import Settings, AppConfig
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter

logger = logging.getLogger(__name__)

app = FastAPI()

current_file_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_file_dir, ".."))
static_dir_abs = os.path.abspath(os.path.join(src_dir, "webapp", "static"))

app.mount("/static", StaticFiles(directory=static_dir_abs), name="static")

templates = Jinja2Templates(directory="src/webapp/templates")

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


def send_notification_email(email_address: str, task: TaskStatus):
    pass


class ParsingForm(BaseModel):
    company_name: str
    company_site: str
    source: str
    email: str
    output_filename: str = "report.csv"
    search_scope: str = Field("country", description="Scope of search: 'country' or 'city'")
    location: str = Field("", description="City or country name for location filtering")

    @classmethod
    async def as_form(cls, request: Request):
        form_data = await request.form()
        return cls(**form_data)


def run_parser_task(parser_class, url: str, task_id: str, proxy_server: Optional[str] = None,
                    user_email: Optional[str] = None, output_filename: str = "report.csv",
                    company_name: str = "", company_site: str = "", source: str = "",
                    search_scope: str = "", location: str = ""):
    active_tasks[task_id] = TaskStatus(
        task_id=task_id,
        status='RUNNING',
        progress='Initializing parser...',
        email=user_email,
        source_info={'company_name': company_name, 'company_site': company_site, 'source': source,
                     'search_scope': search_scope, 'location': location}
    )
    driver = None
    writer = None

    try:
        driver = SeleniumDriver(settings=settings, proxy=proxy_server)
        driver.start()

        parser_instance = parser_class(driver=driver, settings=app_config)

        parsed_output = parser_instance.parse(url=url)

        aggregated_info = parsed_output.get('aggregated_info', {})
        card_data_list = parsed_output.get('cards_data', [])

        if card_data_list:
            active_tasks[task_id].detailed_results = card_data_list
            active_tasks[task_id].statistics = aggregated_info

            writer = CSVWriter(settings=app_config)
            writer.set_file_path(os.path.join(results_path, output_filename))

            with writer:
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
    error_msg = request.query_params.get('error')
    success_msg = request.query_params.get('success')
    return templates.TemplateResponse("index.html", {"request": request, "error": error_msg, "success": success_msg})


@app.post("/start_parsing")
@limiter.limit("10/minute")
async def start_parsing(
        request: Request,
        form_data: ParsingForm = Depends(ParsingForm.as_form)
):
    company_name = form_data.company_name
    company_site = form_data.company_site
    source = form_data.source
    email = form_data.email
    output_filename = form_data.output_filename
    search_scope = form_data.search_scope
    location = form_data.location

    if not company_name or not company_site or not source or not email:
        return RedirectResponse(url="/?error=Missing+required+fields.+Please+fill+in+all+fields.", status_code=302)

    target_url = ""
    parser_class = None

    if source == '2gis':
        encoded_company_name = urllib.parse.quote(company_name)
        if search_scope == "city" and location:
            encoded_location = urllib.parse.quote(location)
            target_url = f"https://2gis.ru/{encoded_location}/search/{encoded_company_name}?search_source=main&company_website={company_site}"
        else:
            target_url = f"https://2gis.ru/search/{encoded_company_name}?search_source=main&company_website={company_site}"
        parser_class = GisParser

    elif source == 'yandex':
        encoded_company_name = urllib.parse.quote(company_name)
        if search_scope == "city" and location:
            encoded_location = urllib.parse.quote(location)
            target_url = f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}"
        else:
            search_text = location if location else "Россия"
            full_search_text = f"{search_text}%20{encoded_company_name}"
            target_url = f"https://yandex.ru/maps/?text={full_search_text}&mode=search&z=3"
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
        source_info={'company_name': company_name, 'company_site': company_site, 'source': source,
                     'search_scope': search_scope, 'location': location}
    )

    thread = threading.Thread(
        target=run_parser_task,
        args=(parser_class, target_url, task_id, proxy_server, email, output_filename,
              company_name, company_site, source, search_scope, location)
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
        return FileResponse(path=file_path, filename=filename, media_type='text/csv',
                            headers={"Content-Disposition": f"attachment; filename={filename}"})
    else:
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")


@app.get("/generate_report/{task_id}")
async def generate_report(task_id: str):
    task = active_tasks.get(task_id)
    if not task or task.status != 'COMPLETED':
        raise HTTPException(status_code=404, detail="Task not found or not completed.")

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

    except Exception as e:
        logger.error(f"Failed to create dummy PDF report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error generating PDF report.")

    return JSONResponse({"message": "PDF report generation requested.", "report_filename": pdf_filename,
                         "report_url": f"/download_report/{pdf_filename}"})


@app.get("/download_report/{filename}")
async def download_report(filename: str):
    report_path = os.path.join(results_path, filename)
    if os.path.exists(report_path):
        return FileResponse(path=report_path, filename=filename, media_type='application/pdf',
                            headers={"Content-Disposition": f"attachment; filename={filename}"})
    else:
        raise HTTPException(status_code=404, detail="Report file not found.")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.webapp.app:app", host="0.0.0.0", port=8000, reload=True, log_level=app_config.log_level.lower())
