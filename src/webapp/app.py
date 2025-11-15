from __future__ import annotations
import logging
import threading
import os
import uuid
from typing import Dict, Any, Optional

from fastapi import FastAPI, Form, HTTPException, UploadFile, File
from fastapi.responses import RedirectResponse, FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from src.config.settings import Settings
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter

logging.basicConfig(level=logging.DEBUG)

app = FastAPI()

templates = Jinja2Templates(directory="src/webapp/templates")

app.mount("/static", StaticFiles(directory="src/webapp/static"), name="static")

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

settings = Settings()

active_tasks: Dict[str, Dict[str, Any]] = {}
RESULTS_DIR_NAME = 'results'
results_path = os.path.join(settings.project_root, RESULTS_DIR_NAME)
os.makedirs(results_path, exist_ok=True)
logging.info(f"Created results directory: {results_path}")

def run_parser_task(parser_class, url: str, task_id: str, proxy_server: Optional[str] = None) -> None:
    active_tasks[task_id] = {'status': 'running', 'progress': 'Initializing...', 'result_file': None, 'error': None}
    driver = None
    writer = None

    try:
        driver_instance = SeleniumDriver(proxy=proxy_server)
        driver_instance.start()
        writer_instance = CSVWriter(settings)
        parser_instance = parser_class(driver=driver_instance, settings=settings)
        parser_instance._url = url

        active_tasks[task_id]['progress'] = f'Starting parsing {parser_class.__name__} for {url}...'

        writer_instance.write_header()

        parser_instance.parse(writer=writer_instance)
        writer_instance.close()

        active_tasks[task_id]['status'] = 'finished'
        active_tasks[task_id]['progress'] = 'Parsing finished successfully.'
        active_tasks[task_id]['result_file'] = writer_instance._file_path

    except Exception as e:
        logging.error(f"Error in parser task {task_id}: {e}", exc_info=True)
        active_tasks[task_id]['status'] = 'error'
        active_tasks[task_id]['error'] = str(e)
        active_tasks[task_id]['progress'] = 'An error occurred during parsing.'
    finally:
        if driver:
            driver.stop()

@app.get("/")
async def index(request: Request):
    try:
        error_msg = request.query_params.get('error')
        success_msg = request.query_params.get('success')
        return templates.TemplateResponse("index.html", {"request": request, "error": error_msg, "success": success_msg})
    except Exception as e:
        logging.error(f"Error rendering template: {e}")
        raise

@app.post("/start_parsing")
async def start_parsing(company_name: str = Form(...), company_site: str = Form(...), source: str = Form(...)):
    if not company_name or not company_site or not source:
        return RedirectResponse(url_for("index", error="Missing required fields."), status_code=302)

    search_query_encoded = company_name.replace(" ", "+")

    target_url = ""
    parser_class = None

    if source == '2gis':
        target_url = f"https://2gis.ru/search/{search_query_encoded}?maybe_web={company_site}"
        parser_class = GisParser
    elif source == 'yandex':
        target_url = f"https://yandex.ru/maps/?text={search_query_encoded},{company_site}"
        parser_class = YandexParser
    else:
        return RedirectResponse(url_for("index", error="Invalid source specified."), status_code=302)

    if not target_url or not parser_class:
        return RedirectResponse(url_for("index", error="Failed to generate target URL or determine parser class."), status_code=302)

    task_id = str(uuid.uuid4())
    proxy_server = os.environ.get("PROXY_SERVER")
    thread = threading.Thread(target=run_parser_task, args=(parser_class, target_url, task_id, proxy_server))
    thread.daemon = True
    thread.start()

    return RedirectResponse(url_for('task_status', task_id=task_id), status_code=302)

@app.get("/task_status/{task_id}")
async def task_status(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    return templates.TemplateResponse("task_status.html", {"task_id": task_id, "task": task})

@app.get("/task_status_api/{task_id}")
async def task_status_api(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.get("/results/{filename}")
async def download_results(filename: str):
    file_path = os.path.join(results_path, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename, media_type='text/csv', headers={"Content-Disposition": f"attachment;filename={filename}"})
    else:
        raise HTTPException(status_code=404, detail="File not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)