from __future__ import annotations
import uuid
import logging
import threading
import os
import urllib.parse
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Tuple

from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter
from src.utils.task_manager import TaskStatus, active_tasks
from src.config.settings import Settings, AppConfig
from src.notifications.sender import send_notification_email

from slowapi import Limiter
from slowapi.util import get_remote_address

app = FastAPI()

templates = Jinja2Templates(directory="src/webapp/templates")

app.mount("/static", StaticFiles(directory="src/webapp/static"), name="static")

limiter = Limiter(key_func=get_remote_address)

logger = logging.getLogger(__name__)

settings = Settings()


class ParsingForm(BaseModel):
    company_name: str
    company_site: str
    source: str
    email: str
    output_filename: str = "report.csv"
    search_scope: str = Field("country", description="Scope of search: 'country' or 'city'")
    location: str = Field("", description="City or country name for location filtering")
    proxy_server: Optional[str] = Field("", description="Proxy server URL (optional)")

    @classmethod
    async def as_form(cls, request: Request):
        form_data = await request.form()
        try:
            return cls(**form_data)
        except Exception as e:
            logger.error(f"Error parsing form data: {e}", exc_info=True)
            raise HTTPException(status_code=422, detail=f"Error processing form data: {e}")


@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "error": None, "success": None})


SUMMARY_FIELDS = [
    ("search_query_name", "Название запроса"),
    ("total_cards_found", "Карточек найдено"),
    ("aggregated_rating", "Средний рейтинг"),
    ("aggregated_reviews_count", "Всего отзывов"),
    ("aggregated_positive_reviews", "Положительных отзывов (4-5⭐)"),
    ("aggregated_negative_reviews", "Отрицательных отзывов (1-3⭐)"),
    ("aggregated_answered_reviews_count", "Отвечено отзывов"),
    ("aggregated_unanswered_reviews_count", "Не отвечено отзывов"),
    ("aggregated_answered_count", "Карточек с ответами"),
    ("aggregated_avg_response_time", "Среднее время ответа (дни)"),
]


@app.get("/tasks")
async def get_all_tasks():
    tasks_list = []
    for task in active_tasks.values():
        task_dict = {
            "task_id": task.task_id,
            "status": task.status,
            "progress": task.progress,
            "email": task.email,
            "source_info": task.source_info,
            "result_file": task.result_file,
            "error": task.error,
            "timestamp": str(task.timestamp)
        }
        tasks_list.append(task_dict)
    return {"tasks": tasks_list}


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
        encoded_company_name = urllib.parse.quote(company_name, safe='')
        encoded_company_site = urllib.parse.quote(company_site, safe='')
        if search_scope == "city" and location:
            encoded_location = urllib.parse.quote(location, safe='')
            target_url = f"https://2gis.ru/{encoded_location}/search/{encoded_company_name}?search_source=main&company_website={encoded_company_site}"
            logger.info(f"2GIS City search URL generated: {target_url}")
        else:
            target_url = f"https://2gis.ru/search/{encoded_company_name}?search_source=main&company_website={encoded_company_site}"
            logger.warning(f"2GIS search scope is 'country' or unspecified location. Using general search URL.")
            logger.info(f"2GIS Country search URL generated: {target_url}")
        parser_class = GisParser

    elif source == 'yandex':
        encoded_company_name = urllib.parse.quote(company_name)
        if search_scope == "city" and location:
            encoded_location = urllib.parse.quote(location)
            if location.lower() == "москва":
                target_url = f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}&ll=37.617300%2C55.755826&z=12"
            elif location.lower() == "санкт-петербург":
                target_url = f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}&ll=30.315868%2C59.939095&z=11"
            else:
                target_url = f"https://yandex.ru/maps/?text={encoded_company_name}%2C+{encoded_location}"
                logger.warning(
                    f"Using generic city search for Yandex Maps for location: {location}. Coordinates may not be precise.")
            logger.info(f"Yandex Maps City search URL generated: {target_url}")
        else:
            search_text = location if location else "Россия"
            full_search_text = f"{search_text}%20{encoded_company_name}"
            target_url = f"https://yandex.ru/maps/?text={full_search_text}&mode=search&z=3"
            logger.warning(
                f"Yandex Maps search scope is 'country'. Using general country search URL. Consider specifying a city for better results.")
            logger.info(f"Yandex Maps Country search URL generated: {target_url}")
        parser_class = YandexParser

    else:
        return RedirectResponse(url="/?error=Invalid+source+specified.+Please+choose+2gis+or+yandex.", status_code=302)

    if not target_url or not parser_class:
        return RedirectResponse(url="/?error=Failed+to+determine+parser+or+URL.", status_code=302)

    task_id = str(uuid.uuid4())
    proxy_server = form_data.proxy_server.strip() if form_data.proxy_server else None
    if not proxy_server:
        proxy_server = os.environ.get("PROXY_SERVER")
    if not proxy_server:
        if hasattr(settings, 'chrome') and hasattr(settings.chrome, 'proxy_server') and settings.chrome.proxy_server:
            proxy_server = settings.chrome.proxy_server
        else:
            proxy_server = None

    active_tasks[task_id] = TaskStatus(
        task_id=task_id,
        status='PENDING',
        progress='Task submitted, waiting to start...',
        email=email,
        source_info={'company_name': company_name, 'company_site': company_site, 'source': source,
                     'search_scope': search_scope, 'location': location}
    )
    logger.info(f"Submitted task {task_id} for {source} (URL: {target_url}) for user {email}.")
    logger.info(f"Proxy server configured for task {task_id}: {proxy_server if proxy_server else 'NONE'}")

    thread = threading.Thread(
        target=run_parser_task,
        args=(parser_class, target_url, task_id, proxy_server, email, output_filename,
              company_name, company_site, source, search_scope, location)
    )
    thread.daemon = True
    thread.start()

    return RedirectResponse(url=f"/tasks/{task_id}", status_code=302)


def run_parser_task(parser_class, url: str, task_id: str, proxy_server: Optional[str] = None,
                    user_email: Optional[str] = None, output_filename: str = "report.csv",
                    company_name: str = "", company_site: str = "", source: str = "",
                    search_scope: str = "", location: str = "") -> None:
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
        logger.info(f"Task {task_id}: Creating SeleniumDriver...")
        driver = SeleniumDriver(settings=settings, proxy=proxy_server)
        
        logger.info(f"Task {task_id}: Starting driver...")
        driver.start()
        logger.info(f"Task {task_id}: Driver started successfully")

        logger.info(f"Task {task_id}: Creating parser instance ({parser_class.__name__})...")
        
        task_settings = settings
        
        if parser_class == YandexParser:
            if search_scope == "country":
                threshold_value = 5000
                logger.info(f"Task {task_id}: Search scope is 'country', setting yandex_min_cards_threshold to {threshold_value}")
            elif search_scope == "city":
                threshold_value = 500
                logger.info(f"Task {task_id}: Search scope is 'city', setting yandex_min_cards_threshold to {threshold_value}")
            else:
                threshold_value = getattr(settings.parser, 'yandex_min_cards_threshold', 500)
                logger.info(f"Task {task_id}: Search scope is '{search_scope}', using default threshold from config: {threshold_value}")
            
            if hasattr(task_settings.parser, 'yandex_min_cards_threshold'):
                original_threshold = task_settings.parser.yandex_min_cards_threshold
                task_settings.parser.yandex_min_cards_threshold = threshold_value
                logger.info(f"Task {task_id}: Updated yandex_min_cards_threshold: {original_threshold} -> {threshold_value}")
        
        parser_instance = parser_class(driver=driver, settings=task_settings)
        logger.info(f"Task {task_id}: Parser instance created successfully")
        
        active_tasks[task_id].progress = 'Parsing started...'
        logger.info(f"Task {task_id}: Starting parsing for URL: {url}")
        
        def update_progress(message: str):
            if task_id in active_tasks:
                active_tasks[task_id].progress = message
                logger.info(f"Task {task_id}: {message}")
        
        try:
            if hasattr(parser_instance, 'set_progress_callback'):
                parser_instance.set_progress_callback(update_progress)
        except:
            pass
        
        parsed_output = parser_instance.parse(url=url)
        logger.info(f"Task {task_id}: Parsing completed. Got {len(parsed_output.get('cards_data', []))} cards")

        aggregated_info = parsed_output.get('aggregated_info', {}) or {}
        card_data_list = parsed_output.get('cards_data', []) or []

        logger.info(f"Task {task_id}: Parsing result - {len(card_data_list)} cards, aggregated_info keys: {list(aggregated_info.keys())}")

        active_tasks[task_id].statistics = aggregated_info
        active_tasks[task_id].detailed_results = card_data_list
        active_tasks[task_id].progress = f'Parsing completed. Found {len(card_data_list)} cards.'

        if card_data_list:
            writer = CSVWriter(settings=settings)
            results_dir = settings.app_config.writer.output_dir
            os.makedirs(results_dir, exist_ok=True)
            writer.set_file_path(os.path.join(results_dir, output_filename))

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
        error_message = str(e)
        if len(error_message) > 500:
            error_message = error_message[:500] + "..."
        active_tasks[task_id].status = 'FAILED'
        active_tasks[task_id].error = error_message
        active_tasks[task_id].progress = f'An error occurred during parsing: {error_message[:100]}'
        logger.error(f"Task {task_id} marked as FAILED. Error: {error_message}")
        if user_email:
            try:
                send_notification_email(user_email, active_tasks[task_id])
            except Exception as email_error:
                logger.error(f"Failed to send notification email: {email_error}")
    finally:
        if driver:
            try:
                if hasattr(driver, '_is_running') and driver._is_running:
                    logger.info(f"Stopping driver for task {task_id}...")
                    driver.stop()
                    logger.info(f"Driver stopped for task {task_id}.")
                elif hasattr(driver, 'driver') and driver.driver:
                    logger.warning(f"Driver state inconsistent for task {task_id}, attempting to stop...")
                    try:
                        driver.driver.quit()
                    except:
                        pass
            except Exception as stop_error:
                logger.error(f"Error stopping driver for task {task_id}: {stop_error}", exc_info=True)


@app.get("/tasks/{task_id}")
async def task_status_page(request: Request, task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    context = {
        "request": request,
        "task": task,
        "statistics": task.statistics or {},
        "cards": task.detailed_results or [],
        "summary_fields": SUMMARY_FIELDS,
        "output_dir": settings.app_config.writer.output_dir,
    }
    return templates.TemplateResponse("task_status.html", context)


@app.get("/api/task_status/{task_id}")
async def get_task_status_json(task_id: str):
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task_dict = {
        "task_id": task.task_id,
        "status": task.status,
        "progress": task.progress,
        "email": task.email,
        "source_info": task.source_info,
        "result_file": task.result_file,
        "error": task.error,
        "timestamp": str(task.timestamp)
    }
    if task.statistics:
        task_dict["statistics"] = task.statistics
    if task.detailed_results:
        task_dict["cards"] = task.detailed_results
        task_dict["cards_count"] = len(task.detailed_results)
    return JSONResponse(task_dict)
