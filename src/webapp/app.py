from __future__ import annotations
import uuid
import logging
import threading
import os
import urllib.parse
from fastapi import FastAPI, Request, Depends, HTTPException, Form, status
from fastapi.responses import RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Tuple
import secrets
from starlette.middleware.sessions import SessionMiddleware

from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter
from src.storage.pdf_writer import PDFWriter
from src.utils.task_manager import TaskStatus, active_tasks
from src.config.settings import Settings, AppConfig
from src.notifications.sender import send_notification_email

from slowapi import Limiter
from slowapi.util import get_remote_address

app = FastAPI()

templates = Jinja2Templates(directory="src/webapp/templates")

app.mount("/static", StaticFiles(directory="src/webapp/static"), name="static")

# Добавляем middleware для сессий
app.add_middleware(SessionMiddleware, secret_key=secrets.token_urlsafe(32))

limiter = Limiter(key_func=get_remote_address)

logger = logging.getLogger(__name__)

settings = Settings()

# Пароль для защиты сайта (можно задать через переменную окружения SITE_PASSWORD)
SITE_PASSWORD = os.environ.get("SITE_PASSWORD", "admin123")  # По умолчанию для теста

# Настраиваем логирование для uvicorn, чтобы видеть логи в реальном времени
import sys
# Убеждаемся, что stdout не буферизуется
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(line_buffering=True, encoding='utf-8')
    except:
        pass


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


def check_auth(request: Request) -> bool:
    """Проверяет, авторизован ли пользователь"""
    return request.session.get("authenticated", False)


@app.get("/login")
async def login_page(request: Request):
    """Страница входа"""
    if check_auth(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login(request: Request, password: str = Form(...)):
    """Обработка входа"""
    if password == SITE_PASSWORD:
        request.session["authenticated"] = True
        return RedirectResponse(url="/", status_code=302)
    else:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный пароль"})


@app.get("/logout")
async def logout(request: Request):
    """Выход из системы"""
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


@app.get("/")
async def read_root(request: Request):
    """Главная страница с проверкой авторизации"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("index.html", {"request": request, "error": None, "success": None})


SUMMARY_FIELDS = [
    ("search_query_name", "Название запроса"),
    ("total_cards_found", "Карточек найдено"),
    ("aggregated_rating", "Средний рейтинг"),
    ("aggregated_reviews_count", "Всего отзывов"),
    ("aggregated_positive_reviews", "Положительных отзывов (4-5⭐)"),
    ("aggregated_negative_reviews", "Отрицательных отзывов (1-3⭐)"),
    ("aggregated_answered_reviews_count", "Отвечено отзывов"),
    ("aggregated_answered_reviews_percent", "Процент отзывов с ответами"),
    ("aggregated_unanswered_reviews_count", "Не отвечено отзывов"),
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
    """Запуск парсинга с проверкой авторизации"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=302)
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

    if source == 'both':
        # Запускаем парсинг обоих источников одновременно
        logger.info(f"Submitted task {task_id} for BOTH sources (Yandex + 2GIS) for user {email}.")
        logger.info(f"Proxy server configured for task {task_id}: {proxy_server if proxy_server else 'NONE'}")
        
        thread = threading.Thread(
            target=run_both_parsers_task,
            args=(task_id, proxy_server, email, output_filename,
                  company_name, company_site, search_scope, location)
        )
        thread.daemon = True
        thread.start()
    elif source == '2gis':
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
        
        logger.info(f"Submitted task {task_id} for 2GIS (URL: {target_url}) for user {email}.")
        logger.info(f"Proxy server configured for task {task_id}: {proxy_server if proxy_server else 'NONE'}")

        thread = threading.Thread(
            target=run_parser_task,
            args=(parser_class, target_url, task_id, proxy_server, email, output_filename,
                  company_name, company_site, source, search_scope, location)
        )
        thread.daemon = True
        thread.start()
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
        
        logger.info(f"Submitted task {task_id} for Yandex (URL: {target_url}) for user {email}.")
        logger.info(f"Proxy server configured for task {task_id}: {proxy_server if proxy_server else 'NONE'}")

        thread = threading.Thread(
            target=run_parser_task,
            args=(parser_class, target_url, task_id, proxy_server, email, output_filename,
                  company_name, company_site, source, search_scope, location)
        )
        thread.daemon = True
        thread.start()
    else:
        return RedirectResponse(url="/?error=Invalid+source+specified.+Please+choose+2gis,+yandex+or+both.", status_code=302)

    return RedirectResponse(url=f"/tasks/{task_id}", status_code=302)


def run_both_parsers_task(task_id: str, proxy_server: Optional[str] = None,
                          user_email: Optional[str] = None, output_filename: str = "report.csv",
                          company_name: str = "", company_site: str = "",
                          search_scope: str = "", location: str = "") -> None:
    """Запускает парсинг обоих источников (Яндекс и 2GIS) параллельно и объединяет результаты"""
    import concurrent.futures
    import sys
    
    # Настраиваем логирование для потока
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            handler.flush()
    
    active_tasks[task_id].status = 'RUNNING'
    active_tasks[task_id].progress = 'Initializing parsers for both sources...'
    sys.stdout.flush()  # Принудительный flush
    
    # Генерируем URL для обоих источников
    encoded_company_name_yandex = urllib.parse.quote(company_name)
    encoded_company_name_gis = urllib.parse.quote(company_name, safe='')
    encoded_company_site = urllib.parse.quote(company_site, safe='')
    
    yandex_url = ""
    if search_scope == "city" and location:
        encoded_location = urllib.parse.quote(location)
        if location.lower() == "москва":
            yandex_url = f"https://yandex.ru/maps/?text={encoded_company_name_yandex}%2C+{encoded_location}&ll=37.617300%2C55.755826&z=12"
        elif location.lower() == "санкт-петербург":
            yandex_url = f"https://yandex.ru/maps/?text={encoded_company_name_yandex}%2C+{encoded_location}&ll=30.315868%2C59.939095&z=11"
        else:
            yandex_url = f"https://yandex.ru/maps/?text={encoded_company_name_yandex}%2C+{encoded_location}"
    else:
        search_text = location if location else "Россия"
        full_search_text = f"{search_text}%20{encoded_company_name_yandex}"
        yandex_url = f"https://yandex.ru/maps/?text={full_search_text}&mode=search&z=3"
    
    gis_url = ""
    if search_scope == "city" and location:
        encoded_location = urllib.parse.quote(location, safe='')
        gis_url = f"https://2gis.ru/{encoded_location}/search/{encoded_company_name_gis}?search_source=main&company_website={encoded_company_site}"
    else:
        gis_url = f"https://2gis.ru/search/{encoded_company_name_gis}?search_source=main&company_website={encoded_company_site}"
    
    logger.info(f"Task {task_id}: Starting parallel parsing - Yandex: {yandex_url}, 2GIS: {gis_url}")
    
    yandex_result = None
    gis_result = None
    yandex_error = None
    gis_error = None
    
    def run_yandex_parser():
        """Запускает парсер Яндекс.Карты"""
        driver = None
        try:
            logger.info(f"Task {task_id}: Starting Yandex parser...")
            sys.stdout.flush()
            driver = SeleniumDriver(settings=settings, proxy=proxy_server)
            driver.start()
            
            task_settings = settings
            if search_scope == "country":
                threshold_value = 5000
            elif search_scope == "city":
                threshold_value = 500
            else:
                threshold_value = getattr(settings.parser, 'yandex_min_cards_threshold', 500)
            
            if hasattr(task_settings.parser, 'yandex_min_cards_threshold'):
                task_settings.parser.yandex_min_cards_threshold = threshold_value
            
            parser = YandexParser(driver=driver, settings=task_settings)
            result = parser.parse(url=yandex_url)
            logger.info(f"Task {task_id}: Yandex parser completed. Found {len(result.get('cards_data', []))} cards")
            return result, None
        except Exception as e:
            logger.error(f"Task {task_id}: Yandex parser error: {e}", exc_info=True)
            return None, str(e)
        finally:
            if driver:
                try:
                    if hasattr(driver, '_is_running') and driver._is_running:
                        driver.stop()
                    elif hasattr(driver, 'driver') and driver.driver:
                        try:
                            driver.driver.quit()
                        except:
                            pass
                except:
                    pass
    
    def run_gis_parser():
        """Запускает парсер 2GIS"""
        driver = None
        try:
            logger.info(f"Task {task_id}: Starting 2GIS parser...")
            sys.stdout.flush()
            driver = SeleniumDriver(settings=settings, proxy=proxy_server)
            driver.start()
            
            parser = GisParser(driver=driver, settings=settings)
            result = parser.parse(url=gis_url)
            logger.info(f"Task {task_id}: 2GIS parser completed. Found {len(result.get('cards_data', []))} cards")
            return result, None
        except Exception as e:
            logger.error(f"Task {task_id}: 2GIS parser error: {e}", exc_info=True)
            return None, str(e)
        finally:
            if driver:
                try:
                    if hasattr(driver, '_is_running') and driver._is_running:
                        driver.stop()
                    elif hasattr(driver, 'driver') and driver.driver:
                        try:
                            driver.driver.quit()
                        except:
                            pass
                except:
                    pass
    
    # Запускаем оба парсера параллельно
    active_tasks[task_id].progress = 'Running Yandex and 2GIS parsers in parallel...'
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        yandex_future = executor.submit(run_yandex_parser)
        gis_future = executor.submit(run_gis_parser)
        
        yandex_result, yandex_error = yandex_future.result()
        gis_result, gis_error = gis_future.result()
    
    # Объединяем результаты
    all_cards = []
    combined_aggregated = {
        'search_query_name': company_name,
        'total_cards_found': 0,
        'aggregated_rating': 0.0,
        'aggregated_reviews_count': 0,
        'aggregated_positive_reviews': 0,
        'aggregated_negative_reviews': 0,
        'aggregated_answered_reviews_count': 0,
        'aggregated_unanswered_reviews_count': 0,
        'aggregated_avg_response_time': 0.0,
        'sources': {}
    }
    
    # Обрабатываем результаты Яндекс
    if yandex_result and not yandex_error:
        yandex_cards = yandex_result.get('cards_data', [])
        yandex_agg = yandex_result.get('aggregated_info', {})
        for card in yandex_cards:
            card['source'] = 'yandex'
            all_cards.append(card)
        combined_aggregated['sources']['yandex'] = yandex_agg
        combined_aggregated['total_cards_found'] += len(yandex_cards)
        combined_aggregated['aggregated_reviews_count'] += yandex_agg.get('aggregated_reviews_count', 0)
        combined_aggregated['aggregated_positive_reviews'] += yandex_agg.get('aggregated_positive_reviews', 0)
        combined_aggregated['aggregated_negative_reviews'] += yandex_agg.get('aggregated_negative_reviews', 0)
        combined_aggregated['aggregated_answered_reviews_count'] += yandex_agg.get('aggregated_answered_reviews_count', 0)
        combined_aggregated['aggregated_unanswered_reviews_count'] += yandex_agg.get('aggregated_unanswered_reviews_count', 0)
    else:
        combined_aggregated['sources']['yandex'] = {'error': yandex_error or 'Unknown error'}
        logger.warning(f"Task {task_id}: Yandex parser failed: {yandex_error}")
    
    # Обрабатываем результаты 2GIS
    if gis_result and not gis_error:
        gis_cards = gis_result.get('cards_data', [])
        gis_agg = gis_result.get('aggregated_info', {})
        for card in gis_cards:
            card['source'] = '2gis'
            all_cards.append(card)
        combined_aggregated['sources']['2gis'] = gis_agg
        combined_aggregated['total_cards_found'] += len(gis_cards)
        combined_aggregated['aggregated_reviews_count'] += gis_agg.get('aggregated_reviews_count', 0)
        combined_aggregated['aggregated_positive_reviews'] += gis_agg.get('aggregated_positive_reviews', 0)
        combined_aggregated['aggregated_negative_reviews'] += gis_agg.get('aggregated_negative_reviews', 0)
        combined_aggregated['aggregated_answered_reviews_count'] += gis_agg.get('aggregated_answered_reviews_count', 0)
        combined_aggregated['aggregated_unanswered_reviews_count'] += gis_agg.get('aggregated_unanswered_reviews_count', 0)
    else:
        combined_aggregated['sources']['2gis'] = {'error': gis_error or 'Unknown error'}
        logger.warning(f"Task {task_id}: 2GIS parser failed: {gis_error}")
    
    # Вычисляем средний рейтинг (взвешенное среднее)
    total_rating_sum = 0.0
    total_rating_count = 0
    if yandex_result and not yandex_error:
        yandex_agg = yandex_result.get('aggregated_info', {})
        yandex_rating = yandex_agg.get('aggregated_rating', 0.0)
        yandex_cards_count = yandex_agg.get('total_cards_found', 0)
        if yandex_rating > 0 and yandex_cards_count > 0:
            total_rating_sum += yandex_rating * yandex_cards_count
            total_rating_count += yandex_cards_count
    if gis_result and not gis_error:
        gis_agg = gis_result.get('aggregated_info', {})
        gis_rating = gis_agg.get('aggregated_rating', 0.0)
        gis_cards_count = gis_agg.get('total_cards_found', 0)
        if gis_rating > 0 and gis_cards_count > 0:
            total_rating_sum += gis_rating * gis_cards_count
            total_rating_count += gis_cards_count
    
    if total_rating_count > 0:
        combined_aggregated['aggregated_rating'] = round(total_rating_sum / total_rating_count, 2)
    
    # Вычисляем процент отзывов с ответами
    total_reviews = combined_aggregated['aggregated_reviews_count']
    answered_reviews = combined_aggregated['aggregated_answered_reviews_count']
    if total_reviews > 0:
        combined_aggregated['aggregated_answered_reviews_percent'] = round((answered_reviews / total_reviews) * 100, 2)
    else:
        combined_aggregated['aggregated_answered_reviews_percent'] = 0.0
    
    # Сохраняем результаты
    active_tasks[task_id].statistics = combined_aggregated
    active_tasks[task_id].detailed_results = all_cards
    active_tasks[task_id].progress = f'Parsing completed. Found {len(all_cards)} cards total (Yandex: {len([c for c in all_cards if c.get("source") == "yandex"])}, 2GIS: {len([c for c in all_cards if c.get("source") == "2gis"])}).'
    
    # Сохраняем в CSV
    if all_cards:
        writer = CSVWriter(settings=settings)
        results_dir = settings.app_config.writer.output_dir
        os.makedirs(results_dir, exist_ok=True)
        writer.set_file_path(os.path.join(results_dir, output_filename))
        
        with writer:
            for record in all_cards:
                writer.write(record)
            logger.info(f"Task {task_id}: Wrote {writer._wrote_count} records to CSV.")
        
        active_tasks[task_id].result_file = os.path.basename(writer._file_path)
    
    active_tasks[task_id].status = 'COMPLETED'
    
    if user_email:
        send_notification_email(user_email, active_tasks[task_id])


def run_parser_task(parser_class, url: str, task_id: str, proxy_server: Optional[str] = None,
                    user_email: Optional[str] = None, output_filename: str = "report.csv",
                    company_name: str = "", company_site: str = "", source: str = "",
                    search_scope: str = "", location: str = "") -> None:
    # Настраиваем логирование для потока
    import sys
    root_logger = logging.getLogger()
    # Убеждаемся, что логи выводятся в stdout
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            handler.flush()
    
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
        sys.stdout.flush()
        driver = SeleniumDriver(settings=settings, proxy=proxy_server)
        
        logger.info(f"Task {task_id}: Starting driver...")
        sys.stdout.flush()
        driver.start()
        logger.info(f"Task {task_id}: Driver started successfully")
        sys.stdout.flush()

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
        sys.stdout.flush()
        
        def update_progress(message: str):
            if task_id in active_tasks:
                active_tasks[task_id].progress = message
                logger.info(f"Task {task_id}: {message}")
                sys.stdout.flush()
        
        try:
            if hasattr(parser_instance, 'set_progress_callback'):
                parser_instance.set_progress_callback(update_progress)
        except:
            pass
        
        parsed_output = parser_instance.parse(url=url)
        logger.info(f"Task {task_id}: Parsing completed. Got {len(parsed_output.get('cards_data', []))} cards")
        sys.stdout.flush()

        aggregated_info = parsed_output.get('aggregated_info', {}) or {}
        card_data_list = parsed_output.get('cards_data', []) or []

        logger.info(f"Task {task_id}: Parsing result - {len(card_data_list)} cards, aggregated_info keys: {list(aggregated_info.keys())}")

        # Вычисляем процент отзывов с ответами
        total_reviews = aggregated_info.get('aggregated_reviews_count', 0)
        answered_reviews = aggregated_info.get('aggregated_answered_reviews_count', 0)
        if total_reviews > 0:
            aggregated_info['aggregated_answered_reviews_percent'] = round((answered_reviews / total_reviews) * 100, 2)
        else:
            aggregated_info['aggregated_answered_reviews_percent'] = 0.0

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
    """Страница статуса задачи с проверкой авторизации"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=302)
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
async def get_task_status_json(request: Request, task_id: str):
    """API для получения статуса задачи с проверкой авторизации"""
    if not check_auth(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
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


@app.get("/tasks/{task_id}/download-pdf")
async def download_pdf(request: Request, task_id: str):
    """Генерирует и возвращает PDF отчет"""
    if not check_auth(request):
        return RedirectResponse(url="/login", status_code=302)
    
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if task.status != 'COMPLETED':
        raise HTTPException(status_code=400, detail="Task is not completed yet")
    
    try:
        # Генерируем PDF
        results_dir = settings.app_config.writer.output_dir
        os.makedirs(results_dir, exist_ok=True)
        
        pdf_filename = f"report_{task_id}.pdf"
        pdf_path = os.path.join(results_dir, pdf_filename)
        
        pdf_writer = PDFWriter(pdf_path)
        pdf_writer.generate_report(
            task_data={},
            statistics=task.statistics or {},
            cards=task.detailed_results or [],
            source_info=task.source_info or {}
        )
        
        # Возвращаем файл
        from fastapi.responses import FileResponse
        return FileResponse(
            pdf_path,
            media_type='application/pdf',
            filename=pdf_filename,
            headers={"Content-Disposition": f"attachment; filename={pdf_filename}"}
        )
    except Exception as e:
        logger.error(f"Error generating PDF for task {task_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error generating PDF: {str(e)}")
