from __future__ import annotations

import json
import logging
import os
import pathlib
from typing import Dict, Any, Optional, Literal

import psutil
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator, root_validator

logger = logging.getLogger(__name__)


def get_project_root() -> pathlib.Path:
    current_path = pathlib.Path(__file__).resolve()
    for _ in range(5):
        if (current_path / '.git').exists() or \
                (current_path / 'config.json').exists() or \
                (current_path / '.env').exists():
            return current_path
        current_path = current_path.parent

    fallback_root = pathlib.Path(os.getcwd())
    logger.warning(
        f"Project root markers not found. Falling back to current working directory: {fallback_root}")
    return fallback_root


class ChromeSettings(BaseModel):
    headless: bool = False
    chromedriver_path: str = "C:/Users/lexxd/Downloads/chromedriver_win32/chromedriver.exe"
    silent_browser: bool = True
    binary_path: Optional[pathlib.Path] = None
    start_maximized: bool = False
    disable_images: bool = True
    memory_limit: int = Field(
        default_factory=lambda: int(psutil.virtual_memory().total / 1024 ** 2 * 0.75) if psutil else 1024)
    proxy_server: Optional[str] = None


class ParserOptions(BaseModel):
    retries: int = 3
    timeout: float = 10.0
    skip_404_response: bool = True
    delay_between_clicks: int = 0
    max_records: int = Field(
        default_factory=lambda: int(psutil.virtual_memory().total / 1024 ** 2 * 0.75) // 2 if psutil else 1000)
    use_gc: bool = False
    gc_pages_interval: int = 10
    yandex_captcha_wait: int = 20
    yandex_reviews_scroll_step: int = 500
    yandex_reviews_scroll_max_iter: int = 100
    yandex_reviews_scroll_min_iter: int = 30
    yandex_card_selectors: list[str] = Field(
        default_factory=lambda: [
            "div.search-business-snippet-view",
            "div.search-snippet-view__body._type_business",
            "div[class*='search-snippet-view__body'][class*='_type_business']",
            "a[href*='/maps/org/']:not([href*='/gallery/'])"
        ]
    )
    yandex_scroll_container: str = ".scroll__container, .scroll__content, .search-list-view__list"
    yandex_scrollable_element_selector: str = ".scroll__container, .scroll__content, [class*='search-list-view'], [class*='scroll']"
    yandex_scroll_step: int = 800
    yandex_scroll_max_iter: int = 200
    yandex_scroll_wait_time: float = 2.0
    yandex_min_cards_threshold: int = 500


class CSVOptions(BaseModel):
    add_rubrics: bool = True
    add_comments: bool = True
    columns_per_entity: int = Field(3, gt=0, le=5)
    remove_empty_columns: bool = True
    remove_duplicates: bool = True
    join_char: str = '; '
    output_filename: str = 'output.csv'


class WriterOptions(BaseModel):
    encoding: str = 'utf-8-sig'
    verbose: bool = True
    format: str = "csv"
    csv: CSVOptions = Field(default_factory=CSVOptions)
    output_dir: str = "./output"


class LogOptions(BaseModel):
    gui_format: str = '%(asctime)s.%(msecs)03d | %(message)s'
    cli_format: str = '%(asctime)s.%(msecs)03d | %(levelname)-8s | %(message)s'
    gui_datefmt: str = '%H:%M:%S'
    cli_datefmt: str = '%d/%m/%Y %H:%M:%S'
    level: str = 'INFO'

    @validator('level')
    def level_validation(cls, v: str) -> str:
        v = v.upper()
        allowed_levels = ('ERROR', 'WARNING', 'WARN', 'INFO', 'DEBUG', 'FATAL', 'CRITICAL', 'NOTSET')
        if v not in allowed_levels:
            raise ValueError(f'Invalid log level: {v}. Must be one of {allowed_levels}')
        return v


class AppConfig(BaseModel):
    app_name: str = "Unified Parser"
    project_root: str = Field(default_factory=lambda: str(get_project_root()))
    config_file: str = Field(default_factory=lambda: str(get_project_root() / "config.json"))
    env_file: str = Field(default_factory=lambda: str(get_project_root() / ".env"))
    root_directory: str = "/"
    environment: str = "development"
    log_level: str = "info"
    chrome: ChromeSettings = Field(default_factory=ChromeSettings)
    writer: WriterOptions = Field(default_factory=WriterOptions)


class Settings(BaseModel):
    chrome: ChromeSettings = Field(default_factory=ChromeSettings)
    parser: ParserOptions = Field(default_factory=ParserOptions)
    log: LogOptions = Field(default_factory=LogOptions)

    app_config: AppConfig = Field(default_factory=AppConfig)

    project_root: str = "."
    config_file: Optional[str] = None
    env_file: Optional[str] = None

    @root_validator(pre=True)
    def load_from_env_and_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        env_file_path_from_values = values.get('env_file')
        if env_file_path_from_values:
            env_file_path = pathlib.Path(env_file_path_from_values)
        else:
            env_file_path = get_project_root() / ".env"

        if env_file_path.exists():
            try:
                load_dotenv(dotenv_path=env_file_path)
                logger.info(f"Loaded environment variables from: {env_file_path}")
            except Exception as e:
                logger.warning(f"Could not load .env file from {env_file_path}: {e}")
        else:
            logger.debug(f".env file not found at {env_file_path}")

        config_file_path_from_values = values.get('config_file')
        if config_file_path_from_values:
            config_file_path = pathlib.Path(config_file_path_from_values)
        else:
            config_file_path = get_project_root() / "config.json"

        config_data = {}
        if config_file_path.exists():
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    logger.info(f"Loaded configuration from: {config_file_path}")
            except Exception as e:
                logger.warning(f"Could not load config.json from {config_file_path}: {e}")
        else:
            logger.debug(f"config.json file not found at {config_file_path}")

        updated_values = {**values, **config_data}

        resolved_root = get_project_root()
        updated_values['project_root'] = str(resolved_root)
        updated_values['config_file'] = str(config_file_path)
        updated_values['env_file'] = str(env_file_path)

        if 'app_config' in updated_values:
            app_config_data = updated_values['app_config']
            if isinstance(app_config_data, dict):
                app_config_data['project_root'] = str(resolved_root)
                if 'writer' not in app_config_data:
                    app_config_data['writer'] = WriterOptions().dict()
            elif isinstance(app_config_data, BaseModel):
                app_config_data.project_root = str(resolved_root)
                if not hasattr(app_config_data, 'writer'):
                    app_config_data.writer = WriterOptions()

        return updated_values


try:
    settings = Settings()

    log_level_str = settings.log.level.upper()
    log_level_int = getattr(logging, log_level_str) if log_level_str in logging._nameToLevel else logging.INFO

    import os
    from logging.handlers import RotatingFileHandler
    
    log_dir = os.path.join(settings.project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "parser.log")
    
    log_format = settings.log.cli_format
    date_format = settings.log.cli_datefmt
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level_int)
    
    root_logger.handlers.clear()
    
    import sys
    # Создаем StreamHandler с небуферизованным выводом в stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level_int)
    console_formatter = logging.Formatter(log_format, datefmt=date_format)
    console_handler.setFormatter(console_formatter)
    # Отключаем буферизацию для немедленного вывода
    if hasattr(console_handler.stream, 'reconfigure'):
        try:
            console_handler.stream.reconfigure(line_buffering=True)
        except:
            pass
    # Принудительно сбрасываем буфер после каждого сообщения
    console_handler.flush = lambda: console_handler.stream.flush() if hasattr(console_handler.stream, 'flush') else None
    root_logger.addHandler(console_handler)
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level_int)
    file_formatter = logging.Formatter(log_format, datefmt=date_format)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Настраиваем принудительный flush для консольного вывода
    class FlushingStreamHandler(logging.StreamHandler):
        def emit(self, record):
            super().emit(record)
            if hasattr(self.stream, 'flush'):
                self.stream.flush()
    
    # Заменяем обычный handler на FlushingStreamHandler
    root_logger.removeHandler(console_handler)
    flushing_handler = FlushingStreamHandler(sys.stdout)
    flushing_handler.setLevel(log_level_int)
    flushing_handler.setFormatter(console_formatter)
    if hasattr(flushing_handler.stream, 'reconfigure'):
        try:
            flushing_handler.stream.reconfigure(line_buffering=True)
        except:
            pass
    root_logger.addHandler(flushing_handler)

    logger.setLevel(log_level_int)
    
    for logger_name in ['src.parsers', 'src.parsers.yandex_parser', 'src.parsers.gis_parser', 
                        'src.drivers', 'src.drivers.selenium_driver', 'src.webapp', 'src.webapp.app']:
        module_logger = logging.getLogger(logger_name)
        module_logger.setLevel(log_level_int)
        module_logger.propagate = True

    logger.info(f"Logger configured with level: {log_level_str}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Settings loaded successfully.")
    logger.info(f"Project root: {settings.project_root}")
    logger.info(f"ChromeDriver path: {settings.chrome.chromedriver_path}")
    logger.info(f"Output directory: {settings.app_config.writer.output_dir}")

except Exception as e:
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s | %(levelname)-8s | %(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S')
    logging.error(f"FATAL: Failed to initialize settings or logger: {e}", exc_info=True)
