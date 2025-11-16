from __future__ import annotations

from typing import Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, validator, root_validator
import pathlib
import os
import json
import logging
from dotenv import load_dotenv
import psutil

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
    logger.warning(f"Project root markers not found. Falling back to current working directory: {fallback_root}")
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
    writer: WriterOptions = Field(default_factory=WriterOptions)


class Settings(BaseModel):
    chrome: ChromeSettings = Field(default_factory=ChromeSettings)
    parser: ParserOptions = Field(default_factory=ParserOptions)
    app_config: AppConfig = Field(default_factory=AppConfig)
    project_root: str = "."
    log: LogOptions = Field(default_factory=LogOptions)

    @root_validator(pre=True)
    def load_from_env_and_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        env_file_path = values.get('env_file', str(get_project_root() / ".env"))
        if os.path.exists(env_file_path):
            try:
                load_dotenv(dotenv_path=env_file_path)
                logger.info(f"Loaded environment variables from: {env_file_path}")
            except Exception as e:
                logger.warning(f"Could not load .env file from {env_file_path}: {e}")

        config_file_path = values.get('config_file', str(get_project_root() / "config.json"))
        if os.path.exists(config_file_path):
            try:
                with open(config_file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    values.update(config_data)
                    logger.info(f"Loaded configuration from: {config_file_path}")
            except Exception as e:
                logger.warning(f"Could not load config.json from {config_file_path}: {e}")

        resolved_root = get_project_root()
        values['project_root'] = str(resolved_root)

        if 'app_config' in values and isinstance(values['app_config'], dict):
            values['app_config']['project_root'] = str(resolved_root)
        elif 'app_config' in values and isinstance(values['app_config'], BaseModel):
            values['app_config'].project_root = str(resolved_root)

        if 'app_config' in values and isinstance(values['app_config'], dict):
            values['app_config']['writer'] = values.get('writer', WriterOptions().dict())
        elif 'app_config' in values and isinstance(values['app_config'], BaseModel):
            values['app_config'].writer = values.get('writer', WriterOptions())

        return values


try:
    settings = Settings()

    log_level = settings.log.level.upper()
    logging.basicConfig(level=log_level, format=settings.log.cli_format, datefmt=settings.log.cli_datefmt)
    logger.info(f"Logger configured with level: {log_level}")
    logger.info(f"Settings loaded successfully. Project root: {settings.project_root}")
    logger.info(f"ChromeDriver path: {settings.chrome.chromedriver_path}")

except Exception as e:
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s | %(levelname)-8s | %(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S')
    logger.error(f"Failed to initialize settings or logger: {e}", exc_info=True)
