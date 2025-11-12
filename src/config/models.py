from __future__ import annotations

import pathlib
from typing import Optional, Dict, Any

from pydantic import BaseModel, Field, PositiveInt, NonNegativeInt, validator, root_validator
import psutil


class ChromeOptions(BaseModel):
    binary_path: Optional[pathlib.Path] = None
    start_maximized: bool = False
    headless: bool = False
    disable_images: bool = True
    silent_browser: bool = True
    memory_limit: PositiveInt = Field(default_factory=lambda: _default_memory_limit())

    @staticmethod
    def _default_memory_limit() -> int:
        memory_total = psutil.virtual_memory().total / 1024 ** 2  # MB
        return _floor_to_hundreds(round(0.75 * memory_total))


class ParserOptions(BaseModel):
    skip_404_response: bool = True
    delay_between_clicks: NonNegativeInt = 0
    max_records: PositiveInt = Field(default_factory=lambda: _default_max_records())
    use_gc: bool = False
    gc_pages_interval: PositiveInt = 10

    @staticmethod
    def _default_max_records() -> int:
        memory_mb = ChromeOptions._default_memory_limit()
        max_records = _floor_to_hundreds((550 * memory_mb / 1024 - 400))
        return max_records if max_records > 0 else 1


class CSVOptions(BaseModel):
    add_rubrics: bool = True
    add_comments: bool = True
    columns_per_entity: int = Field(3, gt=0, le=5)
    remove_empty_columns: bool = True
    remove_duplicates: bool = True
    join_char: str = '; '


class WriterOptions(BaseModel):
    encoding: str = 'utf-8-sig'
    verbose: bool = True
    csv: CSVOptions = CSVOptions()


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
    chrome: ChromeOptions = Field(default_factory=ChromeOptions)
    parser: ParserOptions = Field(default_factory=ParserOptions)
    writer: WriterOptions = Field(default_factory=WriterOptions)
    log: LogOptions = Field(default_factory=LogOptions)

    class Config:
        validate_assignment = True


def _floor_to_hundreds(x: float) -> int:
    return int(x / 100) * 100


def _default_memory_limit() -> int:
    memory_total = psutil.virtual_memory().total / 1024 ** 2  # MB
    return _floor_to_hundreds(round(0.75 * memory_total))


def _default_max_records() -> int:
    memory_mb = _default_memory_limit()
    max_records = _floor_to_hundreds((550 * memory_mb / 1024 - 400))
    return max_records if max_records > 0 else 1


class ChromeOptions(BaseModel):
    binary_path: Optional[pathlib.Path] = None
    start_maximized: bool = False
    headless: bool = False
    disable_images: bool = True
    silent_browser: bool = True
    memory_limit: PositiveInt = Field(default_factory=_default_memory_limit)  # Используем фабрику


class ParserOptions(BaseModel):
    skip_404_response: bool = True
    delay_between_clicks: NonNegativeInt = 0
    max_records: PositiveInt = Field(default_factory=_default_max_records)  # Используем фабрику
    use_gc: bool = False
    gc_pages_interval: PositiveInt = 10


class AppConfig(BaseModel):
    app_name: str = "Unified Parser"
    project_root: pathlib.Path = Field(default_factory=get_project_root)
    chrome: ChromeOptions = Field(default_factory=ChromeOptions)
    parser: ParserOptions = Field(default_factory=ParserOptions)
    writer: WriterOptions = Field(default_factory=WriterOptions)
    log: LogOptions = Field(default_factory=LogOptions)

    class Config:
        validate_assignment = True
        extra = 'ignore'
