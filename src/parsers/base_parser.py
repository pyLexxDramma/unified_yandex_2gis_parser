from __future__ import annotations
import abc
import logging
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import AppConfig
from src.drivers.base_driver import BaseDriver
from src.storage.file_writer import FileWriter

logger = logging.getLogger(__name__)


class BaseWriter(abc.ABC):
    def __init__(self, settings: AppConfig):
        self.settings = settings
        self._file = None
        self._file_path: Optional[str] = None
        self._wrote_count: int = 0

    @abc.abstractmethod
    def write(self, data: Any) -> None:
        pass

    def __enter__(self) -> 'BaseWriter':
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._file:
            self._file.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} object>"


class BaseParser(abc.ABC):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        self.driver = driver
        self.settings = settings
        self._logger = logger
        self._collected_items: List[Any] = []

    @abc.abstractmethod
    def parse(self, url: str) -> List[Dict[str, Any]]:
        pass

    @staticmethod
    @abc.abstractmethod
    def get_url_pattern() -> str:
        pass

    def get_config(self, key: str, default: Any = None) -> Any:
        return self.settings.get(key, default)

    def __enter__(self) -> 'BaseParser':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    def __repr__(self) -> str:
        classname = self.__class__.__name__
        return (f'{classname}(driver={self.driver!r}, '
                f'settings={self.settings!r})')
