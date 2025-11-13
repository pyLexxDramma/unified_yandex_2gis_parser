from __future__ import annotations

import abc
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import settings
import logging

logger = logging.getLogger(__name__)


class BaseDriver(abc.ABC):
    def __init__(self):
        self.settings = settings
        self.driver = None
        self.tab = None

    @abc.abstractmethod
    def start(self) -> None:
        pass

    @abc.abstractmethod
    def stop(self) -> None:
        pass

    @abc.abstractmethod
    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        pass

    @abc.abstractmethod
    def get_page_source(self) -> str:
        pass

    @abc.abstractmethod
    def execute_script(self, script: str, *args) -> Any:
        pass

    @abc.abstractmethod
    def perform_click(self, element: Any) -> None:
        pass

    @abc.abstractmethod
    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        pass

    @abc.abstractmethod
    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Any:
        pass

    @abc.abstractmethod
    def get_element_by_locator(self, locator: Tuple[str, str]) -> Any:
        pass

    @abc.abstractmethod
    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[Any]:
        pass

    @abc.abstractmethod
    def get_responses(self, url_pattern: Optional[str] = None, timeout: int = 10) -> List[Dict[str, Any]]:
        pass

    @abc.abstractmethod
    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
        pass

    @abc.abstractmethod
    def get_response_body(self, response: Dict[str, Any], timeout: int = 10) -> Optional[str]:
        pass

    @abc.abstractmethod
    def add_blocked_requests(self, urls: List[str]) -> None:
        pass

    @abc.abstractmethod
    def add_start_script(self, script: str) -> None:
        pass

    @abc.abstractmethod
    def clear_requests(self) -> None:
        pass

    def __enter__(self) -> BaseDriver:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def __del__(self):
        try:
            self.stop()
        except Exception as e:
            logger.error(f"Error during BaseDriver.__del__: {e}")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} object>"
