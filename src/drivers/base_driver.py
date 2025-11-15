from __future__ import annotations
import abc
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DOMNode = Dict[str, Any]


class BaseDriver(abc.ABC):
    def __init__(self):
        self.driver = None
        self.tab = None
        self._is_running = False

    @abc.abstractmethod
    def start(self) -> None:
        self._is_running = True
        logger.info(f"{self.__class__.__name__} started.")

    @abc.abstractmethod
    def stop(self) -> None:
        if self._is_running:
            self._is_running = False
            logger.info(f"{self.__class__.__name__} stopped.")

    @abc.abstractmethod
    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")

    @abc.abstractmethod
    def get_page_source(self) -> str:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def execute_script(self, script: str, *args) -> Any:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def perform_click(self, element: Any) -> None:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Any:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def get_element_by_locator(self, locator: Tuple[str, str]) -> Any:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[Any]:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def get_responses(self, url_pattern: Optional[str] = None, timeout: int = 10) -> List[Dict[str, Any]]:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Dict[str, Any]]:

        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def get_response_body(self, response: Dict[str, Any], timeout: int = 10) -> Optional[str]:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def add_blocked_requests(self, urls: List[str]) -> None:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def add_start_script(self, script: str) -> None:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    @abc.abstractmethod
    def clear_requests(self) -> None:
        if not self._is_running:
            raise RuntimeError(f"{self.__class__.__name__} is not running.")
        pass

    def __enter__(self) -> BaseDriver:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def __del__(self):
        self.stop()

    def __repr__(self) -> str:
        status = "running" if self._is_running else "stopped"
        return f"<{self.__class__.__name__} {status}>"
