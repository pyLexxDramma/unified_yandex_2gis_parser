from __future__ import annotations
import abc
import logging
from typing import Any, Dict, List, Optional, Tuple, Callable

from selenium.webdriver.remote.webelement import WebElement
from src.config.settings import AppConfig, Settings
from src.drivers.base_driver import BaseDriver

logger = logging.getLogger(__name__)


class BaseParser(abc.ABC):
    def __init__(self, driver: BaseDriver, settings: Settings):
        if not isinstance(driver, BaseDriver):
            raise TypeError("BaseParser requires a BaseDriver instance.")

        if not isinstance(settings, Settings):
            raise TypeError("BaseParser requires a Settings object (which has .parser attribute).")

        self._driver = driver
        self._settings = settings

        try:
            self._max_records = settings.parser.max_records
            self._captcha_wait_time = settings.parser.yandex_captcha_wait
            self._reviews_scroll_step = settings.parser.yandex_reviews_scroll_step
            self._reviews_scroll_iterations_max = settings.parser.yandex_reviews_scroll_max_iter
            self._reviews_scroll_iterations_min = settings.parser.yandex_reviews_scroll_min_iter
        except AttributeError as e:
            logger.error(
                f"Error accessing parser settings: {e}. Ensure 'settings' object has a 'parser' attribute with required options.")
            self._max_records = 100
            self._captcha_wait_time = 20
            self._reviews_scroll_step = 500
            self._reviews_scroll_iterations_max = 100
            self._reviews_scroll_iterations_min = 30

        self._is_running = False
        self._progress_callback: Optional[Callable[[str], None]] = None

    @property
    def driver(self) -> BaseDriver:
        """Доступ к драйверу."""
        return self._driver

    @property
    def settings(self) -> Settings:
        """Доступ к настройкам."""
        return self._settings

    @abc.abstractmethod
    def get_url_pattern(self) -> str:
        pass

    @abc.abstractmethod
    def parse(self, url: str) -> List[Dict[str, Any]]:
        pass

    def _wait_for_requests_finished(self, timeout: int = 10) -> bool:
        try:
            if hasattr(self.driver, 'tab') and hasattr(self.driver.tab, 'set_default_timeout'):
                self.driver.tab.set_default_timeout(timeout)

            if hasattr(self.driver, 'execute_script'):
                script_result = self.driver.execute_script(
                    'return typeof window.openHTTPs === "undefined" ? 0 : window.openHTTPs;')
                return script_result == 0
            else:
                logger.warning("Driver does not support execute_script for request checking.")
                return True
        except Exception as e:
            logger.error(f"Error waiting for requests to finish: {e}", exc_info=True)
            return True

    def _get_links_from_page(self, locator: Tuple[str, str] = ('css selector', 'a')) -> List[WebElement]:
        try:
            return self.driver.get_elements_by_locator(locator)
        except Exception as e:
            logger.error(f"Error getting links by locator {locator}: {e}", exc_info=True)
            return []

    def _get_response_body_from_url(self, url_pattern: str, timeout: int = 10) -> Optional[str]:
        response = self.driver.wait_response(url_pattern, timeout=timeout)
        if response:
            return self.driver.get_response_body(response)
        return None

    def _get_url_with_query_params(self, base_url: str, query_params: Dict[str, str]) -> str:
        from urllib.parse import urlencode, urljoin

        encoded_params = urlencode(query_params)
        return urljoin(base_url, f"?{encoded_params}")
    
    def set_progress_callback(self, callback: Callable[[str], None]) -> None:
        """Устанавливает callback для обновления прогресса"""
        self._progress_callback = callback
    
    def _update_progress(self, message: str) -> None:
        """Вызывает callback для обновления прогресса, если он установлен"""
        if self._progress_callback:
            try:
                self._progress_callback(message)
            except Exception as e:
                logger.warning(f"Error calling progress callback: {e}")
