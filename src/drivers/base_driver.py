from __future__ import annotations
import abc
import logging
from typing import Any, Dict, List, Optional, Tuple
from selenium.webdriver.remote.webelement import WebElement
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class DOMNode:
    def __init__(self, element_type: str, attributes: dict = {}):
        self.element_type = element_type
        self.attributes = attributes

    def add_attribute(self, key: str, value: str):
        self.attributes[key] = value

    def remove_attribute(self, key: str):
        del self.attributes[key]

    def has_attribute(self, key: str) -> bool:
        return key in self.attributes


class BaseDriver(abc.ABC):
    @abc.abstractmethod
    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Any]: pass

    @abc.abstractmethod
    def get_response_body(self, response: Any) -> Optional[str]: pass

    @abc.abstractmethod
    def set_default_timeout(self, timeout: int): pass

    @abc.abstractmethod
    def execute_script(self, script: str) -> Any: pass

    @abc.abstractmethod
    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[WebElement]: pass


class BaseParser(abc.ABC):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        self.driver = driver
        self.settings = settings
        self._driver_options = settings.parser

    @abc.abstractmethod
    def get_url_pattern(self) -> str:
        pass

    @abc.abstractmethod
    def parse(self, url: str) -> Dict[str, Any]:
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

    def _get_links_from_page(self, locator: Tuple[str, str] = ('css selector', 'a')) -> List[Dict[str, Any]]:
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
