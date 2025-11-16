from __future__ import annotations
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from selenium.webdriver import Chrome, ChromeOptions as SeleniumChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.remote.webelement import WebElement

from src.drivers.base_driver import BaseDriver
from src.config.settings import Settings

logger = logging.getLogger(__name__)

from urllib.parse import urlparse


def extract_credentials_from_proxy_url(proxy_url: str) -> tuple:
    parsed_url = urlparse(proxy_url)
    credentials = parsed_url.netloc.split('@')[0]
    username, password = credentials.split(':')
    return username, password


class SeleniumTab:
    def __init__(self, driver: "SeleniumDriver"):
        self._driver = driver
        self._default_timeout = 10

    def set_default_timeout(self, timeout: int):
        self._default_timeout = timeout

    def wait_for_element(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> Optional[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            if not self._driver or not self._driver.driver:
                logger.error("WebDriver not initialized for wait_for_element.")
                return None
            return WebDriverWait(self._driver.driver, wait_timeout).until(
                EC.presence_of_element_located(locator)
            )
        except TimeoutException:
            logger.warning(f"Timeout waiting for element {locator}.")
            return None
        except WebDriverException as e:
            logger.error(f"WebDriverException in wait_for_element with {locator}: {e}", exc_info=True)
            return None

    def wait_for_elements(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> List[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            if not self._driver or not self._driver.driver:
                logger.error("WebDriver not initialized for wait_for_elements.")
                return []
            WebDriverWait(self._driver.driver, wait_timeout).until(
                EC.presence_of_all_elements_located(locator)
            )
            return self._driver.driver.find_elements(*locator)
        except TimeoutException:
            logger.warning(f"Timeout waiting for elements {locator}.")
            return []
        except WebDriverException as e:
            logger.error(f"WebDriverException in wait_for_elements with {locator}: {e}", exc_info=True)
            return []


class SeleniumDriver(BaseDriver):
    def __init__(self, settings: Settings, proxy: Optional[str] = None):
        self.settings = settings
        self.proxy = proxy
        self.driver: Optional[Chrome] = None
        self._tab: Optional[SeleniumTab] = None
        self._is_running = False
        self.current_url: Optional[str] = None

        self._tab = SeleniumTab(self)

    def _initialize_driver(self):
        options = SeleniumChromeOptions()
        if self.settings.chrome.headless:
            options.add_argument("--headless")
        if self.settings.chrome.silent_browser:
            options.add_argument("--disable-gpu")
            options.add_argument("--log-level=3")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])

        if self.settings.chrome.chromedriver_path:
            os.environ["webdriver.chrome.driver"] = self.settings.chrome.chromedriver_path
        else:
            pass

        if self.proxy:
            options.add_argument(f"--proxy-server={self.proxy}")

        try:
            service = Service(ChromeDriverManager().install())
            self.driver = Chrome(service=service, options=options)
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(5)
            logger.info("Selenium WebDriver instance created.")
        except WebDriverException as e:
            logger.error(f"WebDriverException during initialization: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"General error during WebDriver initialization: {e}", exc_info=True)
            raise

    def start(self) -> None:
        if not self._is_running:
            try:
                self._initialize_driver()
                self._is_running = True
                logger.info("SeleniumDriver started.")
            except Exception as e:
                logger.error(f"Error starting SeleniumDriver: {e}", exc_info=True)
                raise
        else:
            logger.warning("SeleniumDriver is already running.")

    def stop(self) -> None:
        if self._is_running and self.driver:
            try:
                self.driver.quit()
                self._is_running = False
                self.driver = None
                self.current_url = None
                logger.info("SeleniumDriver stopped.")
            except WebDriverException as e:
                logger.error(f"WebDriverException during stop: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error stopping SeleniumDriver: {e}", exc_info=True)
        elif not self._is_running:
            logger.warning("SeleniumDriver is not running.")
        else:
            logger.warning("SeleniumDriver state is inconsistent (running but driver is None).")

    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            self.driver.get(url)
            self.current_url = self.driver.current_url
            logger.info(f"Navigated to: {url}")
        except WebDriverException as e:
            logger.error(f"WebDriverException navigating to {url}: {e}", exc_info=True)
            if self.driver: self.current_url = self.driver.current_url
            raise

    def get_page_source(self) -> str:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            return self.driver.page_source
        except WebDriverException as e:
            logger.error(f"WebDriverException getting page source: {e}", exc_info=True)
            return ""

    def execute_script(self, script: str, *args) -> Any:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            return self.driver.execute_script(script, *args)
        except WebDriverException as e:
            logger.error(f"WebDriverException executing script: {e}", exc_info=True)
            return None

    def perform_click(self, element: Any) -> None:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")

        locator = None
        web_element = None

        if isinstance(element, WebElement):
            web_element = element
        elif isinstance(element, dict):
            locator_type = element.get('locator_type', 'xpath')
            locator_value = element.get('locator_value', '')
            if locator_value:
                locator = (locator_type, locator_value)
                web_element = self.tab.wait_for_element(locator)
            else:
                logger.warning(f"Locator value is empty for element dict: {element}")
        else:
            logger.warning(f"Unsupported element type for click: {type(element)}. Element: {element}")
            return

        if web_element:
            try:
                web_element.click()
                logger.info(f"Clicked element (locator: {locator if locator else 'WebElement'}).")
            except WebDriverException as e:
                logger.error(
                    f"WebDriverException clicking element (locator: {locator if locator else 'WebElement'}): {e}",
                    exc_info=True)
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", web_element)
                    web_element.click()
                    logger.info(
                        f"Clicked element after scrollIntoView (locator: {locator if locator else 'WebElement'}).")
                except WebDriverException as e_scroll:
                    logger.error(f"WebDriverException clicking element after scrollIntoView: {e_scroll}", exc_info=True)
        else:
            logger.warning(f"Could not find or click element. Locator: {locator}. Element data: {element}")

    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            WebDriverWait(self.driver, timeout).until(EC.url_contains(url_pattern))
            self.current_url = self.driver.current_url
            logger.info(f"URL contains '{url_pattern}' found. Current URL: {self.current_url}")
            return True
        except TimeoutException:
            self.current_url = self.driver.current_url
            logger.warning(f"Timeout waiting for URL containing '{url_pattern}'. Current URL: {self.current_url}")
            return False
        except WebDriverException as e:
            logger.error(f"WebDriverException waiting for URL containing '{url_pattern}': {e}", exc_info=True)
            return False

    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Any:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        return self.tab.wait_for_element(locator, timeout)

    def get_element_by_locator(self, locator: Tuple[str, str]) -> Any:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            return self.tab.wait_for_element(locator)
        except Exception as e:
            logger.error(f"Error getting element by locator {locator}: {e}", exc_info=True)
            return None

    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[Any]:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        return self.tab.wait_for_elements(locator)

    def get_responses(self, url_pattern: Optional[str] = None, timeout: int = 10) -> List[Dict[str, Any]]:
        logger.warning("get_responses is not fully implemented. Returning empty list.")
        return []

    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")

        script = f"""
        var callback = arguments[arguments.length - 1];
        var urlRegex = new RegExp("{url_pattern}");
        var originalXhrOpen = XMLHttpRequest.prototype.open;

        XMLHttpRequest.prototype.open = function(method, url, async, user, pass) {{
            if (urlRegex.test(url)) {{
                var xhr = this;
                xhr.addEventListener('load', function() {{
                    if (xhr.readyState === 4) {{
                        callback({{
                            url: url,
                            responseBody: xhr.responseText,
                            status: xhr.status
                        }});
                    }}
                }});
                 xhr.addEventListener('error', function() {{
                    callback({{
                        url: url,
                        responseBody: null,
                        status: 'error'
                    }});
                 }});
            }}
            originalXhrOpen.apply(this, arguments);
        }};
        """
        try:
            response_data = self.driver.execute_script(script)
            if response_data:
                logger.info(f"Response captured for URL pattern '{url_pattern}'.")
                return response_data
            else:
                logger.warning(
                    f"No response captured via execute_script for URL pattern '{url_pattern}' within script execution time.")
                return None
        except WebDriverException as e:
            logger.error(f"WebDriverException during wait_response for '{url_pattern}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"General error during wait_response for '{url_pattern}': {e}", exc_info=True)
            return None

    def get_response_body(self, response: Any) -> str:
        if isinstance(response, dict) and 'responseBody' in response:
            return response['responseBody']
        return ""

    def get_current_url(self) -> Optional[str]:
        if self._is_running and self.driver:
            self.current_url = self.driver.current_url
        return self.current_url

    def add_blocked_requests(self, requests: List[str]):
        logger.warning("add_blocked_requests not implemented.")
        pass

    def add_start_script(self, script: str):
        if self._is_running and self.driver:
            try:
                self.driver.execute_script(script)
                logger.info("Start script executed.")
            except WebDriverException as e:
                logger.error(f"WebDriverException executing start script: {e}", exc_info=True)
        else:
            logger.warning("WebDriver not running or not initialized for add_start_script.")
        pass

    def clear_requests(self):
        logger.warning("clear_requests not implemented.")
        pass

    def set_default_timeout(self, timeout: int):
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            self.driver.set_page_load_timeout(timeout)
            logger.info(f"Default timeout set to {timeout} seconds.")
        except WebDriverException as e:
            logger.error(f"WebDriverException setting default timeout: {e}", exc_info=True)
            raise

    @property
    def tab(self) -> SeleniumTab:
        if self._tab is None:
            self._tab = SeleniumTab(self)
        return self._tab
