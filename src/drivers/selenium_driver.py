from __future__ import annotations
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from selenium.webdriver import Chrome, ChromeOptions as SeleniumChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.remote.webelement import WebElement

from src.drivers.base_driver import BaseDriver
from src.config.settings import Settings

logger = logging.getLogger(__name__)


class SeleniumTab:
    def __init__(self, driver: "SeleniumDriver"):
        self._driver = driver
        self._default_timeout = 10

    def set_default_timeout(self, timeout: int):
        self._default_timeout = timeout

    def wait_for_element(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> Optional[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            return WebDriverWait(self._driver.driver, wait_timeout).until(
                EC.presence_of_element_located(locator)
            )
        except TimeoutException:
            return None

    def wait_for_elements(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> List[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            WebDriverWait(self._driver.driver, wait_timeout).until(
                EC.presence_of_all_elements_located(locator)
            )
            return self._driver.driver.find_elements(*locator)
        except TimeoutException:
            return []


class SeleniumDriver(BaseDriver):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.driver: Optional[Chrome] = None
        self.tab = SeleniumTab(self)
        self._initialize_driver()

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

        try:
            self.driver = Chrome(options=options)
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(5)
        except Exception as e:
            logger.error(f"Failed to initialize Selenium WebDriver: {e}")
            raise

    def navigate(self, url: str):
        if not self.driver:
            raise RuntimeError("WebDriver not initialized.")
        self.driver.get(url)

    def get_page_source(self) -> str:
        if not self.driver:
            raise RuntimeError("WebDriver not initialized.")
        return self.driver.page_source

    def execute_script(self, script: str) -> Any:
        if not self.driver:
            raise RuntimeError("WebDriver not initialized.")
        return self.driver.execute_script(script)

    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[Dict[str, Any]]:
        if not self.driver:
            raise RuntimeError("WebDriver not initialized.")
        elements_data = []
        try:
            elements = self.tab.wait_for_elements(locator)
            for element in elements:
                element_data = {
                    'tag_name': element.tag_name,
                    'attributes': element.get_property('attributes') if hasattr(element, 'get_property') else {},
                    'text': element.text
                }
                if not isinstance(element_data['attributes'], dict):
                    element_data['attributes'] = {}

                if element.tag_name == 'a':
                    try:
                        element_data['attributes']['href'] = element.get_attribute('href')
                    except Exception:
                        pass

                elements_data.append(element_data)
        except Exception as e:
            self._logger.error(f"Error getting elements by locator {locator}: {e}")
        return elements_data

    def perform_click(self, element: Dict[str, Any]):
        if not self.driver:
            raise RuntimeError("WebDriver not initialized.")
        try:
            locator = (element.get('locator_type', 'xpath'), element.get('locator_value', ''))
            if locator[1]:
                web_element = self.tab.wait_for_element(locator)
                if web_element:
                    web_element.click()
                    return
            self._logger.warning(f"Could not perform click using locator: {locator}. Attempting fallback click.")
            self._logger.warning(f"Fallback click logic not implemented for element: {element}")
        except Exception as e:
            self._logger.error(f"Error performing click on element: {e}")

    def wait_response(self, regex_url: str, timeout: int = 10) -> Any:
        if not self.driver:
            raise RuntimeError("WebDriver not initialized.")

        script = f"""
        var callback = arguments[arguments.length - 1];
        var urlRegex = new RegExp("{regex_url}");
        var xhrOpen = XMLHttpRequest.prototype.open;
        XMLHttpRequest.prototype.open = function(method, url) {{
            if (urlRegex.test(url)) {{
                var xhr = this;
                xhr.addEventListener('load', function() {{
                    if (xhr.readyState === 4 && xhr.status === 200) {{
                        callback({{
                            url: url,
                            responseBody: xhr.responseText,
                            status: xhr.status
                        }});
                    }}
                }});
            }}
            xhrOpen.apply(this, arguments);
        }};
        """
        try:
            response_data = self.execute_script(script)
            if response_data:
                return response_data
            else:
                time.sleep(timeout)
                return None
        except Exception as e:
            self._logger.error(f"Error during wait_response for {regex_url}: {e}")
            return None

    def get_response_body(self, response: Any) -> str:
        if isinstance(response, dict) and 'responseBody' in response:
            return response['responseBody']
        return ""

    def close(self):
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("Selenium WebDriver closed.")

    @property
    def tab(self) -> SeleniumTab:
        return self._tab
