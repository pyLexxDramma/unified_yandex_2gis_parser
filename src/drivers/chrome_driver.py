from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options as ChromeSeleniumOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from .base_driver import BaseDriver
from src.config.settings import settings


def parse_bool_option(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    value_lower = value.lower()
    if value_lower in ('yes', 'true', '1'):
        return True
    if value_lower in ('no', 'false', '0'):
        return False
    return None


def parse_int_option(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


class ChromeDriver(BaseDriver):
    def __init__(self):
        super().__init__()
        self.chrome_settings = self.settings.chrome
        self.driver: Optional[WebDriver] = None
        self._blocked_urls: List[str] = []
        self._start_scripts: List[str] = []
        self._request_data: List[Dict[str, Any]] = []

    def start(self) -> None:
        chrome_options = ChromeSeleniumOptions()
        if self.chrome_settings.headless:
            chrome_options.add_argument("--headless")
        if self.chrome_settings.start_maximized:
            chrome_options.add_argument("--start-maximized")
        if self.chrome_settings.disable_images:
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("disable-infobars")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-images")
        if self.chrome_settings.silent_browser:
            chrome_options.add_argument("--log-level=3")
        if self.chrome_settings.memory_limit:
            pass
        service = ChromeService(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.set_page_load_timeout(120)
        for script in self._start_scripts:
            try:
                self.driver.execute_script(script)
            except Exception as e:
                print(f"Error executing start script before page load: {e}")
        print("ChromeDriver started.")

    def stop(self) -> None:
        if self.driver:
            self.driver.quit()
            self.driver = None
            print("ChromeDriver stopped.")

    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        if not self.driver:
            raise RuntimeError("Driver not started. Call start() first.")
        try:
            if referer:
                pass
            self.driver.set_page_load_timeout(timeout)
            self.driver.get(url)
            for script in self._start_scripts:
                try:
                    self.driver.execute_script(script)
                except Exception as e:
                    print(f"Error executing start script after navigation: {e}")
        except TimeoutException:
            print(f"Timeout navigating to {url} after {timeout} seconds.")
            raise

    def get_page_source(self) -> str:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        return self.driver.page_source

    def execute_script(self, script: str, *args) -> Any:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        try:
            return self.driver.execute_script(script, *args)
        except Exception as e:
            print(f"Error executing script: {e}")
            return None

    def perform_click(self, element: WebElement) -> None:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        try:
            element.click()
        except Exception:
            try:
                actions = webdriver.ActionChains(self.driver)
                actions.move_to_element(element).click().perform()
            except Exception as e:
                print(f"Error performing click on element: {e}")
                raise

    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.url_contains(url_pattern)
            )
            return True
        except TimeoutException:
            print(f"Timeout waiting for URL pattern '{url_pattern}' after {timeout} seconds.")
            return False

    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Optional[WebElement]:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        try:
            by_strategy = getattr(By, locator[0].upper())
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by_strategy, locator[1]))
            )
            return element
        except (TimeoutException, NoSuchElementException):
            print(f"Element not found for locator {locator} within {timeout} seconds.")
            return None

    def get_element_by_locator(self, locator: Tuple[str, str]) -> Optional[WebElement]:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        try:
            by_strategy = getattr(By, locator[0].upper())
            return self.driver.find_element(by_strategy, locator[1])
        except NoSuchElementException:
            print(f"Element not found for locator {locator}.")
            return None
        except Exception as e:
            print(f"Error getting element by locator {locator}: {e}")
            return None

    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[WebElement]:
        if not self.driver:
            raise RuntimeError("Driver not started.")
        try:
            by_strategy = getattr(By, locator[0].upper())
            return self.driver.find_elements(by_strategy, locator[1])
        except Exception as e:
            print(f"Error getting elements by locator {locator}: {e}")
            return []

    def get_responses(self, url_pattern: Optional[str] = None, timeout: int = 10) -> List[Dict[str, Any]]:
        print("Warning: Selenium does not easily provide network request history. Returning empty list.")
        return []

    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
        print(f"Warning: Cannot reliably wait for response pattern '{url_pattern}' with Selenium. Returning None.")
        return None

    def get_response_body(self, response: Dict[str, Any], timeout: int = 10) -> Optional[str]:
        print("Warning: Cannot get response body with Selenium. Returning None.")
        return None

    def add_blocked_requests(self, urls: List[str]) -> None:
        self._blocked_urls.extend(urls)
        print(f"Note: Blocking requests is not directly supported by Selenium. URLs to block: {urls}")

    def add_start_script(self, script: str) -> None:
        self._start_scripts.append(script)

    def clear_requests(self) -> None:
        self._request_data.clear()
        print(
            "Note: 'clear_requests' called, but Selenium does not actively track requests in a way that can be cleared.")

    def __del__(self):
        self.stop()
