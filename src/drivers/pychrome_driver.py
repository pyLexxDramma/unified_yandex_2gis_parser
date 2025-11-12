from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pychrome
from pydantic import BaseModel

from .base_driver import BaseDriver
from src.config.settings import settings

DOMNode = Dict[str, Any]


class PychromeDriver(BaseDriver):
    def __init__(self):
        super().__init__()
        self.chrome_settings = self.settings.chrome
        self.driver: Optional[pychrome.Browser] = None
        self.tab: Optional[pychrome.Tab] = None
        self._network_responses: List[Dict[str, Any]] = []
        self._blocked_urls: List[str] = []
        self._start_scripts: List[str] = []

    def start(self) -> None:
        chrome_options = self.chrome_settings
        browser_options = {
            'mode': 'new',
            'port': 9222,
            'chrome_path': str(chrome_options.binary_path) if chrome_options.binary_path else None,
            'headless': chrome_options.headless,
            'disable_images': chrome_options.disable_images,
            'silent_browser': chrome_options.silent_browser,
            'start_maximized': chrome_options.start_maximized,
        }
        try:
            if chrome_options.binary_path and not chrome_options.binary_path.exists():
                print(
                    f"Warning: Chrome binary path not found at {chrome_options.binary_path}. Trying to find automatically.")
                browser_options['chrome_path'] = None
            self.driver = pychrome.Browser(**browser_options)
            self.tab = self.driver.new_tab()
            self.tab.start()
            self.tab.set_default_timeout(60)
            for script in self._start_scripts:
                try:
                    self.tab.evaluate(script)
                except Exception as e:
                    print(f"Error executing start script before page load: {e}")
            self.tab.Network.enable()
            self.tab.Network.requestWillBeSent = self._handle_request_will_be_sent
            self.tab.Network.responseReceived = self._handle_response_received
            if self._blocked_urls:
                self.set_blocked_urls(self._blocked_urls)
            print("PychromeDriver started.")
        except Exception as e:
            print(f"Error starting PychromeDriver: {e}")
            raise

    def stop(self) -> None:
        if self.tab:
            self.tab.stop()
            self.tab = None
        if self.driver:
            self.driver.close()
            self.driver = None
        print("PychromeDriver stopped.")

    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        if not self.tab:
            raise RuntimeError("Driver not started. Call start() first.")
        try:
            self.tab.set_default_timeout(timeout)
            self.tab.Navigation.navigate(url=url)
            for script in self._start_scripts:
                try:
                    self.tab.evaluate(script)
                except Exception as e:
                    print(f"Error executing start script after navigation: {e}")
        except Exception as e:
            print(f"Error navigating to {url}: {e}")
            raise

    def get_page_source(self) -> str:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            html_content = self.tab.evaluate('document.documentElement.outerHTML')
            return html_content if isinstance(html_content, str) else json.dumps(html_content)
        except Exception as e:
            print(f"Error getting page source: {e}")
            return ""

    def execute_script(self, script: str, *args) -> Any:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            return self.tab.evaluate(script, *args)
        except Exception as e:
            print(f"Error executing script: {e}")
            return None

    def perform_click(self, element: DOMNode) -> None:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            if isinstance(element, dict) and 'nodeId' in element:
                if 'attributes' in element and 'href' in element['attributes']:
                    selector = f'a[href="{element["attributes"]["href"]}"]'
                    self.tab.evaluate(f'document.querySelector("{selector}").click();')
                else:
                    print(f"Warning: Unexpected DOMNode structure for click: {element}")
            elif isinstance(element, WebElement):
                element.click()
            else:
                print(f"Warning: Cannot perform click on unsupported element type: {type(element)}")
        except Exception as e:
            print(f"Error performing click on element: {e}")
            raise

    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                current_url = self.tab.evaluate('window.location.href')
                if re.search(url_pattern, current_url):
                    return True
                time.sleep(0.5)
            print(f"Timeout waiting for URL pattern '{url_pattern}' after {timeout} seconds.")
            return False
        except Exception as e:
            print(f"Error waiting for URL: {e}")
            return False

    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Optional[DOMNode]:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        by_strategy, value = locator
        js_selector = ""
        if by_strategy.lower() == 'xpath':
            js_selector = f"document.evaluate('{value}', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue"
        elif by_strategy.lower() == 'css selector':
            js_selector = f"document.querySelector('{value}')"
        elif by_strategy.lower() == 'id':
            js_selector = f"document.getElementById('{value}')"
        elif by_strategy.lower() == 'name':
            js_selector = f"document.querySelector('[name=\"{value}\"]')"
        else:
            print(f"Unsupported locator strategy for pychrome: {by_strategy}")
            return None
        if not js_selector:
            return None
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                element_obj = self.tab.evaluate(js_selector)
                if element_obj:
                    return element_obj
            except Exception as e:
                pass
            time.sleep(0.5)
        print(f"Element not found for locator {locator} within {timeout} seconds.")
        return None

    def get_element_by_locator(self, locator: Tuple[str, str]) -> Optional[DOMNode]:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        by_strategy, value = locator
        js_selector = ""
        if by_strategy.lower() == 'xpath':
            js_selector = f"document.evaluate('{value}', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue"
        elif by_strategy.lower() == 'css selector':
            js_selector = f"document.querySelector('{value}')"
        elif by_strategy.lower() == 'id':
            js_selector = f"document.getElementById('{value}')"
        elif by_strategy.lower() == 'name':
            js_selector = f"document.querySelector('[name=\"{value}\"]')"
        else:
            print(f"Unsupported locator strategy for pychrome: {by_strategy}")
            return None
        if not js_selector:
            return None
        try:
            element_obj = self.tab.evaluate(js_selector)
            if element_obj:
                return element_obj
            else:
                return None
        except Exception as e:
            print(f"Error getting element by locator {locator}: {e}")
            return None

    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[DOMNode]:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        by_strategy, value = locator
        js_selector = ""
        if by_strategy.lower() == 'xpath':
            js_selector = f"""
                var result = document.evaluate('{value}', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                var nodes = [];
                for (var i = 0; i < result.snapshotLength; i++) {{
                    nodes.push(result.snapshotItem(i));
                }}
                return nodes;
            """
        elif by_strategy.lower() == 'css selector':
            js_selector = f"document.querySelectorAll('{value}')"
        elif by_strategy.lower() == 'name':
            js_selector = f"document.querySelectorAll('[name=\"{value}\"]')"
        else:
            print(f"Unsupported locator strategy for pychrome: {by_strategy}")
            return []
        if not js_selector:
            return []
        try:
            elements = self.tab.evaluate(js_selector)
            if elements is None:
                return []
            if isinstance(elements, list):
                return elements
            else:
                print(f"Unexpected return type from evaluate for get_elements_by_locator: {type(elements)}")
                return []
        except Exception as e:
            print(f"Error getting elements by locator {locator}: {e}")
            return []

    def _handle_request_will_be_sent(self, message: Dict[str, Any]) -> None:
        pass

    def _handle_response_received(self, message: Dict[str, Any]) -> None:
        response_data = message.get('params', {}).get('response', {})
        request_data = message.get('params', {}).get('request', {})
        response_data['request'] = request_data
        request_url = request_data.get('url')
        if request_url:
            for blocked_url in self._blocked_urls:
                if blocked_url in request_url:
                    return
        self._network_responses.append(response_data)

    def get_responses(self, url_pattern: Optional[str] = None, timeout: int = 10) -> List[Dict[str, Any]]:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        if url_pattern:
            filtered_responses = [
                res for res in self._network_responses
                if re.search(url_pattern, res.get('url', ''))
            ]
            return filtered_responses
        return self._network_responses

    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        start_time = time.time()
        while time.time() - start_time < timeout:
            for response in self._network_responses:
                if re.search(url_pattern, response.get('url', '')):
                    return response
            time.sleep(0.5)
        print(f"Timeout waiting for response pattern '{url_pattern}' after {timeout} seconds.")
        return None

    def get_response_body(self, response: Dict[str, Any], timeout: int = 10) -> Optional[str]:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        request_id = response.get('requestId')
        if not request_id:
            print("Error: Response object does not contain 'requestId'. Cannot get body.")
            return None
        try:
            body_data = self.tab.Network.getResponseBody(requestId=request_id)
            return body_data.get('body')
        except Exception as e:
            print(f"Error getting response body for requestId {request_id}: {e}")
            return None

    def set_blocked_urls(self, urls: List[str]) -> None:
        self._blocked_urls = urls
        if self.tab:
            try:
                self.tab.Network.setBlockedURLs(urls=urls)
            except Exception as e:
                print(f"Error setting blocked URLs: {e}")

    def add_blocked_requests(self, urls: List[str]) -> None:
        self._blocked_urls.extend(urls)
        self.set_blocked_urls(self._blocked_urls)

    def add_start_script(self, script: str) -> None:
        self._start_scripts.append(script)
        if self.tab:
            try:
                self.tab.evaluate(script)
            except Exception as e:
                print(f"Error executing start script immediately: {e}")

    def clear_requests(self) -> None:
        self._network_responses.clear()

    def __del__(self):
        self.stop()
