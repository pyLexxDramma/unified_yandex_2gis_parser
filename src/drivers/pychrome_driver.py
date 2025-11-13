from __future__ import annotations

import base64
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pychrome
from pydantic import BaseModel

from .base_driver import BaseDriver
from src.config.settings import settings
import logging

logger = logging.getLogger(__name__)

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

        self._tab_ready = False
        self._load_event_received = False

    def start(self) -> None:
        if self.driver is not None:
            logger.warning("PychromeDriver is already started.")
            return

        try:
            logger.info("Attempting to connect to Chrome via pychrome.Browser() on default port 9222.")
            self.driver = pychrome.Browser()

            self.tab = self.driver.new_tab()

            if self.tab:
                self.tab.Page.loadEventFired = self._handle_page_load_event
                self.tab.Network.requestWillBeSent = self._handle_request_will_be_sent
                self.tab.Network.responseReceived = self._handle_response_received
            else:
                raise RuntimeError("Failed to create a new tab.")

            self.wait_for_tab_ready(timeout=15)

            self.tab.Page.enable()
            self.tab.Network.enable()

            for script in self._start_scripts:
                try:
                    self.tab.Runtime.evaluate(expression=script)
                except Exception as e:
                    logger.error(f"Error executing start script before page load: {e}")

            if self._blocked_urls:
                self.set_blocked_urls(self._blocked_urls)

            logger.info("PychromeDriver connected successfully.")

        except TypeError as e:
            logger.error(f"TypeError starting PychromeDriver: {e}.")
            self.stop()
            raise
        except Exception as e:
            logger.error(
                f"Failed to start/connect PychromeDriver: {e}. Ensure Chrome is running with --remote-debugging-port=9222.")
            self.stop()
            raise

    def stop(self) -> None:
        if self.tab:
            try:
                self.tab.Network.disable()
                self.tab.Page.disable()
            except Exception as e:
                logger.error(f"Error during tab cleanup: {e}")
            self.tab = None

        if self.driver:
            try:
                if hasattr(self.driver, 'kill'):
                    self.driver.kill()
                    logger.info("PychromeDriver killed.")
                elif hasattr(self.driver, 'close'):
                    self.driver.close()
                    logger.info("PychromeDriver closed.")
                else:
                    logger.warning("PychromeDriver has no known close/kill method. Assuming it closes automatically.")
                self.driver = None
            except Exception as e:
                logger.error(f"Error closing PychromeDriver: {e}")
        else:
            logger.info("PychromeDriver was not running or already stopped.")

    def _handle_page_load_event(self, message: Dict[str, Any]) -> None:
        logger.debug("Page load event received.")
        self._load_event_received = True

    def wait_for_tab_ready(self, timeout: int = 15):
        if not self.tab:
            raise RuntimeError("Tab is not available.")

        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._load_event_received:
                logger.info("Tab is ready (Page.loadEventFired received).")
                return

            time.sleep(0.5)

        raise RuntimeError(
            f"Timeout waiting for tab to be ready (Page.loadEventFired not received) after {timeout} seconds.")

    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        if not self.tab:
            raise RuntimeError("Driver not started. Call start() first.")
        try:
            logger.info(f"Navigating to {url}")
            self._load_event_received = False
            self.tab.Page.navigate(url=url)
            self.wait_for_load_event(timeout=timeout)

            for script in self._start_scripts:
                try:
                    self.tab.Runtime.evaluate(expression=script)
                except Exception as e:
                    logger.error(f"Error executing start script after navigation: {e}")
        except Exception as e:
            logger.error(f"Error navigating to {url}: {e}")
            raise

    def wait_for_load_event(self, timeout: int = 60):
        if not self.tab:
            raise RuntimeError("Driver not started.")

        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._load_event_received:
                logger.info("Page load event confirmed.")
                return
            time.sleep(0.5)

        raise RuntimeError(f"Timeout waiting for page load event after {timeout} seconds.")

    def get_page_source(self) -> str:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            result = self.tab.Runtime.evaluate(expression='document.documentElement.outerHTML')
            if result and 'result' in result and 'value' in result['result']:
                return result['result']['value']
            else:
                logger.warning(f"Could not get page source. Evaluate result: {result}")
                return ""
        except Exception as e:
            logger.error(f"Error getting page source: {e}")
            return ""

    def execute_script(self, script: str, *args) -> Any:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            result = self.tab.Runtime.evaluate(expression=script, arguments=list(args))
            if result and 'result' in result and 'value' in result['result']:
                return result['result']['value']
            else:
                logger.warning(f"Could not execute script. Evaluate result: {result}")
                return None
        except Exception as e:
            logger.error(f"Error executing script: {e}")
            return None

    def perform_click(self, element: DOMNode) -> None:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            if isinstance(element, dict) and 'nodeId' in element:
                node_id = element['nodeId']
                self.tab.DOM.performClick(nodeId=node_id)
                logger.info(f"Clicked on element with nodeId: {node_id}")
            else:
                logger.warning(
                    f"Cannot perform click on unsupported element type or structure: {type(element)}. Expected DOMNode with 'nodeId'.")
        except Exception as e:
            logger.error(f"Error performing click on element: {e}")
            raise

    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        if not self.tab:
            raise RuntimeError("Driver not started.")
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                current_url_result = self.tab.Runtime.evaluate(expression='window.location.href')
                current_url = current_url_result.get('result', {}).get('value') if current_url_result else None

                if current_url and re.search(url_pattern, current_url):
                    logger.info(f"URL pattern '{url_pattern}' matched: {current_url}")
                    return True
                time.sleep(0.5)
            logger.warning(f"Timeout waiting for URL pattern '{url_pattern}' after {timeout} seconds.")
            return False
        except Exception as e:
            logger.error(f"Error waiting for URL: {e}")
            return False

    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Optional[DOMNode]:
        if not self.tab:
            raise RuntimeError("Driver not started.")

        by_strategy, value = locator
        js_selector = ""

        if by_strategy.lower() == 'xpath':
            js_selector = f"""
                var result = document.evaluate('{value}', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                return result.singleNodeValue;
            """
        elif by_strategy.lower() == 'css selector':
            js_selector = f"document.querySelector('{value}')"
        elif by_strategy.lower() == 'id':
            js_selector = f"document.getElementById('{value}')"
        elif by_strategy.lower() == 'name':
            js_selector = f"document.querySelector('[name=\"{value}\"]')"
        else:
            logger.warning(f"Unsupported locator strategy for pychrome: {by_strategy}")
            return None

        if not js_selector:
            return None

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                element_obj_result = self.tab.Runtime.evaluate(expression=js_selector)
                if element_obj_result and 'result' in element_obj_result and element_obj_result['result'] and 'value' in \
                        element_obj_result['result']:
                    node_value = element_obj_result['result']['value']
                    if node_value:
                        return node_value
                elif element_obj_result and 'error' in element_obj_result:
                    logger.error(f"Error evaluating selector '{js_selector}': {element_obj_result['error']}")
            except Exception as e:
                logger.error(f"Exception during evaluate for selector '{js_selector}': {e}")
            time.sleep(0.5)

        logger.warning(f"Element not found for locator {locator} within {timeout} seconds.")
        return None

    def get_element_by_locator(self, locator: Tuple[str, str]) -> Optional[DOMNode]:
        if not self.tab:
            raise RuntimeError("Driver not started.")

        by_strategy, value = locator
        js_selector = ""

        if by_strategy.lower() == 'xpath':
            js_selector = f"""
                var result = document.evaluate('{value}', document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                return result.singleNodeValue;
            """
        elif by_strategy.lower() == 'css selector':
            js_selector = f"document.querySelector('{value}')"
        elif by_strategy.lower() == 'id':
            js_selector = f"document.getElementById('{value}')"
        elif by_strategy.lower() == 'name':
            js_selector = f"document.querySelector('[name=\"{value}\"]')"
        else:
            logger.warning(f"Unsupported locator strategy for pychrome: {by_strategy}")
            return None

        if not js_selector:
            return None

        try:
            element_obj_result = self.tab.Runtime.evaluate(expression=js_selector)
            if element_obj_result and 'result' in element_obj_result and element_obj_result['result'] and 'value' in \
                    element_obj_result['result']:
                return element_obj_result['result']['value']
            else:
                if element_obj_result and 'error' in element_obj_result:
                    logger.error(f"Error evaluating selector '{js_selector}': {element_obj_result['error']}")
                return None
        except Exception as e:
            logger.error(f"Error getting element by locator {locator}: {e}")
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
                    var node = result.snapshotItem(i);
                    nodes.push(node); 
                }}
                return nodes;
            """
        elif by_strategy.lower() == 'css selector':
            js_selector = f"document.querySelectorAll('{value}')"
        elif by_strategy.lower() == 'name':
            js_selector = f"document.querySelectorAll('[name=\"{value}\"]')"
        else:
            logger.warning(f"Unsupported locator strategy for pychrome: {by_strategy}")
            return []

        if not js_selector:
            return []

        try:
            elements_result = self.tab.Runtime.evaluate(expression=js_selector)

            if elements_result and 'result' in elements_result and elements_result['result'] and 'value' in \
                    elements_result['result']:
                elements = elements_result['result']['value']
                if elements is None:
                    return []

                if isinstance(elements, list):
                    return [item for item in elements if isinstance(item, dict)]
                else:
                    logger.warning(
                        f"Unexpected return type from evaluate for get_elements_by_locator: {type(elements)}. Expected list.")
                    return []
            else:
                if elements_result and 'error' in elements_result:
                    logger.error(f"Error evaluating selector '{js_selector}': {elements_result['error']}")
                return []

        except Exception as e:
            logger.error(f"Error getting elements by locator {locator}: {e}")
            return []

    def _handle_request_will_be_sent(self, message: Dict[str, Any]) -> None:
        pass

    def _handle_response_received(self, message: Dict[str, Any]) -> None:
        params = message.get('params')
        if not params:
            return

        response_data = params.get('response', {})
        request_data = params.get('request', {})
        request_url = request_data.get('url')
        request_id = params.get('requestId')

        response_data['requestId'] = request_id
        response_data['request'] = request_data

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
            current_responses = list(self._network_responses)
            for response in current_responses:
                if re.search(url_pattern, response.get('url', '')):
                    logger.info(f"Response matching pattern '{url_pattern}' found: {response.get('url')}")
                    return response
            time.sleep(0.5)

        logger.warning(f"Timeout waiting for response pattern '{url_pattern}' after {timeout} seconds.")
        return None

    def get_response_body(self, response: Dict[str, Any], timeout: int = 10) -> Optional[str]:
        if not self.tab:
            raise RuntimeError("Driver not started.")

        request_id = response.get('requestId')
        if not request_id:
            logger.error("Response object does not contain 'requestId'. Cannot get body.")
            return None

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                body_data = self.tab.Network.getResponseBody(requestId=request_id)

                if body_data and 'body' in body_data:
                    body = body_data.get('body')
                    if body_data.get('base64Encoded'):
                        return base64.b64decode(body).decode('utf-8', errors='ignore')
                    else:
                        return body
                else:
                    logger.warning(f"Could not get response body for requestId {request_id}. Data: {body_data}")
                    return None
            except Exception as e:
                logger.error(f"Error getting response body for requestId {request_id}: {e}")
                return None

        logger.warning(f"Timeout getting response body for requestId {request_id} after {timeout} seconds.")
        return None

    def set_blocked_urls(self, urls: List[str]) -> None:
        self._blocked_urls = urls
        if self.tab:
            try:
                self.tab.Network.setBlockedURLs(urls=urls)
                logger.info(f"Blocked URLs set: {urls}")
            except Exception as e:
                logger.error(f"Error setting blocked URLs: {e}")
        else:
            logger.warning(f"Cannot set blocked URLs, tab is not available. URLs: {urls}")

    def add_blocked_requests(self, urls: List[str]) -> None:
        self._blocked_urls.extend(urls)
        self._blocked_urls = list(set(self._blocked_urls))
        self.set_blocked_urls(self._blocked_urls)

    def add_start_script(self, script: str) -> None:
        self._start_scripts.append(script)
        if self.tab:
            try:
                self.tab.Runtime.evaluate(expression=script)
                logger.info(f"Executed start script immediately: {script[:50]}...")
            except Exception as e:
                logger.error(f"Error executing start script immediately: {e}")

    def clear_requests(self) -> None:
        self._network_responses.clear()
        logger.info("Network responses cleared.")

    def __del__(self):
        try:
            self.stop()
        except Exception as e:
            logger.error(f"Error during PychromeDriver.__del__: {e}")