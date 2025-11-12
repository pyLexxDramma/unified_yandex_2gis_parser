# src/parsers/gis_parser.py

from __future__ import annotations

import json
import re
import time
import urllib.parse
import base64
from typing import Any, Dict, List, Optional, Tuple

from src.parsers.base_parser import BaseParser, BaseWriter, logger
from src.drivers.base_driver import BaseDriver
from src.config.settings import AppConfig

DOMNode = Dict[str, Any]


class GisParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        if not isinstance(driver, BaseDriver):
            raise TypeError("GisParser requires a BaseDriver instance.")

        super().__init__(driver, settings)
        self._url: str = ""

        self._item_response_pattern: str = r'https://catalog\.api\.2gis.[^/]+/.*/items/byid'
        self._skip_404_response: bool = self.settings.parser.skip_404_response
        self._delay_between_clicks: int = self.settings.parser.delay_between_clicks
        self._max_records: int = self.settings.parser.max_records
        self._use_gc: bool = self.settings.parser.use_gc
        self._gc_pages_interval: int = self.settings.parser.gc_pages_interval

    @staticmethod
    def get_url_pattern() -> str:
        return r'https?://2gis\.[^/]+/[^/]+/search/.*'

    def _add_xhr_counter_script(self) -> str:
        xhr_script = r'''
            (function() {
                var oldOpen = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function(method, url, async, user, pass) {
                    if (url.match(/^https?\:\/\/[^\/]*2gis\.[a-z]+/i)) {
                        if (window.openHTTPs === undefined) {
                            window.openHTTPs = 1;
                        } else {
                            window.openHTTPs++;
                        }
                        this.addEventListener("readystatechange", function() {
                            if (this.readyState == 4) {
                                window.openHTTPs--;
                            }
                        }, false);
                    }
                    oldOpen.call(this, method, url, async, user, pass);
                }
            })();
        '''
        return xhr_script

    @property
    def _is_gui_enabled(self) -> bool:
        return False

    def _get_links(self) -> List[DOMNode]:
        def valid_link(node: DOMNode) -> bool:
            if node.get('localName') == 'a' and 'href' in node.get('attributes', {}):
                link_match = re.match(r'.*/(firm|station)/.*\?stat=(?P<data>[a-zA-Z0-9%]+)', node['attributes']['href'])
                if link_match:
                    try:
                        encoded_data = link_match.group('data')
                        decoded_data = urllib.parse.unquote(encoded_data)
                        base64.b64decode(decoded_data)
                        return True
                    except Exception:
                        pass
            return False

        try:
            links = self.driver.get_elements_by_locator(('css selector', 'a'))
            valid_links = [link for link in links if valid_link(link)]
            return valid_links
        except Exception as e:
            self._logger.error(f"Error in _get_links: {e}")
            return []

    def _wait_requests_finished(self) -> bool:
        try:
            self.driver.tab.set_default_timeout(10)
            result = self.driver.execute_script(
                'return typeof window.openHTTPs === "undefined" ? 0 : window.openHTTPs;')
            return result == 0
        except Exception as e:
            self._logger.error(f"Error checking window.openHTTPs: {e}. Assuming requests are finished.")
            return True

    def _get_available_pages(self) -> Dict[int, DOMNode]:
        dom_tree_nodes = self.driver.get_elements_by_locator(('css selector', 'a'))
        available_pages = {}
        for link_node in dom_tree_nodes:
            href = link_node.get('attributes', {}).get('href')
            if href:
                link_match = re.match(r'.*/search/.*/page/(?P<page_number>\d+)', href)
                if link_match:
                    try:
                        page_number = int(link_match.group('page_number'))
                        available_pages[page_number] = link_node
                    except ValueError:
                        pass
        return available_pages

    def _go_page(self, n_page: int) -> Optional[int]:
        available_pages = self._get_available_pages()
        if n_page in available_pages:
            link_node = available_pages[n_page]
            try:
                self.driver.perform_click(link_node)
                time.sleep(2)
                return n_page
            except Exception as e:
                self._logger.error(f"Failed to click on page link for page {n_page}: {e}")
                return None
        else:
            self._logger.warning(f"Page {n_page} not found in available pages.")
            return None

    def parse(self, writer: BaseWriter) -> None:
        if not isinstance(self.driver, BaseDriver):
            self._logger.error("Invalid driver type provided to GisParser.")
            return

        current_page_number = 1
        url = re.sub(r'/page/\d+', '', self._url, re.I)

        walk_page_match = re.search(r'/page/(?P<page_number>\d+)', self._url, re.I)
        walk_page_number = int(walk_page_match.group('page_number')) if walk_page_match else None

        xhr_script = self._add_xhr_counter_script()
        self.driver.add_start_script(xhr_script)

        try:
            self.driver.navigate(url, referer='https://google.com', timeout=120)
        except Exception as e:
            self._logger.error(f"Failed to navigate to {url}: {e}")
            return

        document_response = None
        try:
            responses = self.driver.get_responses(url_pattern=r'catalog\.api\.2gis\..*/items/byid', timeout=10)
            if responses:
                for resp in responses:
                    if re.search(self._item_response_pattern, resp.get('url', '')):
                        document_response = resp
                        break
            if not document_response:
                responses_html = self.driver.get_responses(timeout=5)
                if responses_html:
                    document_response = responses_html[0]
        except Exception as e:
            self._logger.error(f"Could not retrieve initial server response: {e}")

        if not document_response:
            self._logger.error("Error getting server response for initial page.")
            return

        if document_response.get('status') == 404:
            self._logger.warning('Server returned "No exact matches / Not found".')
            if self._skip_404_response:
                return

        collected_records = 0
        visited_links: set[str] = set()

        while True:
            if not self._wait_requests_finished():
                self._logger.warning(
                    "Requests did not finish within the expected timeframe. Proceeding with available data.")

            current_links = self._get_links()
            current_link_addresses = set(link.get('attributes', {}).get('href') for link in current_links if
                                         link.get('attributes', {}).get('href'))

            new_links = [link for link in current_links if link.get('attributes', {}).get('href') not in visited_links]
            if new_links:
                visited_links.update(link.get('attributes', {}).get('href') for link in new_links)
            else:
                if not walk_page_number and not new_links:
                    self._logger.info("No new links found and not walking through pages. Ending parse.")
                    break

            if not walk_page_number:
                for link in new_links:
                    for attempt in range(3):
                        try:
                            self._driver.perform_click(link)
                        except Exception as e:
                            self._logger.error(f"Failed to click link {link.get('attributes', {}).get('href')}: {e}")
                            continue

                        if self._delay_between_clicks > 0:
                            time.sleep(self._delay_between_clicks / 1000)

                        resp = self.driver.wait_response(self._item_response_pattern, timeout=10)

                        if resp and resp.get('status', -1) >= 0:
                            break
                        else:
                            self._logger.warning(
                                f"Attempt {attempt + 1} failed to get response for link {link.get('attributes', {}).get('href')}. Retrying...")
                    else:
                        self._logger.error(
                            f"Failed to get response after 3 attempts for link {link.get('attributes', {}).get('href')}.")
                        continue

                    doc = None
                    if resp and resp.get('status', -1) >= 0:
                        try:
                            data_body = self.driver.get_response_body(resp, timeout=10)
                            if data_body:
                                doc = json.loads(data_body)
                            else:
                                self._logger.error('Response body is empty for link. Skipping position.')
                        except json.JSONDecodeError:
                            self._logger.error('Server returned invalid JSON document: "%s", skipping position.',
                                               data_body)
                        except Exception as e:
                            self._logger.error(f"Error processing response body: {e}")
                    else:
                        self._logger.error('Response was not received or had an error. Skipping position.')

                    if doc:
                        try:
                            writer.write(doc)
                            collected_records += 1
                        except Exception as e:
                            self._logger.error(f"Error writing record to file: {e}")
                    else:
                        self._logger.error('Failed to get document data, skipping position.')

                    if collected_records >= self._max_records:
                        self._logger.info('Reached maximum allowed records for this URL.')
                        return

            if self._use_gc and current_page_number % self._gc_pages_interval == 0:
                self._logger.debug('Running garbage collector.')
                self.driver.execute_script('"gc" in window && window.gc()')

            self.driver.clear_requests()

            next_page_number = current_page_number + 1

            if walk_page_number:
                available_pages = self._get_available_pages()
                available_pages_ahead = {k: v for k, v in available_pages.items()
                                         if k > current_page_number}

                if available_pages_ahead:
                    next_page_number = min(available_pages_ahead.keys(), key=lambda n: abs(n - walk_page_number),
                                           default=current_page_number + 1)
                else:
                    next_page_number = current_page_number + 1
            else:
                next_page_number = current_page_number + 1

            self._logger.info(f"Navigating to page {next_page_number}...")
            navigated_page = self._go_page(next_page_number)

            if navigated_page:
                current_page_number = navigated_page
            else:
                self._logger.info("Could not navigate to the next page. Ending parse.")
                break

            if walk_page_number and walk_page_number <= current_page_number:
                self._logger.info(f"Reached desired page {walk_page_number}. Exiting walk mode.")
                walk_page_number = None

            time.sleep(1)
