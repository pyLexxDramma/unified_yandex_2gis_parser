from __future__ import annotations
import json
import re
import time
import urllib.parse
from typing import Any, Dict, List, Optional

from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.base_parser import BaseParser
from src.config.settings import AppConfig


class GisParser(BaseParser):
    def __init__(self, driver: SeleniumDriver, settings: AppConfig):
        super().__init__(driver, settings)
        self._url: str = ""
        self.settings = settings

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
                    }
                    oldOpen.call(this, method, url, async, user, pass);
                }
            })();
        '''
        return xhr_script

    def _get_links(self) -> List[Dict[str, Any]]:
        def valid_link(node: Dict[str, Any]) -> bool:
            if node.get('localName') == 'a' and 'href' in node.get('attributes', {}):
                link_match = re.match(r'.*/(firm|station)/.*\?stat=(?P<data>[a-zA-Z0-9%]+)', node['attributes']['href'])
                if link_match:
                    try:
                        encoded_data = link_match.group('data')
                        decoded_data = urllib.parse.unquote(encoded_data)
                        return True
                    except Exception:
                        pass
            return False

        try:
            links = self.driver.get_elements_by_locator(('css selector', 'a'))
            valid_links = [link for link in links if valid_link(link)]
            return valid_links
        except Exception:
            return []

    def _wait_requests_finished(self) -> bool:
        try:
            self.driver.tab.set_default_timeout(10)
            result = self.driver.execute_script(
                'return typeof window.openHTTPs === "undefined" ? 0 : window.openHTTPs;')
            return result == 0
        except Exception:
            return True

    def _get_item_data_from_response(self, response_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            items = response_data.get('items')
            if not items or not isinstance(items, list) or not items[0]:
                return None

            item = items[0]
            name = item.get('name', '')
            rating = item.get('rating', '')
            reviews_count = item.get('reviews_count', 0)
            website = item.get('attributes', {}).get('website', '')
            phones = item.get('attributes', {}).get('phones', [])
            rubrics = item.get('rubrics', [])
            answered_count = item.get('metadata', {}).get('answered_count', 0)
            avg_response_time_days = item.get('metadata', {}).get('avg_response_time_days', '')

            return {
                'name': name,
                'rating': rating,
                'reviews_count': reviews_count,
                'website': website,
                'phones': phones,
                'rubrics': rubrics,
                'answered_count': answered_count,
                'avg_response_time_days': avg_response_time_days,
            }
        except Exception:
            return None

    def parse(self, url: str) -> List[Dict[str, Any]]:
        self._url = url
        self.driver.navigate(url)
        self.driver.execute_script(self._add_xhr_counter_script())

        card_links = self._get_links()
        card_data_list = []

        for link in card_links:
            try:
                href = link['attributes']['href']
                self.driver.perform_click(link)
                self._wait_requests_finished()
                response = self.driver.wait_response(r'https://catalog\.api\.2gis\..*/items/byid')
                if response:
                    response_body = self.driver.get_response_body(response)
                    try:
                        item_data = json.loads(response_body)
                        parsed_data = self._get_item_data_from_response(item_data)
                        if parsed_data:
                            card_data_list.append(parsed_data)
                    except json.JSONDecodeError:
                        pass
            except Exception:
                pass

        return card_data_list
