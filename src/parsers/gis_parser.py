from __future__ import annotations
import json
import re
import urllib.parse
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, Tag
from selenium.webdriver.remote.webelement import WebElement as SeleniumWebElement

from src.drivers.base_driver import BaseDriver
from src.config.settings import AppConfig
from src.parsers.base_parser import BaseParser


class GisParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        super().__init__(driver, settings)
        self._url: str = ""

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

    def _get_links(self) -> List[Tag]:
        def valid_link(node: Tag) -> bool:
            if node.name == 'a' and 'href' in node.attrs:
                link_match = re.match(r'.*/(firm|station)/.*\?stat=(?P<data>[a-zA-Z0-9%]+)', node['href'])
                if link_match:
                    try:
                        urllib.parse.unquote(link_match.group('data'))
                        return True
                    except Exception:
                        pass
            return False

        try:
            links = self.driver.get_elements_by_locator(('css selector', 'a'))
            valid_links = [link for link in links if isinstance(link, Tag) and valid_link(link)]
            return valid_links
        except Exception as e:
            logger.error(f"Error getting links: {e}")
            return []

    def _wait_requests_finished(self) -> bool:
        try:
            if hasattr(self.driver, 'tab') and hasattr(self.driver.tab, 'set_default_timeout'):
                self.driver.tab.set_default_timeout(10)

            result = self.driver.execute_script(
                'return typeof window.openHTTPs === "undefined" ? 0 : window.openHTTPs;')
            return result == 0
        except Exception as e:
            logger.error(f"Error waiting for requests to finish: {e}")
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
                'card_name': name,
                'card_address': '',
                'card_rating': rating,
                'card_reviews_count': reviews_count,
                'card_website': website,
                'card_phone': phones[0] if phones else '',
                'card_rubrics': "; ".join(rubrics) if rubrics else '',
                'card_response_status': 'YES' if answered_count > 0 else 'NO',
                'card_avg_response_time': avg_response_time_days,
                'card_reviews_positive': 0,
                'card_reviews_negative': 0,
                'card_reviews_texts': "",
            }
        except Exception as e:
            logger.error(f"Error processing item data from response: {e}")
            return None

    def get_url_pattern(self) -> str:
        return r"https://2gis\.ru/search/.*"

    def parse(self, url: str) -> Dict[str, Any]:
        self._url = url
        self.driver.navigate(url)
        self.driver.execute_script(self._add_xhr_counter_script())

        card_data_list = []
        aggregated_info = {
            'search_query_name': url.split('/search/')[1].split('?')[0].replace('+',
                                                                                ' ') if '/search/' in url else "2gisSearch",
            'total_cards_found': 0,
            'aggregated_rating': 0.0,
            'aggregated_reviews_count': 0,
            'aggregated_positive_reviews': 0,
            'aggregated_negative_reviews': 0,
            'aggregated_answered_count': 0,
            'aggregated_avg_response_time': 0.0,
        }

        try:
            self._wait_requests_finished()


            card_elements = self.driver.get_elements_by_locator(
                ('css selector', 'a[href*="/firm/"]'))

            if not card_elements:
                logger.warning("No firm links found on the initial page.")
                card_elements = self.driver.get_elements_by_locator(('css selector', 'a[href*="/station/"]'))
                if not card_elements:
                    logger.warning("No station links found either.")

            processed_urls = set()

            for element in card_elements:
                if len(card_data_list) >= self._max_records:
                    break

                try:
                    card_url = element.get('href')
                    if not card_url or card_url in processed_urls:
                        continue

                    if not card_url.startswith('http'):
                        card_url = urllib.parse.urljoin("https://2gis.ru", card_url)

                    processed_urls.add(card_url)

                    self.driver.navigate(card_url)
                    self._wait_requests_finished(timeout=20)

                    response = self.driver.wait_response(r'https://catalog\.api\.2gis\..*/items/byid', timeout=15)
                    if response:
                        response_body = self.driver.get_response_body(response)
                        try:
                            item_data_dict = json.loads(response_body)
                            parsed_card_data = self._get_item_data_from_response(item_data_dict)
                            if parsed_card_data:
                                card_data_list.append(parsed_card_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Could not decode JSON from response for {card_url}")
                        except Exception as e:
                            logger.error(f"Error processing API response data for {card_url}: {e}")
                    else:
                        logger.warning(f"No API response found for {card_url}")

                except Exception as e:
                    logger.error(f"Error processing card element with URL {card_url}: {e}")

            aggregated_info['total_cards_found'] = len(card_data_list)

        except Exception as e:
            logger.error(f"Error during 2GIS parsing: {e}", exc_info=True)

        return {'aggregated_info': aggregated_info, 'cards_data': card_data_list}
