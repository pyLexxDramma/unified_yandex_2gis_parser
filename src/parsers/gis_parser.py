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

        self._skip_404_response: bool = self.settings.parser.skip_404_response
        self._delay_between_clicks: int = self.settings.parser.delay_between_clicks
        self._max_records: int = self.settings.parser.max_records

        self._data_mapping: Dict[str, str] = {
            'search_query_name': 'Название поиска',
            'total_cards_found': 'Всего карточек найдено',
            'aggregated_rating': 'Общий рейтинг',
            'aggregated_reviews_count': 'Всего отзывов',
            'aggregated_positive_reviews': 'Всего положительных отзывов',
            'aggregated_negative_reviews': 'Всего отрицательных отзывов',
            'aggregated_answered_count': 'Всего отвечено (карточки)',
            'aggregated_avg_response_time': 'Среднее время ответа (дни)',

            # --- Детальные данные по каждой карточке ---
            'card_name': 'Название карточки',
            'card_rating': 'Рейтинг карточки',
            'card_reviews_count': 'Отзывов по карточке',
            'card_website': 'Сайт карточки',
            'card_phone': 'Телефон карточки',
            'card_rubrics': 'Рубрики карточки',
            'card_response_status': 'Статус ответа (карточка)',
            'card_avg_response_time': 'Среднее время ответа (карточка)',  # Сложно извлечь
            'card_reviews_positive': 'Положительных отзывов (карточка)',
            'card_reviews_negative': 'Отрицательных отзывов (карточка)',
            'card_reviews_texts': 'Тексты отзывов (карточка)',
        }

        self._current_page_number: int = 1
        self._aggregated_data: Dict[str, Any] = {
            'total_cards': 0,
            'total_rating_sum': 0.0,
            'total_reviews_count': 0,
            'total_positive_reviews': 0,
            'total_negative_reviews': 0,
            'total_answered_count': 0,
            'total_response_time_sum_days': 0.0,
        }
        self._collected_card_data: List[Dict[str, Any]] = []
        self._search_query_name: str = ""

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

    def _get_page_navigation_links(self) -> Dict[int, DOMNode]:
        nav_links = {}
        try:
            pagination_elements = self.driver.get_elements_by_locator(('css selector', '.pagination-item'))
            for link_node in pagination_elements:
                text = link_node.get('text', '').strip()
                if text.isdigit():
                    page_number = int(text)
                    nav_links[page_number] = link_node
            return nav_links
        except Exception as e:
            self._logger.error(f"Error finding pagination links: {e}")
            return {}

    def _go_page(self, n_page: int, available_pages: Dict[int, DOMNode]) -> Optional[int]:
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

    def _get_item_data_from_response(self, response_data: Dict[str, Any], card_url: str) -> Optional[Dict[str, Any]]:
        try:
            items = response_data.get('items')
            if not items or not isinstance(items, list) or not items[0]:
                self._logger.warning("API response has no items or items is not a list.")
                return None

            item = items[0]

            name = item.get('name', '')
            rating = item.get('rating', '')
            reviews_count = item.get('reviews_count', 0)

            card_website = item.get('attributes', {}).get('website', '')
            card_phone = self._format_phones(item.get('attributes', {}).get('phones', []))
            card_rubrics = self._format_rubrics(item.get('rubrics', []))

            all_reviews = item.get('reviews', [])
            positive_reviews = 0
            negative_reviews = 0
            reviews_texts = []

            for review in all_reviews:
                review_rating = review.get('rating')
                review_text = review.get('text', '')
                if review_text:
                    reviews_texts.append(f"[{review_rating}] {review_text}")

                if review_rating is not None:
                    try:
                        review_rating_num = float(review_rating)
                        if 4.0 <= review_rating_num <= 5.0:
                            positive_reviews += 1
                        elif 1.0 <= review_rating_num <= 3.0:
                            negative_reviews += 1
                    except (ValueError, TypeError):
                        pass

            answered_count = item.get('metadata', {}).get('answered_count', 0)
            avg_response_time_days = item.get('metadata', {}).get('avg_response_time_days', '')

            return {
                'name': name,
                'card_name': name,
                'card_rating': rating,
                'card_reviews_count': reviews_count,
                'card_website': card_website,
                'card_phone': card_phone,
                'card_rubrics': card_rubrics,
                'card_response_status': "Yes" if answered_count > 0 else "No",  # Простая индикация, есть ли ответ
                'card_avg_response_time': avg_response_time_days,
                'card_reviews_positive': positive_reviews,
                'card_reviews_negative': negative_reviews,
                'card_reviews_texts': "; ".join(reviews_texts),  # Собираем все тексты отзывов
            }
        except Exception as e:
            self._logger.error(f"Error extracting item data from API response: {e}")
            return None

    def parse(self, writer: BaseWriter) -> None:
        if not isinstance(self.driver, BaseDriver):
            self._logger.error("Invalid driver type provided to GisParser.")
            return

        search_url = self._url
        self._search_query_name = search_url.split('/search/')[1].split('/')[
            0] if '/search/' in search_url else 'Unknown Search'
        query_match = re.search(r'/search/([^/]+)', search_url)
        if query_match:
            self._search_query_name = urllib.parse.unquote(query_match.group(1)).replace('+', ' ')
        else:
            self._search_query_name = 'Unknown Search'

        try:
            self.driver.navigate(search_url, timeout=120)
        except Exception as e:
            self._logger.error(f"Failed to navigate to {search_url}: {e}")
            return

        self._current_page_number = 1

        while self._aggregated_data['total_cards'] < self._max_records:
            self._logger.info(f"Processing page {self._current_page_number}...")

            if not self._wait_requests_finished():
                self._logger.warning(f"Requests did not finish on page {self._current_page_number}. Proceeding.")

            card_links = self._get_links()
            if not card_links:
                self._logger.info(f"No company card links found on page {self._current_page_number}.")
                if self._current_page_number > 1 and self._aggregated_data['total_cards'] > 0:
                    self._logger.info(
                        "No cards on current page, but cards were found earlier. Assuming end of results.")
                    break

            cards_processed_on_page = 0
            for link_node in card_links:
                if self._aggregated_data['total_cards'] >= self._max_records:
                    break

                target_link_href = link_node.get('attributes', {}).get('href')
                if not target_link_href:
                    continue

                try:
                    self.driver.perform_click(link_node)
                    if self._delay_between_clicks > 0:
                        time.sleep(self._delay_between_clicks / 1000)
                except Exception as e:
                    self._logger.error(f"Failed to click on company link {target_link_href}: {e}")
                    continue

                response = self.driver.wait_response(r'https://catalog\.api\.2gis\..*/items/byid',
                                                     timeout=15)  # Паттерн для API 2ГИС
                if not response:
                    self._logger.error("Did not receive API response for the company card.")
                    continue

                response_body = self.driver.get_response_body(response, timeout=10)
                if not response_body:
                    self._logger.error("Response body is empty.")
                    continue

                try:
                    item_data = json.loads(response_body)
                except json.JSONDecodeError:
                    self._logger.error("Failed to decode JSON response body.")
                    continue

                processed_item = self._get_item_data_from_response(item_data, target_link_href)

                if processed_item:
                    self._collected_card_data.append(processed_item)
                    cards_on_this_page += 1
                    self._aggregated_data['total_cards'] += 1

                    rating_str = processed_item.get('card_rating', '')
                    try:
                        if rating_str:
                            self._aggregated_data['total_rating_sum'] += float(rating_str)
                    except (ValueError, TypeError):
                        pass

                    self._aggregated_data['total_reviews_count'] += processed_item.get('card_reviews_count', 0)
                    self._aggregated_data['total_positive_reviews'] += processed_item.get('card_reviews_positive', 0)
                    self._aggregated_data['total_negative_reviews'] += processed_item.get('card_reviews_negative', 0)

                    answered_count_str = processed_item.get('card_response_status',
                                                            '')
                    if answered_count_str == "Yes":
                        self._aggregated_data['total_answered_count'] += 1

                    avg_response_time_str = processed_item.get('card_avg_response_time', '')
                    try:
                        if avg_response_time_str:
                            self._aggregated_data['total_response_time_sum_days'] += float(avg_response_time_str)
                    except (ValueError, TypeError):
                        pass

                    self._logger.debug(f"Collected detailed data for: {processed_item.get('card_name')}")
                else:
                    self._logger.warning("No processed item data extracted for a card.")

            if self._aggregated_data['total_cards'] >= self._max_records:
                self._logger.info(f'Reached maximum allowed records ({self._max_records}). Stopping parse.')
                break

            available_pages = self._get_page_navigation_links()
            next_page_number = self._current_page_number + 1

            if next_page_number in available_pages:
                self._logger.info(f"Navigating to page {next_page_number}...")
                navigated_page = self._go_page(next_page_number, available_pages)
                if navigated_page:
                    self._current_page_number = navigated_page
                else:
                    self._logger.warning(f"Failed to navigate to page {next_page_number}. Stopping pagination.")
                    break
            else:
                self._logger.info(f"No next page found after page {self._current_page_number}. Ending pagination.")
                break

            self.driver.clear_requests()

        if self._aggregated_data['total_cards'] > 0:
            final_rating = (self._aggregated_data['total_rating_sum'] / self._aggregated_data['total_cards']) if \
                self._aggregated_data['total_cards'] > 0 else ''
            if final_rating:
                final_rating = round(final_rating, 2)

            avg_response_time_days = ''
            if self._aggregated_data['total_cards'] > 0 and self._aggregated_data['total_response_time_sum_days'] > 0:
                avg_response_time_days = round(
                    self._aggregated_data['total_response_time_sum_days'] / self._aggregated_data['total_cards'], 1)

            final_record = {
                'search_query_name': self._search_query_name,
                'total_cards_found': self._aggregated_data['total_cards'],
                'aggregated_rating': final_rating,
                'aggregated_reviews_count': self._aggregated_data['total_reviews_count'],
                'aggregated_positive_reviews': self._aggregated_data['total_positive_reviews'],
                'aggregated_negative_reviews': self._aggregated_data['total_negative_reviews'],
                'aggregated_answered_count': self._aggregated_data['total_answered_count'],
                'aggregated_avg_response_time': avg_response_time_days,

                'card_name': self._collected_card_data[0].get('card_name',
                                                              'N/A') if self._collected_card_data else 'N/A',
                'card_rating': self._collected_card_data[0].get('card_rating', '') if self._collected_card_data else '',
                'card_reviews_count': self._collected_card_data[0].get('card_reviews_count',
                                                                       '') if self._collected_card_data else '',
                'card_website': self._collected_card_data[0].get('card_website',
                                                                 '') if self._collected_card_data else '',
                'card_phone': self._collected_card_data[0].get('card_phone', '') if self._collected_card_data else '',
                'card_rubrics': self._collected_card_data[0].get('card_rubrics',
                                                                 '') if self._collected_card_data else '',
                'card_response_status': self._collected_card_data[0].get('card_response_status',
                                                                         '') if self._collected_card_data else '',
                'card_avg_response_time': self._collected_card_data[0].get('card_avg_response_time',
                                                                           '') if self._collected_card_data else '',
                'card_reviews_positive': self._collected_card_data[0].get('card_reviews_positive',
                                                                          0) if self._collected_card_data else 0,
                'card_reviews_negative': self._collected_card_data[0].get('card_reviews_negative',
                                                                          0) if self._collected_card_data else 0,
                'card_reviews_texts': self._collected_card_data[0].get('card_reviews_texts',
                                                                       '') if self._collected_card_data else '',
            }

            row_to_write = {}
            for key, header in self._data_mapping.items():
                row_to_write[header] = final_record.get(key, '')

            try:
                writer.write(row_to_write)
                self._logger.info(f"Successfully parsed and wrote aggregated data for: {final_record.get('name')}")
            except Exception as e:
                self._logger.error(f"Error writing aggregated data to writer: {e}")
        else:
            self._logger.info("No company data was collected.")

    def _format_phones(self, phones_data: List[Dict[str, Any]]) -> str:
        if not phones_data:
            return ""
        formatted_phones = []
        for phone_entry in phones_data:
            phone_number = phone_entry.get('number', '')
            comment = phone_entry.get('comment', '')
            full_phone_str = phone_number
            if comment:
                full_phone_str += f" ({comment})"
            formatted_phones.append(full_phone_str)
        return "; ".join(formatted_phones)

    def _format_rubrics(self, rubrics_data: List[Dict[str, Any]]) -> str:
        if not rubrics_data:
            return ""
        formatted_rubrics = []
        for rubric_entry in rubrics_data:
            rubric_name = rubric_entry.get('name', '')
            parent_name = rubric_entry.get('parent_name', '')
            full_rubric_str = rubric_name
            if parent_name:
                full_rubric_str = f"{parent_name} / {rubric_name}"
            formatted_rubrics.append(full_rubric_str)
        return "; ".join(formatted_rubrics)
