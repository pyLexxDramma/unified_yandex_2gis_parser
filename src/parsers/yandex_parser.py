from __future__ import annotations

import re
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

from src.parsers.base_parser import BaseParser, BaseWriter, logger
from src.drivers.base_driver import BaseDriver
from src.config.settings import AppConfig

from bs4 import BeautifulSoup

DOMNode = Dict[str, Any]


class YandexParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        if not isinstance(driver, BaseDriver):
            raise TypeError("YandexParser requires a BaseDriver instance.")

        super().__init__(driver, settings)
        self._url: str = ""

        self._captcha_wait_time: int = getattr(self.settings.parser, 'yandex_captcha_wait', 20)
        self._reviews_scroll_step: int = getattr(self.settings.parser, 'yandex_reviews_scroll_step', 500)
        self._reviews_scroll_iterations_max: int = getattr(self.settings.parser, 'yandex_reviews_scroll_max_iter', 100)
        self._reviews_scroll_iterations_min: int = getattr(self.settings.parser, 'yandex_reviews_scroll_min_iter', 30)
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

            'card_name': 'Название карточки',
            'card_address': 'Адрес карточки',
            'card_rating': 'Рейтинг карточки',
            'card_reviews_count': 'Отзывов по карточке',
            'card_website': 'Сайт карточки',
            'card_phone': 'Телефон карточки',
            'card_rubrics': 'Рубрики карточки',
            'card_response_status': 'Статус ответа (карточка)',
            'card_avg_response_time': 'Среднее время ответа (карточка)',
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
        return r'https?://yandex\.ru/maps/\?.*'

    def _get_page_source_and_soup(self) -> Tuple[str, BeautifulSoup]:
        page_source = self.driver.get_page_source()
        soup = BeautifulSoup(page_source, "lxml")
        return page_source, soup

    def check_captcha(self) -> None:
        page_source, soup = self._get_page_source_and_soup()

        is_captcha = soup.find("div", {"class": "CheckboxCaptcha"}) or \
                     soup.find("div", {"class": "AdvancedCaptcha"})

        if is_captcha:
            self._logger.warning(f"Captcha detected. Waiting for {self._captcha_wait_time} seconds.")
            time.sleep(self._captcha_wait_time)
            self.check_captcha()

    def _get_card_snippet_data(self, card_element: DOMNode) -> Optional[Dict[str, Any]]:
        try:
            name_element = card_element.select_one('h1.card-title-view__title')
            name = name_element.getText() if name_element else ''

            address_element = card_element.select_one('div.business-contacts-view__address-link')
            address = address_element.getText() if address_element else ''

            rating_element = card_element.select_one('span.business-rating-badge-view__rating-text')
            rating = rating_element.getText() if rating_element else ''

            reviews_element = card_element.select_one('a.business-review-view__rating')
            reviews_count_text = reviews_element.getText().strip() if reviews_element else ''
            reviews_count = 0
            if reviews_count_text:
                match = re.search(r'\d+', reviews_count_text)
                if match:
                    reviews_count = int(match.group(0))

            website_element = card_element.select_one('a[itemprop="url"]')
            website = website_element.get('href') if website_element else ''

            positive_reviews = 0
            negative_reviews = 0
            response_status = "UNKNOWN"
            avg_response_time_days = ""
            reviews_texts = ""

            return {
                'card_name': name,
                'card_address': address,
                'card_rating': rating,
                'card_reviews_count': reviews_count,
                'card_website': website,
                'card_phone': '',
                'card_rubrics': '',
                'card_response_status': response_status,
                'card_avg_response_time': avg_response_time_days,
                'card_reviews_positive': positive_reviews,
                'card_reviews_negative': negative_reviews,
                'card_reviews_texts': reviews_texts,
            }
        except Exception as e:
            self._logger.error(f"Error processing Yandex card snippet: {e}")
            return None

    def get_reviews_data(self, driver: BaseDriver) -> Dict[str, Any]:
        reviews_info = {'reviews_count': 0, 'positive_reviews': 0, 'negative_reviews': 0}

        try:
            page_source, soup_content = self._get_page_source_and_soup()
        except Exception as e:
            self._logger.error(f"Failed to get page source before handling reviews: {e}")
            return reviews_info

        reviews_count = 0
        try:
            count_elements = soup_content.select('div.tabs-select-view__counter')
            if count_elements:
                reviews_count_text = count_elements[-1].text.strip()
                match = re.search(r'\d+', reviews_count_text)
                if match:
                    reviews_count = int(match.group(0))
                self._logger.info(f"Total reviews found: {reviews_count}")
            else:
                self._logger.warning("Could not find reviews count element.")
        except (ValueError, AttributeError, IndexError) as e:
            self._logger.warning(f"Could not determine review count: {e}")
        except Exception as e:
            self._logger.error(f"Unexpected error getting review count: {e}")
            return reviews_info

        reviews_info['reviews_count'] = reviews_count

        if reviews_count > 0:
            if reviews_count > 150:
                find_range = range(self._reviews_scroll_iterations_max)
            else:
                find_range = range(self._reviews_scroll_iterations_min)

            scroll_container_selector = '.scroll__container'
            for i in find_range:
                try:
                    scroll_elements = driver.get_elements_by_locator(('css selector', scroll_container_selector))
                    if scroll_elements and len(scroll_elements) > 1:
                        script_to_execute = f"document.querySelectorAll('{scroll_container_selector}')[1].scrollTop={self._reviews_scroll_step * i};"
                        driver.execute_script(script_to_execute)
                        time.sleep(0.2)
                    else:
                        self._logger.warning(
                            f"Scroll container not found or unexpected structure at iteration {i}. Stopping scroll.")
                        break
                except Exception as e:
                    self._logger.warning(f"Error during scroll iteration {i}: {e}. Stopping scroll.")
                    break

            try:
                page_source, soup_content = self._get_page_source_and_soup()
                positive_reviews = 0
                negative_reviews = 0

                for review_element in soup_content.select('div.business-review-view'):
                    rating_element = review_element.select_one('span.business-review-view__rating-text')

                    if rating_element:
                        rating_str = rating_element.getText().strip()
                        try:
                            rating_num = float(rating_str)
                            if 4.0 <= rating_num <= 5.0:
                                positive_reviews += 1
                            elif 1.0 <= rating_num <= 3.0:
                                negative_reviews += 1
                        except (ValueError, TypeError):
                            pass

                reviews_info['positive_reviews'] = positive_reviews
                reviews_info['negative_reviews'] = negative_reviews
                self._logger.info(f"Classified {positive_reviews} positive and {negative_reviews} negative reviews.")

            except Exception as e:
                self._logger.error(f"Error extracting review texts and classifying: {e}")

        return reviews_info

    def parse(self, writer: BaseWriter) -> None:
        if not isinstance(self.driver, BaseDriver):
            self._logger.error("Invalid driver type provided to YandexParser.")
            return

        search_url = self._url
        query_match = re.search(r'text=([^&]+)', search_url)
        if query_match:
            self._search_query_name = urllib.parse.unquote(query_match.group(1)).replace('+', ' ')
        else:
            self._search_query_name = 'Unknown Search'

        try:
            self.driver.navigate(search_url, timeout=120)
            self.check_captcha()
        except Exception as e:
            self._logger.error(f"Failed to navigate to {search_url}: {e}")
            return

        self._current_page_number = 1

        while self._aggregated_data['total_cards'] < self._max_records:
            self._logger.info(f"Processing page {self._current_page_number}...")

            try:
                page_source, soup_content = self._get_page_source_and_soup()
            except Exception as e:
                self._logger.error(f"Failed to get page source for page {self._current_page_number}: {e}")
                break

            company_card_selector = 'div.search-business-snippet-view'
            company_cards = soup_content.select(company_card_selector)

            if not company_cards:
                self._logger.info(f"No company cards found on page {self._current_page_number}.")
                if self._current_page_number == 1 and not self._aggregated_data['total_cards']:
                    pass
                elif self._current_page_number > 1:
                    self._logger.info(
                        "No cards on current page, but cards were found earlier. Assuming end of results.")
                    break

            for card_element in company_cards:
                if self._aggregated_data['total_cards'] >= self._max_records:
                    break

                processed_item = self._get_card_snippet_data(card_element)

                if processed_item:
                    self._aggregated_data['total_cards'] += 1

                    rating_str = processed_item.get('card_rating', '')
                    try:
                        if rating_str:
                            self._aggregated_data['total_rating_sum'] += float(rating_str)
                    except (ValueError, TypeError):
                        pass

                    reviews_count = processed_item.get('card_reviews_count', 0)
                    self._aggregated_data['total_reviews_count'] += reviews_count

                    # Положительные/отрицательные отзывы - из сниппета их не получить, пока оставляем 0
                    # Если нужно, потребуется кликать на каждую карточку и парсить детальную страницу.

                    self._collected_card_data.append(processed_item)
                    self._logger.debug(f"Collected snippet data for: {processed_item.get('card_name')}")
                else:
                    self._logger.warning("No processed item data extracted for a card.")

            if self._aggregated_data['total_cards'] >= self._max_records:
                self._logger.info(f'Reached maximum allowed records ({self._max_records}). Stopping parse.')
                break

            pagination_links = self._get_page_navigation_links()  # Адаптировать селектор для Яндекс!
            # Пример селектора для Яндекс: 'a.link.link_size_s.link_theme_normal.link_jslider-page-item'
            # Или: 'a.pager__link.pager__link_direction_next'
            # Проверить реальную структуру страницы.
            # Для примера, используем generic селектор, который может не работать.
            if not pagination_links:
                self._logger.info(f"No pagination links found on page {self._current_page_number}.")
                break

            next_page_number = self._current_page_number + 1

            if next_page_number in pagination_links:
                self._logger.info(f"Navigating to page {next_page_number}...")
                navigated_page = self._go_page(next_page_number, pagination_links)
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
            final_rating = round(self._aggregated_data['total_rating_sum'] / self._aggregated_data['total_cards'], 2) if \
                self._aggregated_data['total_cards'] > 0 and self._aggregated_data['total_rating_sum'] else ''

            final_record = {
                'search_query_name': self._search_query_name,
                'total_cards_found': self._aggregated_data['total_cards'],
                'aggregated_rating': final_rating,
                'aggregated_reviews_count': self._aggregated_data['total_reviews_count'],
                'aggregated_positive_reviews': self._aggregated_data['total_positive_reviews'],
                'aggregated_negative_reviews': self._aggregated_data['total_negative_reviews'],
                'aggregated_answered_count': self._aggregated_data['total_answered_count'],
                'aggregated_avg_response_time': self._aggregated_data['total_response_time_sum_days'],

                'card_name': self._collected_card_data[0].get('card_name',
                                                              'N/A') if self._collected_card_data else 'N/A',
                'card_address': self._collected_card_data[0].get('card_address',
                                                                 '') if self._collected_card_data else '',
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
