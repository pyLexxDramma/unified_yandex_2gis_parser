from __future__ import annotations
import json
import re
import logging
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, Field
from selenium.webdriver.remote.webelement import WebElement as SeleniumWebElement

from src.drivers.base_driver import BaseDriver, DOMNode
from src.config.settings import AppConfig
from src.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


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
            'card_avg_response_time': 'Среднее время ответа (дни, карточка)',
            'card_reviews_positive': 'Положительных отзывов (карточка)',
            'card_reviews_negative': 'Отрицательных отзывов (карточка)',
            'card_reviews_texts': 'Тексты отзывов (карточка)',
            'review_rating': 'Оценка отзыва',
            'review_text': 'Текст отзыва',
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
            logger.warning(f"Captcha detected. Waiting for {self._captcha_wait_time} seconds.")
            time.sleep(self._captcha_wait_time)
            self.check_captcha()

    def _get_card_snippet_data(self, card_element: Tag) -> Optional[Dict[str, Any]]:
        try:
            name_element = card_element.select_one('h1.card-title-view__title')
            name = name_element.get_text(strip=True) if name_element else ''

            address_element = card_element.select_one('div.business-contacts-view__address-link')
            address = address_element.get_text(strip=True) if address_element else ''

            rating_element = card_element.select_one('span.business-rating-badge-view__rating-text')
            rating = rating_element.get_text(strip=True) if rating_element else ''

            reviews_element = card_element.select_one('a.business-review-view__rating')
            reviews_count_text = reviews_element.get_text(strip=True) if reviews_element else ''
            reviews_count = 0
            if reviews_count_text:
                match = re.search(r'(\d+)', reviews_count_text)
                if match:
                    reviews_count = int(match.group(0))

            website_element = card_element.select_one('a[itemprop="url"]')
            website = website_element.get('href') if website_element else ''

            phone_element = card_element.select_one('span.business-contacts-view__phone-number')
            phone = phone_element.get_text(strip=True) if phone_element else ''

            rubrics_elements = card_element.select('a.rubric-view__title')
            rubrics = "; ".join([r.get_text(strip=True) for r in rubrics_elements]) if rubrics_elements else ''

            return {
                'card_name': name,
                'card_address': address,
                'card_rating': rating,
                'card_reviews_count': reviews_count,
                'card_website': website,
                'card_phone': phone,
                'card_rubrics': rubrics,
                'card_response_status': "UNKNOWN",
                'card_avg_response_time': "",
                'card_reviews_positive': 0,
                'card_reviews_negative': 0,
                'card_reviews_texts': "",
                'review_rating': None,
                'review_text': None,
            }
        except Exception as e:
            logger.error(f"Error processing Yandex card snippet: {e}")
            return None

    def _get_card_reviews_info(self) -> Dict[str, Any]:
        reviews_info = {'reviews_count': 0, 'positive_reviews': 0, 'negative_reviews': 0, 'texts': [], 'details': []}

        try:
            page_source, soup_content = self._get_page_source_and_soup()
        except Exception as e:
            logger.error(f"Failed to get page source before handling reviews: {e}")
            return reviews_info

        reviews_count_total = 0
        try:
            count_elements = soup_content.select('div.tabs-select-view__counter')
            if count_elements:
                reviews_count_text = count_elements[-1].get_text(strip=True)
                match = re.search(r'(\d+)', reviews_count_text)
                if match:
                    reviews_count_total = int(match.group(0))
                logger.info(f"Total reviews found on page: {reviews_count_total}")
            else:
                logger.warning("Could not find reviews count element.")
        except (ValueError, AttributeError, IndexError) as e:
            logger.warning(f"Could not determine review count: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting review count: {e}")
            return reviews_info

        if reviews_count_total == 0:
            return reviews_info

        scroll_iterations = 0
        max_scroll_iterations = self._reviews_scroll_iterations_max
        min_scroll_iterations = self._reviews_scroll_iterations_min
        scroll_step = self._reviews_scroll_step

        while scroll_iterations < max_scroll_iterations:
            logger.info(f"Scrolling to load more reviews. Iteration: {scroll_iterations + 1}")
            try:
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                time.sleep(1)
                scroll_iterations += 1
                page_source, soup_content = self._get_page_source_and_soup()

                current_reviews_count = 0
                count_elements = soup_content.select('div.tabs-select-view__counter')
                if count_elements:
                    reviews_count_text = count_elements[-1].get_text(strip=True)
                    match = re.search(r'(\d+)', reviews_count_text)
                    if match:
                        current_reviews_count = int(match.group(0))

                if current_reviews_count >= reviews_count_total:
                    logger.info("All reviews loaded.")
                    break

            except Exception as e:
                logger.error(f"Error during scrolling for reviews: {e}")
                break

        if scroll_iterations < min_scroll_iterations:
            logger.warning(f"Scroll iterations ({scroll_iterations}) less than minimum ({min_scroll_iterations}).")

        try:
            review_cards = soup_content.select('div.review-card-view')
            for card in review_cards:
                try:
                    rating_element = card.select_one('span.business-rating-badge-view__rating-text')
                    rating_text = rating_element.get_text(strip=True) if rating_element else "0"
                    rating_value = float(rating_text) if rating_text.replace('.', '', 1).isdigit() else 0.0

                    positive = 0
                    negative = 0
                    if rating_value >= 4.0:
                        positive = 1
                    elif rating_value < 3.0:
                        negative = 1

                    review_text_element = card.select_one('div.business-review-view__body-text')
                    review_text = review_text_element.get_text(strip=True) if review_text_element else ""

                    reviews_info['details'].append({
                        'review_rating': rating_value,
                        'review_text': review_text
                    })

                    if rating_value >= 4.0:
                        reviews_info['positive_reviews'] += 1
                    elif rating_value < 3.0:
                        reviews_info['negative_reviews'] += 1

                except Exception as e:
                    logger.warning(f"Error processing individual review card: {e}")

            reviews_info['reviews_count'] = len(review_cards)
        except Exception as e:
            logger.error(f"Error processing review cards: {e}")

        return reviews_info

    def _parse_cards(self, search_query_url: str) -> List[Dict[str, Any]]:
        logger.info(f"Navigating to search results page: {search_query_url}")
        self.driver.navigate(search_query_url)
        self.check_captcha()

        processed_urls = set()

        while len(self._collected_card_data) < self._max_records:
            logger.info(f"Processing Yandex Maps page (current cards collected: {len(self._collected_card_data)})")
            self.check_captcha()

            try:
                page_source, soup = self._get_page_source_and_soup()
                cards_on_page = soup.select('div.card-view, div.search-business-snippet-view')

                if not cards_on_page:
                    logger.info("No cards found on this page. Stopping.")
                    break

                for card_element in cards_on_page:
                    if len(self._collected_card_data) >= self._max_records:
                        break

                    card_snippet = self._get_card_snippet_data(card_element)
                    if card_snippet and card_snippet.get('card_name'):
                        try:
                            card_link_element = card_element.select_one(
                                'a.card-view__link, a.search-business-snippet-view__title')
                            card_url = card_link_element.get('href') if card_link_element else None

                            if card_url:
                                if not card_url.startswith('http'):
                                    card_url = urllib.parse.urljoin("https://yandex.ru", card_url)

                                if card_url in processed_urls:
                                    continue
                                processed_urls.add(card_url)

                                self.driver.navigate(card_url)
                                self.check_captcha()
                                card_details_soup = BeautifulSoup(self.driver.get_page_source(), "lxml")

                                card_name_detail = card_details_soup.select_one('h1.card-title-view__title')
                                card_snippet['card_name'] = card_name_detail.get_text(
                                    strip=True) if card_name_detail else card_snippet['card_name']

                                address_detail = card_details_soup.select_one(
                                    'div.business-contacts-view__address-link')
                                card_snippet['card_address'] = address_detail.get_text(
                                    strip=True) if address_detail else card_snippet['card_address']

                                rating_detail = card_details_soup.select_one(
                                    'span.business-rating-badge-view__rating-text')
                                card_snippet['card_rating'] = rating_detail.get_text(strip=True) if rating_detail else \
                                    card_snippet['card_rating']

                                website_detail = card_details_soup.select_one('a[itemprop="url"]')
                                card_snippet['card_website'] = website_detail.get('href') if website_detail else \
                                    card_snippet['card_website']

                                phone_detail = card_details_soup.select_one('span.business-contacts-view__phone-number')
                                card_snippet['card_phone'] = phone_detail.get_text(strip=True) if phone_detail else ""

                                rubrics_detail = card_details_soup.select('a.rubric-view__title')
                                card_snippet['card_rubrics'] = "; ".join(
                                    [r.get_text(strip=True) for r in rubrics_detail]) if rubrics_detail else ""

                                reviews_data = self._get_card_reviews_info()
                                card_snippet['card_reviews_count'] = reviews_data.get('reviews_count', 0)
                                card_snippet['card_reviews_positive'] = reviews_data.get('positive_reviews', 0)
                                card_snippet['card_reviews_negative'] = reviews_data.get('negative_reviews', 0)
                                card_snippet['card_reviews_texts'] = "; ".join(reviews_data.get('texts', []))

                                try:
                                    card_rating_float = float(card_snippet['card_rating']) if isinstance(
                                        card_snippet['card_rating'], str) and card_snippet['card_rating'].replace('.',
                                                                                                                  '',
                                                                                                                  1).isdigit() else 0.0
                                    self._aggregated_data['total_rating_sum'] += card_rating_float
                                    self._aggregated_data['total_reviews_count'] += card_snippet['card_reviews_count']
                                    self._aggregated_data['total_positive_reviews'] += card_snippet[
                                        'card_reviews_positive']
                                    self._aggregated_data['total_negative_reviews'] += card_snippet[
                                        'card_reviews_negative']

                                    self._aggregated_data['total_answered_count'] += 1 if card_snippet.get(
                                        'card_response_status', 'UNKNOWN') != 'UNKNOWN' else 0
                                    self._aggregated_data['total_response_time_sum_days'] += 0.0

                                except (ValueError, TypeError) as e:
                                    logger.warning(
                                        f"Could not parse rating or other data for aggregation for card '{card_snippet.get('card_name', 'Unknown')}': {e}")

                                card_snippet['detailed_reviews'] = reviews_data.get('details', [])

                                self._collected_card_data.append(card_snippet)
                            else:
                                logger.warning(
                                    f"Card link not found or href is empty for card snippet: {card_snippet.get('card_name', 'Unknown')}")
                        except Exception as e:
                            logger.warning(
                                f"Could not fully process card: {card_snippet.get('card_name', 'Unknown')}. Error: {e}")
                    else:
                        logger.warning(
                            f"Skipping card snippet due to missing name or basic data: {card_element.get_text(strip=True)[:50]}...")

                if len(self._collected_card_data) >= self._max_records:
                    logger.info(f"Max records ({self._max_records}) reached. Stopping card parsing.")
                    break

                next_page_button = soup.find('a', {'aria-label': 'Следующая страница'})
                if next_page_button and next_page_button.get('href'):
                    next_page_url = urllib.parse.urljoin("https://yandex.ru", next_page_button.get('href'))
                    if next_page_url in processed_urls:
                        logger.info("Next page URL already processed. Stopping pagination.")
                        break

                    self.driver.navigate(next_page_url)
                    page_number += 1
                    time.sleep(2)
                else:
                    logger.info("No next page button found or no href. Stopping pagination.")
                    break
            except Exception as e:
                logger.error(f"Error processing Yandex Maps page: {e}", exc_info=True)
                break

        return self._collected_card_data

    def parse(self, url: str) -> Dict[str, Any]:
        self._url = url
        self._search_query_name = url.split('text=')[-1].split('&')[0] if 'text=' in url else "YandexMapsSearch"
        logger.info(f"Starting Yandex Parser for URL: {url}")

        collected_cards_data = self._parse_cards(url)

        if not collected_cards_data:
            logger.warning("No data was collected from Yandex Maps.")
            return {'aggregated_info': {}, 'cards_data': []}

        total_cards = len(collected_cards_data)
        self._aggregated_data['total_cards'] = total_cards

        if total_cards > 0:
            valid_ratings = [float(card.get('card_rating', 0)) if isinstance(card.get('card_rating'), str) and card.get(
                'card_rating').replace('.', '', 1).isdigit() else 0.0 for card in collected_cards_data]
            avg_rating = sum(valid_ratings) / total_cards if total_cards > 0 else 0.0

            self._aggregated_data['aggregated_rating'] = avg_rating
            self._aggregated_data['total_reviews_count'] = sum(
                [card.get('card_reviews_count', 0) for card in collected_cards_data])
            self._aggregated_data['total_positive_reviews'] = sum(
                [card.get('card_reviews_positive', 0) for card in collected_cards_data])
            self._aggregated_data['total_negative_reviews'] = sum(
                [card.get('card_reviews_negative', 0) for card in collected_cards_data])
            self._aggregated_data['total_answered_count'] = sum(
                [1 for card in collected_cards_data if card.get('card_response_status', 'UNKNOWN') != 'UNKNOWN'])
            self._aggregated_data['total_response_time_sum_days'] = sum([float(
                card.get('card_avg_response_time', 0)) if isinstance(card.get('card_avg_response_time'),
                                                                     str) and card.get(
                'card_avg_response_time').replace('.', '', 1).isdigit() else 0.0 for card in collected_cards_data])

        else:
            avg_rating = 0.0

        aggregated_info = {
            'search_query_name': self._search_query_name,
            'total_cards_found': total_cards,
            'aggregated_rating': round(avg_rating, 2),
            'aggregated_reviews_count': self._aggregated_data['total_reviews_count'],
            'aggregated_positive_reviews': self._aggregated_data['total_positive_reviews'],
            'aggregated_negative_reviews': self._aggregated_data['total_negative_reviews'],
            'aggregated_answered_count': self._aggregated_data['total_answered_count'],
            'aggregated_avg_response_time': round(self._aggregated_data['total_response_time_sum_days'] / total_cards,
                                                  2) if total_cards > 0 and self._aggregated_data[
                'total_response_time_sum_days'] else 0.0,
        }

        return {'aggregated_info': aggregated_info, 'cards_data': collected_cards_data}
