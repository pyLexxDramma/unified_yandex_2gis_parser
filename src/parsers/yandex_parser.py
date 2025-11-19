from __future__ import annotations
import json
import os
import re
import logging
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, Field
from selenium.webdriver.remote.webelement import WebElement as SeleniumWebElement

from src.drivers.base_driver import BaseDriver, DOMNode
from src.config.settings import AppConfig, Settings
from src.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class YandexParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        if not isinstance(driver, BaseDriver):
            raise TypeError("YandexParser requires a BaseDriver instance.")

        super().__init__(driver, settings)
        self._url: str = ""

        self._captcha_wait_time: int = getattr(self._settings.parser, 'yandex_captcha_wait', 20)
        self._reviews_scroll_step: int = getattr(self._settings.parser, 'yandex_reviews_scroll_step', 500)
        self._reviews_scroll_iterations_max: int = getattr(self._settings.parser, 'yandex_reviews_scroll_max_iter', 100)
        self._reviews_scroll_iterations_min: int = getattr(self._settings.parser, 'yandex_reviews_scroll_min_iter', 30)
        self._max_records: int = getattr(self._settings.parser, 'max_records', 1000)
        
        self._card_selectors: List[str] = getattr(self._settings.parser, 'yandex_card_selectors', [
            "div.search-business-snippet-view",
            "div.search-snippet-view__body._type_business",
            "a[href*='/maps/org/']:not([href*='/gallery/'])"
        ])
        self._scroll_container: str = getattr(self._settings.parser, 'yandex_scroll_container', 
                                               ".scroll__container, .scroll__content, .search-list-view__list")
        self._scrollable_element_selector: str = getattr(self._settings.parser, 'yandex_scrollable_element_selector',
                                                         ".scroll__container, .scroll__content, [class*='search-list-view'], [class*='scroll']")
        self._scroll_step: int = getattr(self._settings.parser, 'yandex_scroll_step', 400)
        self._scroll_max_iter: int = getattr(self._settings.parser, 'yandex_scroll_max_iter', 200)
        self._scroll_wait_time: float = getattr(self._settings.parser, 'yandex_scroll_wait_time', 1.5)
        self._min_cards_threshold: int = getattr(self._settings.parser, 'yandex_min_cards_threshold', 500)

        self._data_mapping: Dict[str, str] = {
            'search_query_name': 'Название поиска',
            'total_cards_found': 'Всего карточек найдено',
            'aggregated_rating': 'Общий рейтинг',
            'aggregated_reviews_count': 'Всего отзывов',
            'aggregated_positive_reviews': 'Всего положительных отзывов',
            'aggregated_negative_reviews': 'Всего отрицательных отзывов',
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
            'total_answered_reviews_count': 0,
            'total_unanswered_reviews_count': 0,
            'total_response_time_sum_days': 0.0,
            'total_response_time_calculated_count': 0,
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
            name_selectors = [
                'h1.card-title-view__title',
                '.search-business-snippet-view__title',
                'a.search-business-snippet-view__title',
                'a.catalogue-snippet-view__title',
                'a[class*="title"]',
                'h2[class*="title"]',
                'h3[class*="title"]',
            ]
            name = ''
            for selector in name_selectors:
                name_element = card_element.select_one(selector)
                if name_element:
                    name = name_element.get_text(strip=True)
                    if name:
                        break

            address_selectors = [
                'div.business-contacts-view__address-link',
                '.search-business-snippet-view__address',
                'div[class*="address"]',
                'span[class*="address"]',
            ]
            address = ''
            for selector in address_selectors:
                address_element = card_element.select_one(selector)
                if address_element:
                    address = address_element.get_text(strip=True)
                    if address:
                        break

            rating_selectors = [
                'span.business-rating-badge-view__rating-text',
                '.search-business-snippet-view__rating-text',
                'span[class*="rating"]',
                'div[class*="rating"]',
            ]
            rating = ''
            for selector in rating_selectors:
                rating_element = card_element.select_one(selector)
                if rating_element:
                    rating = rating_element.get_text(strip=True)
                    if rating:
                        break

            reviews_selectors = [
                'a.business-review-view__rating',
                '.search-business-snippet-view__link-reviews',
                'a[class*="review"]',
                'span[class*="review"]',
            ]
            reviews_count = 0
            for selector in reviews_selectors:
                reviews_element = card_element.select_one(selector)
                if reviews_element:
                    reviews_count_text = reviews_element.get_text(strip=True)
                    if reviews_count_text:
                        match = re.search(r'(\d+)', reviews_count_text)
                        if match:
                            reviews_count = int(match.group(0))
                            break
                if reviews_count > 0:
                    break

            website_selectors = [
                'a[itemprop="url"]',
                'a[class*="website"]',
                'a[href^="http"]',
            ]
            website = ''
            for selector in website_selectors:
                website_element = card_element.select_one(selector)
                if website_element:
                    website = website_element.get('href', '')
                    if website and 'yandex.ru' not in website:
                        break

            phone_selectors = [
                'span.business-contacts-view__phone-number',
                'a[href^="tel:"]',
                'span[class*="phone"]',
            ]
            phone = ''
            for selector in phone_selectors:
                phone_element = card_element.select_one(selector)
                if phone_element:
                    phone = phone_element.get_text(strip=True)
                    if not phone and phone_element.get('href'):
                        phone = phone_element.get('href').replace('tel:', '').strip()
                    if phone:
                        phone = phone.replace('Показать телефон', '').replace('показать телефон', '').strip()
                        break

            rubrics_elements = card_element.select('a.rubric-view__title, a[class*="rubric"], a[href*="/rubric/"]')
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
                'card_answered_reviews_count': 0,
                'card_unanswered_reviews_count': reviews_count,
                'detailed_reviews': [],
                'review_rating': None,
                'review_text': None,
            }
        except Exception as e:
            logger.error(f"Error processing Yandex card snippet: {e}")
            return None

    def _extract_card_data_from_detail_page(self, card_details_soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Извлекает данные карточки со страницы деталей организации."""
        try:
            card_snippet = {
                'card_name': '',
                'card_address': '',
                'card_rating': '',
                'card_reviews_count': 0,
                'card_website': '',
                'card_phone': '',
                'card_rubrics': '',
                'card_response_status': "UNKNOWN",
                'card_avg_response_time': "",
                'card_reviews_positive': 0,
                'card_reviews_negative': 0,
                'card_reviews_texts': "",
                'card_answered_reviews_count': 0,
                'card_unanswered_reviews_count': 0,
                'detailed_reviews': [],
            }
            
            # Расширенный поиск имени карточки
            name_selectors = [
                'h1.card-title-view__title',
                'h1[class*="title"]',
                'h1[class*="card-title"]',
                'h1.business-card-title-view__title',
                'h1',
                'div[class*="title"]',
                'span[class*="title"]',
            ]
            
            name_detail = None
            for selector in name_selectors:
                name_detail = card_details_soup.select_one(selector)
                if name_detail:
                    name_text = name_detail.get_text(strip=True)
                    if name_text:
                        card_snippet['card_name'] = name_text
                        logger.debug(f"Found card name using selector '{selector}': {name_text[:50]}")
                        break
            
            if not card_snippet.get('card_name'):
                logger.warning(f"Could not find card name on detail page. Available h1 tags: {[h.get_text(strip=True)[:50] for h in card_details_soup.select('h1')]}")

            address_detail = card_details_soup.select_one('div.business-contacts-view__address-link')
            card_snippet['card_address'] = address_detail.get_text(strip=True) if address_detail else ''

            rating_detail = card_details_soup.select_one('span.business-rating-badge-view__rating-text')
            card_snippet['card_rating'] = rating_detail.get_text(strip=True) if rating_detail else ''

            website_detail = card_details_soup.select_one('a[itemprop="url"], .business-website-view__link')
            card_snippet['card_website'] = website_detail.get('href') if website_detail else ''

            # Улучшенный поиск телефона
            phone_selectors = [
                'span.business-contacts-view__phone-number',
                'a[href^="tel:"]',
                'span[class*="phone"]',
                'div[class*="phone"]',
                'span[itemprop="telephone"]',
                'a.business-contacts-view__phone-link',
            ]
            
            phone_text = ""
            for selector in phone_selectors:
                phone_elements = card_details_soup.select(selector)
                if phone_elements:
                    for phone_elem in phone_elements:
                        phone_text = phone_elem.get_text(strip=True)
                        if not phone_text and phone_elem.get('href'):
                            href = phone_elem.get('href', '')
                            if href.startswith('tel:'):
                                phone_text = href.replace('tel:', '').strip()
                        if phone_text:
                            phone_text = phone_text.replace('Показать телефон', '').replace('показать телефон', '').strip()
                            break
                if phone_text:
                    break
            
            card_snippet['card_phone'] = phone_text

            # Улучшенный поиск рубрик
            rubric_selectors = [
                'a.rubric-view__title',
                'a[class*="rubric"]',
                'span[class*="rubric"]',
                'div[class*="rubric"]',
                'a[href*="/rubric/"]',
            ]
            
            rubrics_list = []
            for selector in rubric_selectors:
                rubrics_detail = card_details_soup.select(selector)
                if rubrics_detail:
                    for r in rubrics_detail:
                        rubric_text = r.get_text(strip=True)
                        if rubric_text and rubric_text not in rubrics_list:
                            rubrics_list.append(rubric_text)
                    if rubrics_list:
                        break
            
            card_snippet['card_rubrics'] = "; ".join(rubrics_list) if rubrics_list else ""

            # Улучшенный поиск информации об ответах
            response_selectors = [
                '.business-header-view__quick-response-badge',
                'div[class*="response"]',
                'span[class*="response"]',
                'div.business-response-view',
            ]
            
            response_status = "UNKNOWN"
            for selector in response_selectors:
                response_status_element = card_details_soup.select_one(selector)
                if response_status_element:
                    response_text = response_status_element.get_text(strip=True)
                    if response_text:
                        response_status = response_text
                        break
            
            card_snippet['card_response_status'] = response_status
            
            # Улучшенный поиск времени ответа
            time_selectors = [
                '.business-header-view__avg-response-time',
                'div[class*="response-time"]',
                'span[class*="response-time"]',
            ]
            
            avg_response_time_text = ""
            for selector in time_selectors:
                avg_response_time_element = card_details_soup.select_one(selector)
                if avg_response_time_element:
                    avg_response_time_text = avg_response_time_element.get_text(strip=True)
                    if avg_response_time_text:
                        break
            
            if avg_response_time_text:
                if "час" in avg_response_time_text.lower() or "hour" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(час|hour)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        hours = float(match.group(1))
                        card_snippet['card_avg_response_time'] = round(hours / 24, 2)
                elif "день" in avg_response_time_text.lower() or "day" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(день|day)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        card_snippet['card_avg_response_time'] = float(match.group(1))
                elif "недел" in avg_response_time_text.lower() or "week" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(недел|week)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        weeks = float(match.group(1))
                        card_snippet['card_avg_response_time'] = weeks * 7
                elif "месяц" in avg_response_time_text.lower() or "month" in avg_response_time_text.lower():
                    match = re.search(r'(\d+(\.\d+)?)\s*(месяц|month)', avg_response_time_text, re.IGNORECASE)
                    if match:
                        months = float(match.group(1))
                        card_snippet['card_avg_response_time'] = months * 30
                else:
                    card_snippet['card_avg_response_time'] = ""
            else:
                card_snippet['card_avg_response_time'] = ""

            # Собираем информацию об отзывах
            reviews_data = self._get_card_reviews_info()
            card_snippet['card_reviews_count'] = reviews_data.get('reviews_count', 0)
            card_snippet['card_reviews_positive'] = reviews_data.get('positive_reviews', 0)
            card_snippet['card_reviews_negative'] = reviews_data.get('negative_reviews', 0)
            
            # Сохраняем тексты отзывов
            review_texts = []
            for detail in reviews_data.get('details', []):
                if detail.get('review_text'):
                    review_texts.append(detail.get('review_text'))
            card_snippet['card_reviews_texts'] = "; ".join(review_texts)
            card_snippet['detailed_reviews'] = reviews_data.get('details', [])
            
            # Собираем информацию о количестве отвеченных отзывов
            answered_reviews_count = 0
            try:
                answered_selectors = [
                    'div[class*="answered"]',
                    'span[class*="answered"]',
                    'div.business-review-view__response',
                    'div.review-item-view__response',
                ]
                for selector in answered_selectors:
                    answered_elements = card_details_soup.select(selector)
                    if answered_elements:
                        answered_reviews_count = len(answered_elements)
                        break
            except Exception as e:
                logger.warning(f"Error counting answered reviews: {e}")
            
            card_snippet['card_answered_reviews_count'] = answered_reviews_count
            card_snippet['card_unanswered_reviews_count'] = max(0, card_snippet['card_reviews_count'] - answered_reviews_count)

            # Проверяем, что хотя бы имя карточки найдено
            if not card_snippet.get('card_name'):
                logger.warning(f"Card name is empty. Card snippet keys: {list(card_snippet.keys())}")
                # Сохраняем HTML для отладки
                try:
                    debug_html_path = os.path.join('output', f'debug_card_no_name_{int(time.time())}.html')
                    os.makedirs('output', exist_ok=True)
                    with open(debug_html_path, 'w', encoding='utf-8') as f:
                        f.write(str(card_details_soup))
                    logger.info(f"Saved debug HTML to {debug_html_path}")
                except Exception as e:
                    logger.error(f"Could not save debug HTML: {e}")
                return None
            
            logger.debug(f"Successfully extracted card data: name='{card_snippet.get('card_name', '')[:50]}', address='{card_snippet.get('card_address', '')[:50]}'")
            return card_snippet
        except Exception as e:
            logger.error(f"Error extracting card data from detail page: {e}", exc_info=True)
            return None

    def _update_aggregated_data(self, card_snippet: Dict[str, Any]) -> None:
        """Обновляет агрегированные данные на основе данных карточки."""
        try:
            rating_str = str(card_snippet.get('card_rating', '')).replace(',', '.').strip()
            try:
                card_rating_float = float(rating_str) if rating_str and rating_str.replace('.', '', 1).isdigit() else 0.0
            except (ValueError, TypeError):
                card_rating_float = 0.0
            
            self._aggregated_data['total_rating_sum'] += card_rating_float
            
            reviews_count = card_snippet.get('card_reviews_count', 0) or 0
            positive_reviews = card_snippet.get('card_reviews_positive', 0) or 0
            negative_reviews = card_snippet.get('card_reviews_negative', 0) or 0
            answered_reviews = card_snippet.get('card_answered_reviews_count', 0) or 0
            
            self._aggregated_data['total_reviews_count'] += reviews_count
            self._aggregated_data['total_positive_reviews'] += positive_reviews
            self._aggregated_data['total_negative_reviews'] += negative_reviews
            self._aggregated_data['total_answered_reviews_count'] += answered_reviews
            self._aggregated_data['total_unanswered_reviews_count'] += max(0, reviews_count - answered_reviews)

            if card_snippet.get('card_response_status') != 'UNKNOWN' or answered_reviews > 0:
                self._aggregated_data['total_answered_count'] += 1
                
            if card_snippet.get('card_avg_response_time'):
                try:
                    response_time_str = str(card_snippet['card_avg_response_time']).strip()
                    if response_time_str:
                        response_time_days = float(response_time_str)
                        if response_time_days > 0:
                            self._aggregated_data['total_response_time_sum_days'] += response_time_days
                            self._aggregated_data['total_response_time_calculated_count'] += 1
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not convert response time to float for card '{card_snippet.get('card_name', 'Unknown')}': {card_snippet.get('card_avg_response_time')}")
            
            logger.info(f"Aggregated data updated for '{card_snippet.get('card_name', 'Unknown')}': "
                       f"rating={card_rating_float}, reviews={reviews_count}, "
                       f"positive={positive_reviews}, negative={negative_reviews}")
        except Exception as e:
            logger.warning(
                f"Could not parse rating or other data for aggregation for card '{card_snippet.get('card_name', 'Unknown')}': {e}", exc_info=True)

    def _get_card_reviews_info(self) -> Dict[str, Any]:
        reviews_info = {'reviews_count': 0, 'positive_reviews': 0, 'negative_reviews': 0, 'texts': [], 'details': []}

        try:
            page_source, soup_content = self._get_page_source_and_soup()
        except Exception as e:
            logger.error(f"Failed to get page source before handling reviews: {e}")
            return reviews_info

        reviews_count_total = 0
        try:
            reviews_link = soup_content.select_one('a[href*="/reviews/"]')
            if reviews_link:
                reviews_url = reviews_link.get('href')
                if reviews_url:
                    if not reviews_url.startswith('http'):
                        reviews_url = urllib.parse.urljoin("https://yandex.ru", reviews_url)
                    logger.info(f"Navigating to reviews page: {reviews_url}")
                    try:
                        self.driver.navigate(reviews_url)
                        time.sleep(3)
                        page_source, soup_content = self._get_page_source_and_soup()
                    except Exception as nav_error:
                        logger.warning(f"Could not navigate to reviews page: {nav_error}")
        except Exception as e:
            logger.warning(f"Error trying to navigate to reviews: {e}")
        
        try:
            count_selectors = [
                'div.tabs-select-view__counter',
                '.search-business-snippet-view__link-reviews',
                'a[href*="/reviews/"]',
                'span.business-rating-badge-view__reviews-count',
                'div.business-header-view__reviews-count',
                'a.business-review-view__rating',
            ]
            
            for selector in count_selectors:
                count_elements = soup_content.select(selector)
            if count_elements:
                    for elem in count_elements:
                        reviews_count_text = elem.get_text(strip=True)
                        matches = re.findall(r'(\d+)', reviews_count_text)
                        if matches:
                            potential_count = max([int(m) for m in matches])
                            if potential_count > reviews_count_total:
                                reviews_count_total = potential_count
                                logger.info(f"Found reviews count {reviews_count_total} using selector: {selector}")
            
            if reviews_count_total > 0:
                logger.info(f"Total reviews found on page: {reviews_count_total}")
            else:
                logger.warning("Could not find reviews count element. Trying to navigate to reviews tab...")
                try:
                    reviews_tab = soup_content.select_one('a[href*="/reviews/"], button[data-tab="reviews"]')
                    if reviews_tab:
                        reviews_url = reviews_tab.get('href')
                        if reviews_url:
                            if not reviews_url.startswith('http'):
                                reviews_url = urllib.parse.urljoin("https://yandex.ru", reviews_url)
                            logger.info(f"Navigating to reviews page: {reviews_url}")
                            self.driver.navigate(reviews_url)
                            time.sleep(3)
                            page_source, soup_content = self._get_page_source_and_soup()
                            for selector in count_selectors:
                                count_elements = soup_content.select(selector)
                                if count_elements:
                                    for elem in count_elements:
                                        reviews_count_text = elem.get_text(strip=True)
                                        matches = re.findall(r'(\d+)', reviews_count_text)
                                        if matches:
                                            potential_count = max([int(m) for m in matches])
                                            if potential_count > reviews_count_total:
                                                reviews_count_total = potential_count
                except Exception as nav_error:
                    logger.warning(f"Could not navigate to reviews tab: {nav_error}")
        except (ValueError, AttributeError, IndexError) as e:
            logger.warning(f"Could not determine review count: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting review count: {e}")
            return reviews_info

        if reviews_count_total == 0:
            logger.warning("No reviews found or reviews count is 0")
            return reviews_info

        scroll_iterations = 0
        max_scroll_iterations = self._reviews_scroll_iterations_max
        min_scroll_iterations = self._reviews_scroll_iterations_min
        scroll_step = self._reviews_scroll_step
        
        scroll_container_script = """
        var containers = document.querySelectorAll('.scroll__container, [class*="scroll"], [class*="reviews"]');
        for (var i = 0; i < containers.length; i++) {
            var container = containers[i];
            if (container.scrollHeight > container.clientHeight && container.scrollHeight > 500) {
                return container;
            }
        }
        return null;
        """
        
        scroll_container = None
        try:
            scroll_container = self.driver.execute_script(scroll_container_script)
            if scroll_container:
                logger.info("Found scrollable container for reviews")
        except:
            pass

        last_review_count = 0
        no_change_count = 0

        while scroll_iterations < max_scroll_iterations:
            if scroll_iterations % 10 == 0:
                logger.info(f"Scrolling to load more reviews. Iteration: {scroll_iterations + 1}/{max_scroll_iterations}")
            try:
                # Прокручиваем контейнер или window
                if scroll_container:
                    scroll_script = f"""
                    var container = arguments[0];
                    var oldScrollTop = container.scrollTop;
                    var oldScrollHeight = container.scrollHeight;
                    container.scrollTop = Math.min(container.scrollTop + {scroll_step}, container.scrollHeight - container.clientHeight);
                    return {{
                        'scrolled': container.scrollTop !== oldScrollTop,
                        'newHeight': container.scrollHeight
                    }};
                    """
                    self.driver.execute_script(scroll_script, scroll_container)
                else:
                    self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                
                time.sleep(1.5)  # Увеличиваем время ожидания
                scroll_iterations += 1
                page_source, soup_content = self._get_page_source_and_soup()

                # Считаем текущее количество найденных карточек отзывов
                review_cards_temp = soup_content.select('div[class*="review-card"], div[class*="review-item"], div[class*="business-review"]')
                current_reviews_count = len(review_cards_temp)
                
                # Также пробуем получить общее количество из счетчика
                count_elements = soup_content.select(
                    'div.tabs-select-view__counter, .search-business-snippet-view__link-reviews, [class*="reviews-count"]')
                if count_elements:
                    for elem in count_elements:
                        reviews_count_text = elem.get_text(strip=True)
                    match = re.search(r'(\d+)', reviews_count_text)
                    if match:
                            potential_count = int(match.group(0))
                            if potential_count > current_reviews_count:
                                current_reviews_count = potential_count

                # Проверяем, изменилось ли количество
                if current_reviews_count == last_review_count:
                    no_change_count += 1
                    # Увеличиваем количество итераций без изменений до 20 для более тщательной загрузки
                    if no_change_count >= 20:  # 20 итераций без изменений
                        logger.info(f"Review count unchanged after {no_change_count} scrolls. Stopping.")
                        break
                    elif no_change_count % 5 == 0:
                        logger.info(f"Review count unchanged for {no_change_count} iterations (current: {current_reviews_count}, target: {reviews_count_total})")
                else:
                    no_change_count = 0
                    logger.info(f"✓ Reviews found: {current_reviews_count} (target: {reviews_count_total}, +{current_reviews_count - last_review_count})")
                
                last_review_count = current_reviews_count

                if current_reviews_count >= reviews_count_total and reviews_count_total > 0:
                    logger.info(f"All reviews loaded: {current_reviews_count} >= {reviews_count_total}")
                    break

            except Exception as e:
                logger.error(f"Error during scrolling for reviews: {e}", exc_info=True)
                break

        if scroll_iterations < min_scroll_iterations:
            logger.warning(f"Scroll iterations ({scroll_iterations}) less than minimum ({min_scroll_iterations}).")

        try:
            # Расширенные селекторы для поиска отзывов (приоритет более специфичным)
            review_selectors = [
                'div[class*="review-card"]',
                'div[class*="review-item"]',
                'div[class*="business-review"]',
                'li[class*="review"]',
                'div[class*="Review"]',
                'div[class*="review"]',
                'article[class*="review"]',
            ]
            
            review_cards = []
            seen_ids = set()
            for selector in review_selectors:
                found = soup_content.select(selector)
                if found:
                    for card in found:
                        card_id = id(card)
                        if card_id not in seen_ids:
                            seen_ids.add(card_id)
                            review_cards.append(card)
                    logger.info(f"Found {len(found)} review cards using selector: {selector}")
            
            logger.info(f"Total unique review cards found: {len(review_cards)}")
            
            # Дедупликация по тексту и автору
            seen_reviews = set()  # Для отслеживания уникальных отзывов
            
            # Если не нашли через селекторы, пробуем найти любые элементы с рейтингом
            if len(review_cards) == 0:
                logger.warning("No review cards found with standard selectors, trying alternative approach...")
                # Ищем элементы с рейтингом
                rating_elements = soup_content.select('[class*="rating"], [class*="star"], [data-rating]')
                logger.info(f"Found {len(rating_elements)} elements with rating classes")
                # Берем родительские элементы как карточки отзывов
                for rating_elem in rating_elements:
                    parent = rating_elem.parent
                    if parent and id(parent) not in seen_ids:
                        seen_ids.add(id(parent))
                        review_cards.append(parent)
            
            for card in review_cards:
                try:
                    # Расширенные селекторы для рейтинга (приоритет более специфичным)
                    rating_selectors = [
                        'span.business-rating-badge-view__rating-text',
                        '.review-item-view__rating-text',
                        'div[class*="rating-text"]',
                        'span[class*="rating-text"]',
                        'div[class*="rating-value"]',
                        'span[class*="rating-value"]',
                        '[data-rating]',
                        'span[class*="rating"]',
                        'div[class*="rating"]',
                        '[class*="star"][class*="rating"]',
                        'span[title*="звезд"]',
                        'span[title*="star"]',
                        '[aria-label*="звезд"]',
                        '[aria-label*="star"]',
                    ]
                    
                    rating_value = 0.0
                    
                    # Метод 1: Ищем рейтинг в data-атрибутах карточки (самый надежный)
                    card_data_rating = card.get('data-rating') or card.get('data-score') or card.get('data-value')
                    if card_data_rating:
                        try:
                            rating_value = float(card_data_rating)
                            if 1.0 <= rating_value <= 5.0:
                                logger.debug(f"Found rating {rating_value} from card data attribute")
                        except:
                            pass
                    
                    # Метод 2: Ищем рейтинг в классах карточки (например, rating-4, star-4)
                    if rating_value == 0.0:
                        card_classes = ' '.join(card.get('class', []))
                        # Ищем паттерны типа rating-4, star-4, rating_4, star_4
                        rating_class_match = re.search(r'(?:rating|star)[-_]?(\d)', card_classes, re.IGNORECASE)
                        if rating_class_match:
                            try:
                                rating_value = float(rating_class_match.group(1))
                                if 1.0 <= rating_value <= 5.0:
                                    logger.debug(f"Found rating {rating_value} from card class")
                            except:
                                pass
                    
                    # Метод 3: Ищем через селекторы рейтинга
                    if rating_value == 0.0:
                        for rating_selector in rating_selectors:
                            rating_elements = card.select(rating_selector)
                            for rating_element in rating_elements:
                                # Пробуем из data-атрибутов (самый надежный способ)
                                if rating_element.get('data-rating'):
                                    try:
                                        rating_value = float(rating_element.get('data-rating'))
                                        if 1.0 <= rating_value <= 5.0:
                                            logger.debug(f"Found rating {rating_value} from element data-rating")
                                            break
                                    except:
                                        pass
                                
                                # Пробуем из текста
                                rating_text = rating_element.get_text(strip=True)
                                if rating_text:
                                    # Ищем числа (рейтинг обычно 1-5)
                                    matches = re.findall(r'([1-5](?:[.,]\d+)?)', rating_text)
                                    if matches:
                                        try:
                                            rating_value = float(matches[0].replace(',', '.'))
                                            if 1.0 <= rating_value <= 5.0:
                                                logger.debug(f"Found rating {rating_value} from element text: {rating_text[:30]}")
                                                break
                                        except:
                                            pass
                                    
                                    # Ищем количество звезд в тексте (⭐⭐⭐⭐ = 4)
                                    star_count_in_text = rating_text.count('⭐')
                                    if star_count_in_text > 0 and star_count_in_text <= 5:
                                        rating_value = float(star_count_in_text)
                                        logger.debug(f"Found rating {rating_value} from star count in text: {rating_text[:30]}")
                                        break
                                
                                # Пробуем из title или aria-label
                                title = rating_element.get('title', '') or rating_element.get('aria-label', '')
                                if title:
                                    matches = re.findall(r'([1-5](?:[.,]\d+)?)', title)
                                    if matches:
                                        try:
                                            rating_value = float(matches[0].replace(',', '.'))
                                            if 1.0 <= rating_value <= 5.0:
                                                logger.debug(f"Found rating {rating_value} from title/aria-label: {title[:30]}")
                                                break
                                        except:
                                            pass
                                    
                                    # Ищем количество звезд в title/aria-label
                                    star_count_in_title = title.count('⭐')
                                    if star_count_in_title > 0 and star_count_in_title <= 5:
                                        rating_value = float(star_count_in_title)
                                        logger.debug(f"Found rating {rating_value} from star count in title: {title[:30]}")
                                        break
                                
                                # Пробуем найти по количеству звезд в классе
                                class_attr = ' '.join(rating_element.get('class', []))
                                if 'star' in class_attr.lower() or '⭐' in rating_text:
                                    # Ищем количество звезд в тексте (приоритет)
                                    star_count = rating_text.count('⭐') if rating_text else 0
                                    if star_count == 0:
                                        # Ищем в классе паттерны типа star-4, rating-4
                                        star_class_match = re.search(r'(?:star|rating)[-_]?(\d)', class_attr, re.IGNORECASE)
                                        if star_class_match:
                                            try:
                                                star_count = int(star_class_match.group(1))
                                            except:
                                                pass
                                    if star_count > 0 and star_count <= 5:
                                        rating_value = float(star_count)
                                        logger.debug(f"Found rating {rating_value} from star count/class: {class_attr[:50]}")
                                        break
                            
                            if rating_value > 0:
                                break
                    
                    # Метод 4: Ищем рейтинг во всем тексте карточки
                    if rating_value == 0.0:
                        card_full_text = card.get_text(separator=' ', strip=True)
                        # Ищем паттерны типа "4.0", "4,0", "4 звезд", "⭐⭐⭐⭐"
                        rating_matches = re.findall(r'([1-5](?:[.,]\d+)?)\s*(?:звезд|star|⭐)', card_full_text, re.IGNORECASE)
                        if rating_matches:
                            try:
                                rating_value = float(rating_matches[0].replace(',', '.'))
                                if 1.0 <= rating_value <= 5.0:
                                    logger.debug(f"Found rating {rating_value} from full card text")
                            except:
                                pass
                        
                        # Если не нашли число, ищем количество звезд
                        if rating_value == 0.0:
                            star_count_full = card_full_text.count('⭐')
                            if star_count_full > 0 and star_count_full <= 5:
                                rating_value = float(star_count_full)
                                logger.debug(f"Found rating {rating_value} from star count in full card text")
                    
                    # Расширенные селекторы для текста отзыва (приоритет более специфичным)
                    review_text_selectors = [
                        'div.business-review-view__body-text',
                        '.review-item-view__comment-text',
                        'div[class*="review-text"]',
                        'div[class*="comment-text"]',
                        'div[class*="body-text"]',
                        'p[class*="review-text"]',
                        'p[class*="comment"]',
                        'div[class*="text"][class*="review"]',
                        'div[class*="content"][class*="review"]',
                        'p[class*="review"]',
                        'div[class*="text"]',
                        'div[class*="content"]',
                    ]
                    
                    review_text = ""
                    for text_selector in review_text_selectors:
                        review_text_elements = card.select(text_selector)
                        for review_text_element in review_text_elements:
                            review_text = review_text_element.get_text(separator=' ', strip=True)
                            # Убираем лишние пробелы и переносы строк
                            review_text = ' '.join(review_text.split())
                            if review_text and len(review_text) > 10:  # Минимум 10 символов
                                break
                        if review_text and len(review_text) > 10:
                            break
                    
                    # Если не нашли через селекторы, пробуем найти любой текст в карточке
                    if not review_text or len(review_text) < 10:
                        # Ищем все текстовые элементы, исключая рейтинг и метаданные
                        all_text = card.get_text(separator=' ', strip=True)
                        # Убираем рейтинг и другие метаданные
                        cleaned_text = re.sub(r'\d+[.,]\d+\s*(звезд|star|⭐)', '', all_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'\d{1,2}\s*(янв|фев|мар|апр|май|июн|июл|авг|сен|окт|ноя|дек)\s*\d{4}?', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'\d{1,2}\s*(день|дня|дней|недел|недели|недель|месяц|месяца|месяцев|год|года|лет)\s*(назад)?', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'Оцените это место', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'Качество лечения.*?положительный', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = re.sub(r'Персонал.*?положительный', '', cleaned_text, flags=re.IGNORECASE)
                        cleaned_text = ' '.join(cleaned_text.split())  # Убираем лишние пробелы
                        cleaned_text = cleaned_text.strip()
                        if len(cleaned_text) > 20:  # Минимум 20 символов для текста отзыва
                            review_text = cleaned_text[:1000]  # Увеличиваем лимит до 1000 символов
                    
                    # Нормализуем текст для дедупликации
                    normalized_text = review_text.strip().lower() if review_text else ""
                    # Создаем ключ для дедупликации (текст + рейтинг)
                    review_key = f"{normalized_text[:100]}_{rating_value}"  # Первые 100 символов + рейтинг
                    
                    # Пропускаем дубликаты
                    if review_key in seen_reviews:
                        logger.debug(f"Skipping duplicate review: {normalized_text[:50]}... (rating: {rating_value})")
                        continue
                    
                    seen_reviews.add(review_key)
                    
                    # Если рейтинг не найден, но есть текст, пробуем найти рейтинг в тексте карточки еще раз
                    if rating_value == 0.0 and review_text:
                        # Ищем рейтинг в самом тексте отзыва или в родительских элементах
                        parent_card = card.parent if hasattr(card, 'parent') else card
                        rating_in_text = re.search(r'(\d)[.,]?\s*(?:звезд|star|⭐)', review_text, re.IGNORECASE)
                        if rating_in_text:
                            try:
                                rating_value = float(rating_in_text.group(1))
                                logger.debug(f"Found rating {rating_value} in review text")
                            except:
                                pass

                    # Ищем имя автора отзыва
                    author_name = ""
                    author_selectors = [
                        'div[class*="author"]',
                        'div[class*="reviewer"]',
                        'span[class*="author"]',
                        'span[class*="reviewer"]',
                        'a[class*="author"]',
                        'div[class*="user"]',
                        'span[class*="user"]',
                        'div[class*="name"]',
                    ]
                    
                    for author_selector in author_selectors:
                        author_elements = card.select(author_selector)
                        for author_element in author_elements:
                            author_text = author_element.get_text(strip=True)
                            # Пропускаем слишком короткие или общие тексты
                            if author_text and len(author_text) > 2 and len(author_text) < 100:
                                # Пропускаем тексты, которые похожи на метаданные
                                if not any(skip in author_text.lower() for skip in ['день', 'недел', 'месяц', 'год', 'назад', 'сегодня', 'вчера']):
                                    author_name = author_text
                                    break
                        if author_name:
                            break

                    # Ищем дату отзыва для этого отзыва
                    review_date = None
                    review_date_selectors = [
                        'time[datetime]',
                        '[datetime]',
                        'span[class*="date"]',
                        'div[class*="date"]',
                        'span[class*="time"]',
                        'div[class*="time"]',
                    ]
                    for date_selector in review_date_selectors:
                        date_elems = card.select(date_selector)
                        for date_elem in date_elems:
                            if date_elem.get('datetime'):
                                date_text = date_elem.get('datetime')
                                review_date = self._parse_date_string(date_text)
                                if review_date:
                                    break
                            date_text = date_elem.get_text(strip=True)
                            if date_text:
                                review_date = self._parse_date_string(date_text)
                                if review_date:
                                    break
                        if review_date:
                            break
                    
                    reviews_info['details'].append({
                        'review_rating': rating_value,
                        'review_text': review_text if review_text else "Без текста",
                        'review_author': author_name if author_name else "",
                        'review_date': review_date.strftime('%d.%m.%Y') if review_date else ""
                    })

                    logger.debug(f"Added review: rating={rating_value}, text_length={len(review_text)}, text_preview={review_text[:50]}...")

                    # Позитивные отзывы: 4-5 звезд (>= 4.0)
                    if rating_value >= 4.0 and rating_value <= 5.0:
                        reviews_info['positive_reviews'] += 1
                        logger.debug(f"Positive review (rating={rating_value}): {review_text[:50]}...")
                    # Негативные отзывы: 1-3 звезды (> 0 и < 4.0)
                    elif rating_value > 0.0 and rating_value < 4.0:
                        reviews_info['negative_reviews'] += 1
                        logger.debug(f"Negative review (rating={rating_value}): {review_text[:50]}...")
                    # Если рейтинг = 0, не считаем ни положительным, ни отрицательным
                    elif rating_value == 0.0:
                        logger.warning(f"Review with rating 0.0 found (could not extract rating): {review_text[:50]}...")
                    # Если рейтинг вне диапазона, логируем предупреждение
                    else:
                        logger.warning(f"Review with invalid rating {rating_value} found: {review_text[:50]}...")

                except Exception as e:
                    logger.warning(f"Error processing individual review card: {e}", exc_info=True)

            # Общее количество отзывов = количество уникальных отзывов (после дедупликации)
            reviews_info['reviews_count'] = len(reviews_info['details'])
            
            # Если количество отзывов не совпадает с найденными карточками, используем найденное количество
            if reviews_info['reviews_count'] == 0 and reviews_count_total > 0:
                logger.warning(f"Found {reviews_count_total} reviews count but 0 review cards. Using count from page.")
                reviews_info['reviews_count'] = reviews_count_total
            
            # Логируем подробную статистику
            total_with_rating = sum(1 for d in reviews_info['details'] if d.get('review_rating', 0) > 0)
            total_without_rating = len(reviews_info['details']) - total_with_rating
            logger.info(f"Reviews summary: total={reviews_info['reviews_count']}, positive={reviews_info['positive_reviews']}, negative={reviews_info['negative_reviews']}, with_rating={total_with_rating}, without_rating={total_without_rating}")
        except Exception as e:
            logger.error(f"Error processing review cards: {e}")

        return reviews_info

    def _calculate_avg_response_time_from_reviews(self, card_details_soup: BeautifulSoup, reviews_data: Dict[str, Any]) -> Tuple[float, int]:
        """Вычисляет среднее время ответа из дат отзывов и ответов
        
        Returns:
            tuple: (average_days, successfully_calculated_count)
        """
        try:
            import locale
            
            # Пробуем установить русскую локаль для парсинга дат
            try:
                locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
            except:
                try:
                    locale.setlocale(locale.LC_TIME, 'Russian_Russia.1251')
                except:
                    pass  # Используем дефолтную локаль
            
            response_times = []
            
            # Ищем все отзывы с ответами на странице
            review_selectors = [
                'div[class*="review-card"]',
                'div[class*="review-item"]',
                'div[class*="business-review"]',
                'li[class*="review"]',
                'div[class*="Review"]',
                'article[class*="review"]',
            ]
            
            for selector in review_selectors:
                review_elements = card_details_soup.select(selector)
                if review_elements:
                    logger.debug(f"Found {len(review_elements)} review elements with selector: {selector}")
                    for review_elem in review_elements:
                        # Ищем блок ответа компании (расширенные селекторы)
                        response_block = None
                        response_selectors = [
                            'div[class*="response"]',
                            'div[class*="answer"]',
                            'div[class*="company-answer"]',
                            'div[class*="business-response"]',
                            'div[class*="review-response"]',
                            'div[class*="owner-response"]',
                        ]
                        for resp_selector in response_selectors:
                            response_block = review_elem.select_one(resp_selector)
                            if response_block:
                                break
                        
                        if response_block:
                            logger.debug(f"Found response block, searching for dates...")
                            # Ищем дату отзыва (расширенные селекторы, приоритет более специфичным)
                            review_date_selectors = [
                                'time[datetime]',
                                '[datetime]',
                                'span[class*="date"][class*="review"]',
                                'div[class*="date"][class*="review"]',
                                'span.business-review-view__date',
                                '.review-item-view__date',
                                'span[class*="date"]',
                                'div[class*="date"]',
                                'span[class*="time"]',
                                'div[class*="time"]',
                            ]
                            
                            review_date = None
                            for date_selector in review_date_selectors:
                                date_elems = review_elem.select(date_selector)
                                for date_elem in date_elems:
                                    # Сначала пробуем datetime атрибут
                                    if date_elem.get('datetime'):
                                        date_text = date_elem.get('datetime')
                                        review_date = self._parse_date_string(date_text)
                                        if review_date:
                                            break
                                    
                                    # Потом пробуем текст
                                    date_text = date_elem.get_text(strip=True)
                                    if date_text:
                                        review_date = self._parse_date_string(date_text)
                                        if review_date:
                                            break
                                if review_date:
                                    break
                            
                            # Ищем дату ответа (расширенные селекторы, приоритет более специфичным)
                            response_date_selectors = [
                                'time[datetime]',
                                '[datetime]',
                                'span[class*="date"][class*="response"]',
                                'div[class*="date"][class*="response"]',
                                'span[class*="date"]',
                                'div[class*="date"]',
                                'span[class*="time"]',
                                'div[class*="time"]',
                            ]
                            
                            response_date = None
                            for date_selector in response_date_selectors:
                                date_elems = response_block.select(date_selector)
                                for date_elem in date_elems:
                                    # Сначала пробуем datetime атрибут
                                    if date_elem.get('datetime'):
                                        date_text = date_elem.get('datetime')
                                        response_date = self._parse_date_string(date_text)
                                        if response_date:
                                            break
                                    
                                    # Потом пробуем текст
                                    date_text = date_elem.get_text(strip=True)
                                    if date_text:
                                        response_date = self._parse_date_string(date_text)
                                        if response_date:
                                            break
                                if response_date:
                                    break
                            
                            # Если нашли обе даты, вычисляем разницу
                            if review_date and response_date:
                                time_diff = response_date - review_date
                                if time_diff.total_seconds() > 0:  # Ответ должен быть после отзыва
                                    days = time_diff.days + (time_diff.seconds / 86400)  # Точность до дня с дробной частью
                                    response_times.append(days)
                                    logger.debug(f"✓ Found response time: {days:.2f} days (review: {review_date.date()}, response: {response_date.date()})")
                                else:
                                    logger.debug(f"⚠ Response date ({response_date.date()}) is before review date ({review_date.date()}), skipping")
                            else:
                                if not review_date:
                                    logger.debug(f"⚠ Could not find review date in review element")
                                if not response_date:
                                    logger.debug(f"⚠ Could not find response date in response block")
                    
                    if response_times:
                        break  # Нашли отзывы, не нужно искать дальше
            
            # Вычисляем среднее
            if response_times:
                avg_days = sum(response_times) / len(response_times)
                min_days = min(response_times)
                max_days = max(response_times)
                logger.info(f"✓ Calculated average response time from {len(response_times)} reviews: {avg_days:.2f} days (min: {min_days:.2f}, max: {max_days:.2f})")
                return (avg_days, len(response_times))
            else:
                logger.warning(f"⚠ No response times found. Checked {len(review_elements) if 'review_elements' in locals() else 0} review elements")
            
            return (0.0, 0)
        except Exception as e:
            logger.error(f"✗ Error calculating avg response time from reviews: {e}", exc_info=True)
            return (0.0, 0)
    
    def _parse_date_string(self, date_string: str) -> Optional[datetime]:
        """Парсит строку с датой в различных форматах"""
        try:
            import locale
            
            date_string = date_string.strip()
            
            try:
                locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
            except:
                try:
                    locale.setlocale(locale.LC_TIME, 'Russian_Russia.1251')
                except:
                    pass
            
            date_formats = [
                '%Y-%m-%d',
                '%d.%m.%Y',
                '%d/%m/%Y',
                '%d %B %Y',
                '%d %b %Y',
                '%d-%m-%Y',
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_string, fmt)
                except:
                    continue
            
            date_formats_no_year = [
                '%d %B',
                '%d %b',
                '%d.%m',
                '%d/%m',
            ]
            
            for fmt in date_formats_no_year:
                try:
                    parsed_date = datetime.strptime(date_string, fmt)
                    return parsed_date.replace(year=datetime.now().year)
                except:
                    continue
            
            relative_patterns = [
                (r'(\d+)\s*(час|часа|часов)\s*(назад|ago)', 1/24),
                (r'(\d+)\s*(день|дня|дней)\s*(назад|ago)', 1),
                (r'(\d+)\s*(недел|недели|недель)\s*(назад|ago)', 7),
                (r'(\d+)\s*(месяц|месяца|месяцев)\s*(назад|ago)', 30),
                (r'(\d+)\s*(год|года|лет)\s*(назад|ago)', 365),
                (r'вчера|yesterday', 1),
                (r'сегодня|today', 0),
            ]
            
            for pattern, multiplier in relative_patterns:
                match = re.search(pattern, date_string, re.IGNORECASE)
                if match:
                    if 'вчера' in pattern.lower() or 'yesterday' in pattern.lower():
                        days_ago = 1
                    elif 'сегодня' in pattern.lower() or 'today' in pattern.lower():
                        days_ago = 0
                    else:
                        days_ago = float(match.group(1)) * multiplier
                    return datetime.now() - timedelta(days=days_ago)
            
            logger.debug(f"Could not parse date string: '{date_string}'")
            return None
        except Exception as e:
            logger.debug(f"Error parsing date string '{date_string}': {e}")
            return None

    def _scroll_to_load_all_cards(self, max_scrolls: Optional[int] = None, scroll_step: Optional[int] = None) -> int:
        """Прокручивает контейнер результатов для загрузки всех карточек используя JavaScript."""
        logger.info("=" * 60)
        logger.info("Starting _scroll_to_load_all_cards method")
        logger.info("=" * 60)
        
        previous_card_count = 0
        no_change_count = 0
        scroll_iterations = 0
        last_height = 0
        max_card_count = 0
        
        # Используем значения из конфига, если не указаны
        if max_scrolls is None:
            max_scrolls = self._scroll_max_iter
        if scroll_step is None:
            scroll_step = self._scroll_step
        
        logger.info(f"Scroll parameters: Max iterations={max_scrolls}, Scroll step={scroll_step}px, Wait time={self._scroll_wait_time}s")
        
        # Проверяем, работает ли JavaScript вообще
        try:
            test_result = self.driver.execute_script("return document.body.scrollHeight")
            logger.info(f"✓ JavaScript execution test successful. Document height: {test_result}px")
        except Exception as js_test_error:
            logger.error(f"✗ JavaScript execution test FAILED: {js_test_error}", exc_info=True)
            logger.error("Cannot proceed with scrolling - JavaScript is not working!")
            return 0
        
        # Находим прокручиваемый элемент - это критически важно!
        # Анализ показал, что правильный селектор: .scroll__container
        scrollable_element_selector = None
        try:
            # Сначала пробуем селекторы из конфига (приоритет: .scroll__container)
            # Используем JSON для безопасной передачи селектора в JavaScript
            selector_json = json.dumps(self._scrollable_element_selector)
            find_scrollable_script = f"""
            // Пробуем селекторы из конфига по порядку
            var selectorStr = {selector_json};
            var selectors = selectorStr.split(',').map(s => s.trim());
            for (var i = 0; i < selectors.length; i++) {{
                var els = document.querySelectorAll(selectors[i]);
                for (var j = 0; j < els.length; j++) {{
                    var el = els[j];
                    if (el && el.scrollHeight > el.clientHeight && el.scrollHeight > 500) {{
                        return {{
                            'selector': selectors[i],
                            'scrollHeight': el.scrollHeight,
                            'clientHeight': el.clientHeight,
                            'scrollTop': el.scrollTop,
                            'className': el.className || '',
                            'tagName': el.tagName
                        }};
                    }}
                }}
            }}
            
            // Если не нашли через селекторы, пробуем через scrollbar
            var scrollbar = document.querySelector('.scroll__scrollbar');
            if (scrollbar) {{
                var parent = scrollbar.parentElement;
                while (parent && parent !== document.body) {{
                    if (parent.scrollHeight > parent.clientHeight && parent.scrollHeight > 500) {{
                        // Пробуем создать селектор из класса
                        var classes = parent.className;
                        if (classes && typeof classes === 'string') {{
                            var classList = classes.split(' ').filter(c => c && c.length > 0);
                            if (classList.length > 0) {{
                                return {{
                                    'selector': '.' + classList[0],
                                    'scrollHeight': parent.scrollHeight,
                                    'clientHeight': parent.clientHeight,
                                    'scrollTop': parent.scrollTop,
                                    'className': parent.className || '',
                                    'tagName': parent.tagName
                                }};
                            }}
                        }}
                        // Если нет класса, используем тег (но это менее надежно)
                        return {{
                            'selector': parent.tagName.toLowerCase(),
                            'scrollHeight': parent.scrollHeight,
                            'clientHeight': parent.clientHeight,
                            'scrollTop': parent.scrollTop,
                            'className': parent.className || '',
                            'tagName': parent.tagName
                        }};
                    }}
                    parent = parent.parentElement;
                }}
            }}
            
            return null;
            """
            scrollable_info = self.driver.execute_script(find_scrollable_script)
            
            if scrollable_info and isinstance(scrollable_info, dict):
                scrollable_element_selector = scrollable_info.get('selector')
                
                logger.info(f"✓ Found scrollable element: selector='{scrollable_element_selector}', "
                          f"scrollHeight={scrollable_info.get('scrollHeight')}px, "
                          f"clientHeight={scrollable_info.get('clientHeight')}px, "
                          f"tag={scrollable_info.get('tagName')}, "
                          f"classes={scrollable_info.get('className')[:50]}")
            else:
                logger.warning(f"Could not find scrollable element with selectors '{self._scrollable_element_selector}', will try window scroll")
                scrollable_element_selector = None
        except Exception as e:
            logger.error(f"Error finding scrollable element: {e}", exc_info=True)
            scrollable_element_selector = None
        
        while scroll_iterations < max_scrolls:
            try:
                # Получаем текущую высоту ДО прокрутки
                logger.debug(f"Scroll iteration {scroll_iterations + 1}/{max_scrolls}: Getting current height...")
                
                if scrollable_element_selector:
                    # Экранируем селектор для JavaScript
                    escaped_selector = scrollable_element_selector.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"').replace('\n', ' ').replace('\r', ' ')
                    # Получаем информацию о прокручиваемом элементе
                    height_script = f"""
                    var selector = '{escaped_selector}';
                    var container = document.querySelector(selector);
                    if (container) {{
                        return {{
                            'scrollHeight': container.scrollHeight,
                            'clientHeight': container.clientHeight,
                            'scrollTop': container.scrollTop
                        }};
                    }}
                    return null;
                    """
                    try:
                        height_info = self.driver.execute_script(height_script)
                        if height_info and isinstance(height_info, dict):
                            current_height = height_info.get('scrollHeight', 0)
                            current_scroll_top = height_info.get('scrollTop', 0)
                            logger.debug(f"Scrollable element height: {current_height}px, scrollTop: {current_scroll_top}px")
                        else:
                            current_height = 0
                            logger.warning(f"Could not get scrollable element height info. Result: {height_info}")
                    except Exception as height_error:
                        logger.error(f"Error getting scrollable element height: {height_error}", exc_info=True)
                        current_height = 0
                else:
                    # Используем высоту документа
                    try:
                        current_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, document.body.offsetHeight, document.documentElement.offsetHeight);")
                        current_scroll_top = self.driver.execute_script("return window.pageYOffset || document.documentElement.scrollTop || 0;")
                        logger.debug(f"Document height: {current_height}px, scrollTop: {current_scroll_top}px")
                    except Exception as height_error:
                        logger.error(f"Error getting document height: {height_error}", exc_info=True)
                        current_height = 0
                
                # Логируем только каждую 5-ю итерацию или при значительных изменениях
                if scroll_iterations % 5 == 0 or scroll_iterations == 0:
                    logger.info(f"Scroll iteration {scroll_iterations + 1}/{max_scrolls}: Current height = {current_height}px, Previous height = {last_height}px")
                
                # Прокручиваем используя JavaScript - прокручиваем конкретный элемент
                try:
                    if scrollable_element_selector:
                        # Прокручиваем конкретный прокручиваемый элемент через element.scrollTop
                        # ВАЖНО: Используем пошаговую прокрутку для постепенной загрузки контента
                        # Экранируем селектор для JavaScript используя json.dumps
                        escaped_selector_json = json.dumps(scrollable_element_selector)
                        scroll_script = f"""
                        var selector = {escaped_selector_json};
                        var container = document.querySelector(selector);
                        if (container) {{
                            var oldScrollTop = container.scrollTop;
                            var oldScrollHeight = container.scrollHeight;
                            
                            // ВАЖНО: Пошаговая прокрутка вместо прокрутки до самого низа
                            // Это позволяет контенту загружаться постепенно
                            var maxScrollTop = container.scrollHeight - container.clientHeight;
                            var newScrollTop = Math.min(
                                oldScrollTop + {scroll_step},
                                maxScrollTop
                            );
                            container.scrollTop = newScrollTop;
                            
                            return {{
                                'scrollHeight': container.scrollHeight,
                                'scrollTop': container.scrollTop,
                                'clientHeight': container.clientHeight,
                                'oldScrollTop': oldScrollTop,
                                'oldScrollHeight': oldScrollHeight,
                                'changed': container.scrollTop !== oldScrollTop || container.scrollHeight !== oldScrollHeight,
                                'isAtBottom': container.scrollTop >= (maxScrollTop - 10)
                            }};
                        }}
                        return {{'error': 'Container not found'}};
                        """
                        scroll_result = self.driver.execute_script(scroll_script)
                        if scroll_result and isinstance(scroll_result, dict):
                            if scroll_result.get('error'):
                                logger.error(f"✗ Scrollable element scroll error: {scroll_result.get('error')}")
                            elif scroll_result.get('changed'):
                                # Логируем только при значительных изменениях или каждую 5-ю итерацию
                                if scroll_iterations % 5 == 0 or abs(scroll_result.get('scrollTop', 0) - scroll_result.get('oldScrollTop', 0)) > 500:
                                    logger.info(f"✓ Scrolled: scrollTop {scroll_result.get('oldScrollTop')} -> {scroll_result.get('scrollTop')}, height {scroll_result.get('oldScrollHeight')} -> {scroll_result.get('scrollHeight')}")
                            # Убираем warning для обычных случаев без изменений
                    else:
                        # Прокручиваем window - используем несколько методов для надежности
                        old_scroll_top = self.driver.execute_script("return window.pageYOffset || document.documentElement.scrollTop || 0;")
                        old_scroll_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);")
                        
                        # Проверяем, что получили валидные значения
                        if old_scroll_top is None:
                            old_scroll_top = 0
                        if old_scroll_height is None:
                            old_scroll_height = 0
                        
                        # Метод 1: scrollBy (пошаговая прокрутка)
                        self.driver.execute_script(f"window.scrollBy({{top: {scroll_step}, left: 0, behavior: 'auto'}});")
                        time.sleep(0.1)
                        
                        # Метод 2: scrollTo до низа (более агрессивный)
                        max_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, document.body.offsetHeight, document.documentElement.offsetHeight);")
                        if max_height is None:
                            max_height = old_scroll_height
                        self.driver.execute_script(f"window.scrollTo({{top: {max_height}, left: 0, behavior: 'auto'}});")
                        time.sleep(0.1)
                        
                        # Метод 3: через document.documentElement
                        self.driver.execute_script("document.documentElement.scrollTop = document.documentElement.scrollHeight;")
                        time.sleep(0.1)
                        
                        # Метод 4: через document.body
                        self.driver.execute_script("document.body.scrollTop = document.body.scrollHeight;")
                        time.sleep(0.1)
                        
                        # Метод 5: через window.scroll (альтернативный способ)
                        self.driver.execute_script("window.scroll(0, document.body.scrollHeight);")
                        
                        # Проверяем результат
                        new_scroll_top = self.driver.execute_script("return window.pageYOffset || document.documentElement.scrollTop || 0;")
                        new_scroll_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, document.body.offsetHeight, document.documentElement.offsetHeight);")
                        
                        # Проверяем, что получили валидные значения
                        if new_scroll_top is None:
                            new_scroll_top = old_scroll_top
                        if new_scroll_height is None:
                            new_scroll_height = old_scroll_height
                        
                        scroll_diff = new_scroll_top - old_scroll_top
                        height_diff = new_scroll_height - old_scroll_height
                        
                        if scroll_diff > 0 or height_diff > 0:
                            logger.info(f"✓ Window scrolled: scrollTop {old_scroll_top} -> {new_scroll_top} (+{scroll_diff}px), height {old_scroll_height} -> {new_scroll_height} (+{height_diff}px)")
                        else:
                            logger.warning(f"⚠ Window scroll may not have worked: scrollTop {old_scroll_top} -> {new_scroll_top} (no change), height {old_scroll_height} -> {new_scroll_height} (no change)")
                            
                            # Пробуем дополнительно через ActionChains (если доступен реальный WebDriver)
                            try:
                                from selenium.webdriver.common.action_chains import ActionChains
                                from selenium.webdriver.common.by import By
                                
                                # Получаем реальный WebDriver из SeleniumDriver
                                if hasattr(self.driver, 'driver') and self.driver.driver:
                                    webdriver_instance = self.driver.driver
                                    body_element = webdriver_instance.find_element(By.TAG_NAME, "body")
                                    actions = ActionChains(webdriver_instance)
                                    # Прокручиваем к элементу
                                    actions.scroll_to_element(body_element).perform()
                                    # Также пробуем прокрутку через Keys
                                    from selenium.webdriver.common.keys import Keys
                                    body_element.send_keys(Keys.PAGE_DOWN)
                                    body_element.send_keys(Keys.PAGE_DOWN)
                                    logger.info("✓ Tried ActionChains scroll as fallback")
                            except Exception as ac_error:
                                logger.debug(f"ActionChains scroll failed: {ac_error}")
                        
                        scroll_result = None
                except Exception as scroll_error:
                    logger.error(f"✗ Error executing scroll script: {scroll_error}", exc_info=True)
                    # Пробуем альтернативный метод прокрутки
                    try:
                        logger.info("Trying alternative scroll method...")
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(0.5)
                        logger.info("Alternative scroll method executed.")
                    except Exception as alt_error:
                        logger.error(f"Alternative scroll method also failed: {alt_error}")
                        break
                
                # Ждем загрузки нового контента (время из конфига)
                # ВАЖНО: Ждем достаточно долго, чтобы контент успел загрузиться
                time.sleep(self._scroll_wait_time)
                
                # Дополнительно: ждем, пока высота не стабилизируется
                if scrollable_element_selector:
                    stability_wait = 0
                    max_stability_wait = 5  # Максимум 5 попыток по 0.5 секунды
                    last_check_height = current_height
                    while stability_wait < max_stability_wait:
                        time.sleep(0.5)
                        # Экранируем селектор для JavaScript
                        escaped_selector = scrollable_element_selector.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"').replace('\n', ' ').replace('\r', ' ')
                        check_script = f"""
                        var selector = '{escaped_selector}';
                        var container = document.querySelector(selector);
                        return container ? container.scrollHeight : null;
                        """
                        try:
                            check_height = self.driver.execute_script(check_script)
                            if check_height:
                                if abs(check_height - last_check_height) < 10:
                                    # Высота стабилизировалась
                                    break
                                last_check_height = check_height
                        except:
                            pass
                        stability_wait += 1
                
                # Получаем новую высоту ПОСЛЕ прокрутки и ожидания
                try:
                    if scrollable_element_selector:
                        # Получаем новую высоту прокручиваемого элемента
                        # Экранируем селектор для JavaScript
                        escaped_selector = scrollable_element_selector.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"').replace('\n', ' ').replace('\r', ' ')
                        height_script = f"""
                        var selector = '{escaped_selector}';
                        var container = document.querySelector(selector);
                        if (container) {{
                            return {{
                                'scrollHeight': container.scrollHeight,
                                'clientHeight': container.clientHeight,
                                'scrollTop': container.scrollTop
                            }};
                        }}
                        return null;
                        """
                        new_height_info = self.driver.execute_script(height_script)
                        if new_height_info and isinstance(new_height_info, dict):
                            new_height = new_height_info.get('scrollHeight', 0) or 0
                            new_scroll_top = new_height_info.get('scrollTop', 0) or 0
                        else:
                            new_height = current_height  # Используем текущую высоту
                            new_scroll_top = 0
                            if scroll_iterations % 10 == 0:  # Логируем только периодически
                                logger.warning(f"Could not get new scrollable element height info. Result: {new_height_info}")
                    else:
                        # Используем высоту документа
                        new_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight, document.body.offsetHeight, document.documentElement.offsetHeight);")
                        new_scroll_top = self.driver.execute_script("return window.pageYOffset || document.documentElement.scrollTop || 0;")
                        # Проверяем на None
                        if new_height is None:
                            new_height = current_height
                        if new_scroll_top is None:
                            new_scroll_top = 0
                except Exception as height_error:
                    logger.error(f"Error getting new height: {height_error}", exc_info=True)
                    new_height = current_height  # Используем старую высоту в случае ошибки
                
                height_change = new_height - current_height
                # Логируем только при значительных изменениях или каждую 5-ю итерацию
                if scroll_iterations % 5 == 0 or abs(height_change) > 200:
                    logger.info(f"After scroll: New height = {new_height}px, Height changed = {height_change}px")
                
                # Получаем количество карточек ПОСЛЕ прокрутки
                page_source, soup = self._get_page_source_and_soup()
                new_cards = []
                seen_ids = set()
                for selector in self._card_selectors:
                    found = soup.select(selector)
                    for card in found:
                        card_id = id(card)
                        if card_id not in seen_ids:
                            seen_ids.add(card_id)
                            new_cards.append(card)
                new_card_count = len(new_cards)
                
                # Обновляем максимальное количество найденных карточек
                if new_card_count > max_card_count:
                    max_card_count = new_card_count
                    # Логируем только когда находим больше карточек
                    logger.info(f"✓ Cards found: {new_card_count} (max so far: {max_card_count})")
                elif scroll_iterations % 10 == 0:  # Или каждую 10-ю итерацию
                    logger.info(f"Cards found: {new_card_count} (max so far: {max_card_count})")
                
                # Проверяем, изменилась ли высота страницы
                height_unchanged = abs(new_height - last_height) < 10
                cards_unchanged = new_card_count == previous_card_count
                
                # ВАЖНО: Не останавливаемся, если нашли меньше порога карточек (настраивается через yandex_min_cards_threshold в config.json)
                if height_unchanged and cards_unchanged:
                    # Высота не изменилась и количество карточек тоже
                    no_change_count += 1
                    # Если нашли мало карточек, продолжаем прокручивать дольше
                    min_cards_threshold = self._min_cards_threshold  # Используем настраиваемый порог из config.json
                    required_no_change = 20 if max_card_count < min_cards_threshold else 15  # Увеличиваем количество итераций без изменений
                    
                    if scroll_iterations % 5 == 0 or no_change_count % 5 == 0:  # Логируем периодически
                        logger.info(f"No change detected ({no_change_count}/{required_no_change}): height={new_height}px, cards={new_card_count}")
                    
                    if no_change_count >= required_no_change:
                        # Дополнительная проверка: пробуем прокрутить до самого низа еще раз
                        if scrollable_element_selector and max_card_count < min_cards_threshold:
                            logger.info(f"Found only {max_card_count} cards (< {min_cards_threshold}), trying multiple scroll attempts...")
                            try:
                                # Делаем несколько попыток прокрутки
                                for scroll_attempt in range(5):
                                    # Прокручиваем до самого низа
                                    # Экранируем селектор для JavaScript
                                    escaped_selector = scrollable_element_selector.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"').replace('\n', ' ').replace('\r', ' ')
                                    final_scroll_script = f"""
                                    var selector = '{escaped_selector}';
                                    var container = document.querySelector(selector);
                                    if (container) {{
                                        container.scrollTop = container.scrollHeight;
                                        return container.scrollTop;
                                    }}
                                    // Fallback: прокручиваем window
                                    window.scrollTo(0, document.body.scrollHeight);
                                    return window.pageYOffset || document.documentElement.scrollTop || 0;
                                    """
                                    self.driver.execute_script(final_scroll_script)
                                    time.sleep(2)  # Ждем загрузки
                                    
                                    # Пересчитываем карточки
                                    page_source, soup = self._get_page_source_and_soup()
                                    final_cards = []
                                    seen_ids = set()
                                    for selector in self._card_selectors:
                                        found = soup.select(selector)
                                        for card in found:
                                            card_id = id(card)
                                            if card_id not in seen_ids:
                                                seen_ids.add(card_id)
                                                final_cards.append(card)
                                    final_count = len(final_cards)
                                    
                                    if final_count > max_card_count:
                                        logger.info(f"✓ Found more cards after scroll attempt {scroll_attempt + 1}: {final_count} (was {max_card_count})")
                                        max_card_count = final_count
                                        no_change_count = 0  # Сбрасываем счетчик, продолжаем
                                        break
                                    elif scroll_attempt < 4:
                                        logger.debug(f"Scroll attempt {scroll_attempt + 1}: still {final_count} cards, trying again...")
                                
                                # Если после всех попыток нашли больше карточек, продолжаем основной цикл
                                if max_card_count > previous_card_count:
                                    continue
                            except Exception as e:
                                logger.warning(f"Final scroll attempt failed: {e}")
                        
                        logger.info("=" * 60)
                        logger.info(f"Scroll stopped: Height and card count unchanged after {no_change_count} scrolls.")
                        logger.info(f"Final results: {max_card_count} cards found, height = {new_height}px")
                        logger.info("=" * 60)
                        break
                else:
                    # Высота или количество карточек изменились - продолжаем
                    no_change_count = 0
                    if new_card_count > previous_card_count:
                        logger.info(f"✓ New cards loaded: {previous_card_count} -> {new_card_count} (+{new_card_count - previous_card_count})")
                    if new_height > last_height:
                        logger.info(f"✓ Height increased from {last_height}px to {new_height}px (+{new_height - last_height}px), continuing scroll...")
                
                previous_card_count = new_card_count
                last_height = new_height
                scroll_iterations += 1
                
            except Exception as e:
                logger.error(f"✗ Error during scrolling iteration {scroll_iterations + 1}: {e}", exc_info=True)
                # Не прерываем сразу, пробуем продолжить
                scroll_iterations += 1
                if scroll_iterations >= max_scrolls:
                    break
        
        logger.info("=" * 60)
        logger.info(f"Finished scrolling. Total scrolls: {scroll_iterations}/{max_scrolls}")
        logger.info(f"Final card count: {max_card_count if max_card_count > previous_card_count else previous_card_count}")
        logger.info("=" * 60)
        return max_card_count if max_card_count > previous_card_count else previous_card_count

    def _parse_cards(self, search_query_url: str) -> List[Dict[str, Any]]:
        # Инициализируем данные перед началом парсинга
        self._collected_card_data = []
        self._current_page_number = 1
        self._aggregated_data = {
            'total_cards': 0,
            'total_rating_sum': 0.0,
            'total_reviews_count': 0,
            'total_positive_reviews': 0,
            'total_negative_reviews': 0,
            'total_answered_count': 0,
            'total_answered_reviews_count': 0,
            'total_unanswered_reviews_count': 0,
            'total_response_time_sum_days': 0.0,
            'total_response_time_calculated_count': 0,
        }
        
        logger.info(f"=== Starting _parse_cards ===")
        logger.info(f"Max records: {self._max_records}, Current cards: {len(self._collected_card_data)}")
        logger.info(f"Navigating to search results page: {search_query_url}")
        
        try:
            self.driver.navigate(search_query_url)
            self.check_captcha()
        except Exception as e:
            logger.error(f"❌ Error navigating to search page: {e}", exc_info=True)
            return []
        
        # Ждем загрузки страницы
        time.sleep(3)

        processed_urls = set()

        # Основной цикл обработки страниц
        logger.info(f"Entering main while loop. Condition: {len(self._collected_card_data)} < {self._max_records}")
        while len(self._collected_card_data) < self._max_records:
            logger.info(f"Processing Yandex Maps page {self._current_page_number} (current cards collected: {len(self._collected_card_data)})")
            self.check_captcha()

            # ВАЖНО: Проверяем, что драйвер активен перед обработкой
            logger.info(f"Checking driver status for page {self._current_page_number}...")
            try:
                if hasattr(self.driver, 'driver') and self.driver.driver:
                    try:
                        current_url = self.driver.driver.current_url
                        logger.info(f"✓ Driver is active. Current URL: {current_url}")
                    except Exception as session_error:
                        logger.error(f"❌ Browser session lost: {session_error}")
                        break
                else:
                    logger.error("❌ Driver is not initialized")
                    break
            except Exception as e:
                logger.error(f"❌ Error checking driver: {e}", exc_info=True)
                break

            logger.info(f"✓ Driver check passed. Entering main processing block for page {self._current_page_number}")
            try:
                # Сохраняем URL текущей страницы поиска перед парсингом карточек
                current_search_page_url = self.driver.driver.current_url if hasattr(self.driver, 'driver') and self.driver.driver else search_query_url
                logger.info(f"Current search page URL: {current_search_page_url}")
                
                # ШАГ 1: Прокручиваем страницу до тех пор, пока появляются новые карточки
                logger.info(f"Step 1: Scrolling page {self._current_page_number} to load all cards...")
                initial_page_source, initial_soup = self._get_page_source_and_soup()
                initial_cards = []
                seen_ids = set()
                for selector in self._card_selectors:
                    found = initial_soup.select(selector)
                    for card in found:
                        card_id = id(card)
                        if card_id not in seen_ids:
                            seen_ids.add(card_id)
                            initial_cards.append(card)
                initial_count = len(initial_cards)
                logger.info(f"Initial cards found before scrolling: {initial_count}")
                
                # Прокручиваем до тех пор, пока появляются новые карточки
                final_card_count = self._scroll_to_load_all_cards()
                logger.info(f"Scroll completed. Found {final_card_count} cards (was {initial_count}).")
                time.sleep(3)  # Дополнительное ожидание после прокрутки
                
                # ШАГ 2: Собираем все ссылки на карточки после прокрутки
                logger.info(f"Step 2: Collecting all card URLs from page {self._current_page_number}...")
                page_source, soup = self._get_page_source_and_soup()
                cards_on_page = []
                seen_ids = set()
                for selector in self._card_selectors:
                    found = soup.select(selector)
                    for card in found:
                        card_id = id(card)
                        if card_id not in seen_ids:
                            seen_ids.add(card_id)
                            cards_on_page.append(card)
                
                logger.info(f"Found {len(cards_on_page)} cards on page {self._current_page_number} after scrolling")
                
                # Если не нашли через селекторы, пробуем найти через ссылки на организации
                if not cards_on_page:
                    logger.warning("No cards found with standard selectors. Trying alternative method...")
                    org_links = soup.select('a[href*="/maps/org/"]:not([href*="/gallery/"])')
                    logger.info(f"Found {len(org_links)} organization links on page")
                    if org_links:
                        logger.info(f"Processing {len(org_links)} organization links...")
                        # Создаем виртуальные карточки из ссылок
                        for link in org_links:
                            if len(self._collected_card_data) >= self._max_records:
                                break
                            card_url = link.get('href')
                            if card_url and card_url not in processed_urls:
                                if not card_url.startswith('http'):
                                    card_url = urllib.parse.urljoin("https://yandex.ru", card_url)
                                processed_urls.add(card_url)

                                logger.info(f"Navigating to card detail page: {card_url}")
                                self.driver.navigate(card_url)
                                self.check_captcha()
                                time.sleep(2)
                                
                                card_details_soup = BeautifulSoup(self.driver.get_page_source(), "lxml")
                                card_snippet = self._extract_card_data_from_detail_page(card_details_soup)
                                
                                if card_snippet and card_snippet.get('card_name'):
                                    self._collected_card_data.append(card_snippet)
                                    self._update_aggregated_data(card_snippet)
                                    
                                    # ВАЖНО: Возвращаемся на страницу поиска после обработки карточки
                                    logger.info(f"Returning to search page after processing card (alternative method)...")
                                    self.driver.navigate(search_query_url)
                                    self.check_captcha()
                                    time.sleep(2)
                                    # Прокручиваем снова, чтобы увидеть все карточки
                                    self._scroll_to_load_all_cards()
                                    time.sleep(2)
                                    # Обновляем page_source после возврата
                                    page_source, soup = self._get_page_source_and_soup()
                        
                        if len(self._collected_card_data) > 0:
                            logger.info(f"Successfully collected {len(self._collected_card_data)} cards via alternative method.")
                            # НЕ прерываем цикл - продолжаем искать на следующих страницах
                
                # Если карточек на странице нет, пробуем перейти на следующую страницу
                if not cards_on_page:
                    logger.warning(f"No cards found on this page. Trying to find next page...")
                    # Не останавливаемся сразу - пробуем найти следующую страницу

                # ВАЖНО: Сначала собираем ВСЕ ссылки на карточки, затем парсим их по очереди
                # Это намного быстрее, чем возвращаться на страницу поиска после каждой карточки
                cards_processed_this_page = 0
                total_cards_to_process = len(cards_on_page)
                logger.info(f"📋 Found {total_cards_to_process} cards on page {self._current_page_number}. Collecting all card URLs first...")
                
                # Собираем все ссылки на карточки
                card_urls_to_parse = []
                cards_without_links = 0
                for card_element in cards_on_page:
                    if len(self._collected_card_data) + len(card_urls_to_parse) >= self._max_records:
                        break
                    
                    card_url = None
                    
                    # Если элемент - это ссылка на организацию, обрабатываем её напрямую
                    if card_element.name == 'a' and card_element.get('href') and '/maps/org/' in card_element.get('href', ''):
                        card_url = card_element.get('href')
                        if not card_url.startswith('http'):
                            card_url = urllib.parse.urljoin("https://yandex.ru", card_url)
                    else:
                        # Ищем ссылку в элементе карточки
                        link_selectors = [
                            'a.card-view__link',
                            'a.search-business-snippet-view__title',
                            'a.catalogue-snippet-view__title',
                            'a[href*="/maps/org/"]',
                            'a.search-snippet-view__title-link',
                            'a[class*="title"]',
                            'a[class*="link"]',
                        ]
                        
                        for selector in link_selectors:
                            card_link_element = card_element.select_one(selector)
                            if card_link_element and card_link_element.get('href'):
                                href = card_link_element.get('href')
                                if '/maps/org/' in href and '/gallery/' not in href:
                                    card_url = href
                                    break
                        
                        # Ищем в родительских элементах
                        if not card_url:
                            current = card_element
                            for _ in range(5):
                                parent = current.find_parent()
                                if not parent:
                                    break
                                for selector in link_selectors:
                                    link = parent.select_one(selector)
                                    if link and link.get('href'):
                                        href = link.get('href')
                                        if '/maps/org/' in href and '/gallery/' not in href:
                                            card_url = href
                                            break
                                if card_url:
                                    break
                                all_links = parent.find_all('a', href=lambda x: x and '/maps/org/' in str(x) and '/gallery/' not in str(x))
                                if all_links:
                                    card_url = all_links[0].get('href')
                                    break
                                current = parent
                        
                        # Дополнительный поиск: ищем любые ссылки внутри карточки
                        if not card_url:
                            all_links_in_card = card_element.find_all('a', href=True)
                            for link in all_links_in_card:
                                href = link.get('href', '')
                                if '/maps/org/' in href and '/gallery/' not in href:
                                    card_url = href
                                    break
                    
                    if card_url:
                        if not card_url.startswith('http'):
                            card_url = urllib.parse.urljoin("https://yandex.ru", card_url)
                        if card_url not in processed_urls and '/gallery/' not in card_url:
                            card_urls_to_parse.append(card_url)
                            processed_urls.add(card_url)
                    else:
                        cards_without_links += 1
                
                if cards_without_links > 0:
                    logger.warning(f"⚠ Found {cards_without_links} cards without valid links on page {self._current_page_number} (total cards: {len(cards_on_page)}, links found: {len(card_urls_to_parse)}). These cards will be skipped.")
                logger.info(f"📊 Page {self._current_page_number} summary: {len(cards_on_page)} cards found, {len(card_urls_to_parse)} with links, {cards_without_links} without links")
                
                logger.info(f"✓ Collected {len(card_urls_to_parse)} unique card URLs. Starting to parse them...")
                
                # Теперь парсим все карточки по очереди БЕЗ возврата на страницу поиска
                for card_url in card_urls_to_parse:
                    if len(self._collected_card_data) >= self._max_records:
                        logger.info(f"Reached max records limit ({self._max_records}). Processed {len(self._collected_card_data)} cards total.")
                        break

                    try:
                        logger.info(f"Parsing card {cards_processed_this_page + 1}/{len(card_urls_to_parse)}: {card_url}")
                        self.driver.navigate(card_url)
                        self.check_captcha()
                        time.sleep(2)
                        
                        card_details_soup = BeautifulSoup(self.driver.get_page_source(), "lxml")
                        card_snippet = self._extract_card_data_from_detail_page(card_details_soup)

                        if card_snippet and card_snippet.get('card_name'):
                            self._collected_card_data.append(card_snippet)
                            self._update_aggregated_data(card_snippet)
                            cards_processed_this_page += 1
                            logger.info(f"✓ Successfully processed card {len(self._collected_card_data)}/{self._max_records}: {card_snippet.get('card_name', 'Unknown')}")
                        else:
                            logger.warning(f"Could not extract data from card: {card_url}")
                    except Exception as e:
                        logger.error(f"Error parsing card {card_url}: {e}", exc_info=True)
                        continue
                
                # ШАГ 5: Агрегируем данные (уже сделано в _update_aggregated_data)
                logger.info(f"✓ Processed {cards_processed_this_page}/{len(card_urls_to_parse)} cards on page {self._current_page_number}. Total collected: {len(self._collected_card_data)}/{self._max_records}")
                
                # Проверяем, нужно ли перейти на следующую страницу
                if len(self._collected_card_data) >= self._max_records:
                    logger.info(f"Reached max records limit ({self._max_records}). Stopping.")
                    break

            except Exception as e:
                logger.error(f"❌ Error processing cards on page {self._current_page_number}: {e}", exc_info=True)
                logger.error(f"Error type: {type(e).__name__}, Error message: {str(e)}")
                import traceback
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
                break

            # ШАГ 6: Возвращаемся на страницу поиска для пагинации (если не достигли лимита)
            if len(self._collected_card_data) < self._max_records:
                logger.info("Step 6: Returning to search page for pagination...")
                # Возвращаемся на ту же страницу поиска, с которой начинали парсить карточки
                if 'current_search_page_url' in locals():
                    self.driver.navigate(current_search_page_url)
                else:
                    self.driver.navigate(search_query_url)
                self.check_captcha()
                time.sleep(2)
                # Обновляем HTML для поиска кнопки следующей страницы
                page_source, soup = self._get_page_source_and_soup()
            else:
                # Если достигли лимита, выходим из цикла
                break
            
            # Улучшенный поиск кнопки следующей страницы
            next_page_url = None
            next_page_button = None
            
            logger.info(f"Searching for next page button (current page: {self._current_page_number}, cards found: {len(self._collected_card_data)})")
            
            # Пробуем разные способы найти следующую страницу
            # 1. По aria-label
            next_page_button = soup.find('a', {'aria-label': 'Следующая страница'})
            if not next_page_button:
                next_page_button = soup.find('a', {'aria-label': 'Next page'})
            if next_page_button:
                logger.info("Found next page button by aria-label")
            
            # 2. По классу (next, pagination-next, и т.д.)
            if not next_page_button:
                next_page_button = soup.find('a', {'class': lambda x: x and ('next' in str(x).lower() or 'pagination' in str(x).lower())})
                if next_page_button:
                    logger.info(f"Found next page button by class: {next_page_button.get('class')}")
            
            # 3. По тексту ссылки
            if not next_page_button:
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    link_text = link.get_text(strip=True).lower()
                    href = link.get('href', '').lower()
                    if any(keyword in link_text for keyword in ['следующ', 'next', 'дальше', 'ещё', 'more']) or \
                       any(keyword in href for keyword in ['page=', 'p=', 'next']):
                        next_page_button = link
                        logger.info(f"Found next page button by text/href: '{link_text}' / '{href}'")
                        break
            
            # 4. По номерам страниц (ищем ссылку на страницу current_page + 1)
            if not next_page_button:
                page_links = soup.find_all('a', href=True)
                current_page_num = self._current_page_number
                for link in page_links:
                    link_text = link.get_text(strip=True)
                    href = link.get('href', '')
                    try:
                        # Пробуем извлечь номер страницы из текста
                        if link_text.isdigit():
                            page_num = int(link_text)
                            if page_num == current_page_num + 1:
                                next_page_button = link
                                logger.info(f"Found next page button by page number: {page_num}")
                                break
                        # Пробуем извлечь номер страницы из href (page=2, p=2, и т.д.)
                        page_match = re.search(r'[?&](?:page|p)=(\d+)', href, re.IGNORECASE)
                        if page_match:
                            page_num = int(page_match.group(1))
                            if page_num == current_page_num + 1:
                                next_page_button = link
                                logger.info(f"Found next page button by href page number: {page_num}")
                                break
                    except:
                        pass
            
            # 5. Пробуем найти через JavaScript/Selenium (если кнопка скрыта или динамическая)
            # ВАЖНО: Только легкая прокрутка вниз для поиска кнопки, НЕ полная прокрутка страницы
            if not next_page_button:
                try:
                    if hasattr(self.driver, 'driver') and self.driver.driver:
                        # Легкая прокрутка вниз, чтобы увидеть кнопку пагинации (если она внизу)
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)
                        # Обновляем HTML после прокрутки
                        page_source, soup = self._get_page_source_and_soup()
                        
                        from selenium.webdriver.common.by import By
                        
                        next_page_selectors = [
                            'a[aria-label*="Следующая"]',
                            'a[aria-label*="Next"]',
                            'a[class*="next"]',
                            'a[class*="pagination-next"]',
                            'a[class*="pagination"]',
                            'button[aria-label*="Следующая"]',
                            'button[aria-label*="Next"]',
                            'a[href*="page="]',
                        ]
                        
                        for selector in next_page_selectors:
                            try:
                                elements = self.driver.driver.find_elements(By.CSS_SELECTOR, selector)
                                for element in elements:
                                    if element and element.is_displayed():
                                        href = element.get_attribute('href')
                                        if href:
                                            current_page_num = self._current_page_number
                                            page_match = re.search(r'[?&](?:page|p)=(\d+)', href, re.IGNORECASE)
                                            if page_match:
                                                page_num = int(page_match.group(1))
                                                if page_num == current_page_num + 1:
                                                    next_page_button = soup.new_tag('a', href=href)
                                                    logger.info(f"Found next page button via Selenium selector: {selector}, href: {href}")
                                                    break
                                            elif current_page_num == 1:
                                                next_page_button = soup.new_tag('a', href=href)
                                                logger.info(f"Found potential next page button via Selenium selector: {selector}, href: {href}")
                                                break
                                if next_page_button:
                                    break
                            except Exception as sel_error:
                                logger.debug(f"Could not find element with selector '{selector}': {sel_error}")
                                continue
                except Exception as e:
                    logger.debug(f"Could not find next page via Selenium: {e}")
            
            if next_page_button and next_page_button.get('href'):
                next_page_url = urllib.parse.urljoin("https://yandex.ru", next_page_button.get('href'))
                if next_page_url in processed_urls:
                    logger.info("Next page URL already processed. Stopping pagination.")
                    break
                processed_urls.add(next_page_url)
                logger.info(f"✓ Found next page! Navigating to page {self._current_page_number + 1}: {next_page_url}")
                self.driver.navigate(next_page_url)
                self.check_captcha()
                self._current_page_number += 1
                time.sleep(3)
                continue
            else:
                logger.info(f"No next page found after processing {len(self._collected_card_data)} cards on {self._current_page_number} pages. Stopping pagination.")
                # Сохраняем HTML для отладки
                try:
                    debug_html_path = os.path.join('output', f'debug_no_next_page_{self._current_page_number}.html')
                    os.makedirs('output', exist_ok=True)
                    with open(debug_html_path, 'w', encoding='utf-8') as f:
                        f.write(str(soup))
                    logger.info(f"Saved debug HTML to {debug_html_path}")
                except:
                    pass
                break

        return self._collected_card_data

    def parse(self, url: str) -> Dict[str, Any]:
        self._url = url
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        search_text_param = query_params.get('text')
        if search_text_param:
            search_text_value = search_text_param[0]
            if ',' in search_text_value:
                parts = search_text_value.split(',', 1)
                self._search_query_name = parts[1].strip()
            else:
                self._search_query_name = search_text_value
        else:
            self._search_query_name = "YandexMapsSearch"

        logger.info(f"Starting Yandex Parser for URL: {url}. Search query name extracted as: {self._search_query_name}")
        logger.info(f"Parser initialized. Max records: {self._max_records}, Current cards: {len(self._collected_card_data)}")

        try:
            logger.info("Calling _parse_cards...")
            collected_cards_data = self._parse_cards(url)
            logger.info(f"_parse_cards returned {len(collected_cards_data)} cards")
        except Exception as e:
            logger.error(f"❌ Error in _parse_cards: {e}", exc_info=True)
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            collected_cards_data = []

        if not collected_cards_data:
            logger.warning("No data was collected from Yandex Maps.")
            # Все равно возвращаем структуру с пустыми данными, но с правильной структурой
            return {
                'aggregated_info': {
                    'search_query_name': self._search_query_name,
                    'total_cards_found': 0,
                    'aggregated_rating': 0.0,
                    'aggregated_reviews_count': 0,
                    'aggregated_positive_reviews': 0,
                    'aggregated_negative_reviews': 0,
                    'aggregated_avg_response_time': 0.0,
                },
                'cards_data': []
            }

        total_cards = len(collected_cards_data)
        self._aggregated_data['total_cards'] = total_cards

        if total_cards > 0:
            valid_ratings = []
            for card in collected_cards_data:
                rating_str = card.get('card_rating', '')
                if rating_str:
                    # Заменяем запятую на точку для русской локали
                    rating_str = str(rating_str).replace(',', '.').strip()
                    try:
                        rating_float = float(rating_str)
                        valid_ratings.append(rating_float)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse rating '{card.get('card_rating')}' for card '{card.get('card_name', 'Unknown')}'")
            avg_rating = sum(valid_ratings) / len(valid_ratings) if valid_ratings else 0.0

            self._aggregated_data['aggregated_rating'] = avg_rating
            self._aggregated_data['total_reviews_count'] = sum(
                [card.get('card_reviews_count', 0) for card in collected_cards_data])
            self._aggregated_data['total_positive_reviews'] = sum(
                [card.get('card_reviews_positive', 0) for card in collected_cards_data])
            self._aggregated_data['total_negative_reviews'] = sum(
                [card.get('card_reviews_negative', 0) for card in collected_cards_data])
            self._aggregated_data['total_answered_reviews_count'] = sum(
                [card.get('card_answered_reviews_count', 0) for card in collected_cards_data])
            self._aggregated_data['total_unanswered_reviews_count'] = sum(
                [card.get('card_unanswered_reviews_count', 0) for card in collected_cards_data])
            self._aggregated_data['total_answered_count'] = sum(
                [1 for card in collected_cards_data if card.get('card_response_status', 'UNKNOWN') != 'UNKNOWN' or card.get('card_answered_reviews_count', 0) > 0])

            total_response_time_days = 0.0
            cards_with_response_time = 0
            for card in collected_cards_data:
                response_time_str = card.get('card_avg_response_time', '')
                if response_time_str:
                    try:
                        response_time_value = float(response_time_str)
                        if response_time_value > 0:
                            total_response_time_days += response_time_value
                            cards_with_response_time += 1
                    except (ValueError, TypeError):
                        logger.warning(
                            f"Could not convert response time to float for card '{card.get('card_name', 'Unknown')}': {response_time_str}")
            self._aggregated_data['total_response_time_sum_days'] = total_response_time_days
            self._aggregated_data['total_response_time_calculated_count'] = cards_with_response_time


        else:
            avg_rating = 0.0

        aggregated_info = {
            'search_query_name': self._search_query_name,
            'total_cards_found': total_cards,
            'aggregated_rating': round(avg_rating, 2),
            'aggregated_reviews_count': self._aggregated_data['total_reviews_count'],
            'aggregated_positive_reviews': self._aggregated_data['total_positive_reviews'],
            'aggregated_negative_reviews': self._aggregated_data['total_negative_reviews'],
            'aggregated_answered_reviews_count': self._aggregated_data['total_answered_reviews_count'],
            'aggregated_unanswered_reviews_count': self._aggregated_data['total_unanswered_reviews_count'],
            'aggregated_avg_response_time': round(
                self._aggregated_data['total_response_time_sum_days'] / self._aggregated_data.get('total_response_time_calculated_count', 1), 2
            ) if self._aggregated_data.get('total_response_time_calculated_count', 0) > 0 and self._aggregated_data['total_response_time_sum_days'] > 0 else 0.0,
        }

        return {'aggregated_info': aggregated_info, 'cards_data': collected_cards_data}
