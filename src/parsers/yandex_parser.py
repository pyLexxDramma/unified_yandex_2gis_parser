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
        self._csrf_token: Optional[str] = None

    @staticmethod
    def get_url_pattern() -> str:
        return r'https?://yandex\.ru/(maps|business)/.*'

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

    def get_name(self, soup_content: BeautifulSoup) -> str:
        try:
            element = soup_content.find("h1", {"class": "card-title-view__title"})
            return element.getText() if element else ""
        except Exception as e:
            self._logger.error(f"Error getting name: {e}")
            return ""

    def get_address(self, soup_content: BeautifulSoup) -> str:
        try:
            element = soup_content.find("div", {"class": "business-contacts-view__address-link"})
            return element.getText() if element else ""
        except Exception as e:
            self._logger.error(f"Error getting address: {e}")
            return ""

    def get_company_url(self, soup_content: BeautifulSoup) -> str:
        try:
            element = soup_content.find("a", {"class": "card-title-view__title-link"})
            if element and element.get('href'):
                return "https://yandex.ru" + element.get('href')
            return ""
        except Exception as e:
            self._logger.error(f"Error getting company URL: {e}")
            return ""

    def get_company_id(self, soup_content: BeautifulSoup) -> str:
        try:
            element = soup_content.find("div", {"class": "business-card-view"})
            return element.get('data-id') if element else ""
        except Exception as e:
            self._logger.error(f"Error getting company ID: {e}")
            return ""

    def get_website(self, soup_content: BeautifulSoup) -> str:
        try:
            element = soup_content.find("span", {"class": "business-urls-view__text"})
            return element.getText() if element else ""
        except Exception as e:
            self._logger.error(f"Error getting website: {e}")
            return ""

    def get_opening_hours(self, soup_content: BeautifulSoup) -> List[str]:
        opening_hours = []
        try:
            for data in soup_content.find_all("meta", {"itemprop": "openingHours"}):
                if data.get('content'):
                    opening_hours.append(data.get('content'))
            return opening_hours
        except Exception as e:
            self._logger.error(f"Error getting opening hours: {e}")
            return []

    def get_goods(self, soup_content: BeautifulSoup) -> Dict[str, Any]:
        return {}

    def get_rating(self, soup_content: BeautifulSoup) -> str:
        rating = ""
        try:
            rating_container = soup_content.find("div", {"class": "business-card-title-view__header-rating"})
            if rating_container:
                rating_text_element = rating_container.find("span",
                                                            {"class": "business-rating-badge-view__rating-text"})
                if rating_text_element:
                    rating = rating_text_element.getText()
            return rating
        except Exception as e:
            self._logger.error(f"Error getting rating: {e}")
            return ""

    def get_reviews(self) -> List[str]:
        self._logger.info("Getting reviews...")

        reviews: List[str] = []

        try:
            page_source, soup_content = self._get_page_source_and_soup()
        except Exception as e:
            self._logger.error(f"Failed to get page source before handling reviews: {e}")
            return []

        reviews_count = 0
        try:
            count_elements = soup_content.find_all("div", {"class": "tabs-select-view__counter"})
            if count_elements:
                reviews_count_text = count_elements[-1].text.strip()
                reviews_count = int(reviews_count_text)
                self._logger.info(f"Total reviews found: {reviews_count}")
            else:
                self._logger.warning("Could not find reviews count element.")
        except (ValueError, AttributeError, IndexError) as e:
            self._logger.warning(f"Could not determine review count: {e}")
        except Exception as e:
            self._logger.error(f"Unexpected error getting review count: {e}")
            return []

        if reviews_count > 150:
            find_range = range(self._reviews_scroll_iterations_max)
        else:
            find_range = range(self._reviews_scroll_iterations_min)

        scroll_container_selector = '.scroll__container'
        for i in find_range:
            try:
                script_to_execute = f"document.querySelectorAll('{scroll_container_selector}')[1].scrollTop={self._reviews_scroll_step * i};"
                self.driver.execute_script(script_to_execute)
                time.sleep(0.2)
            except Exception as e:
                self._logger.warning(f"Error during scroll iteration {i}: {e}. Stopping scroll.")
                break

        try:
            page_source, soup_content = self._get_page_source_and_soup()  # Получаем новый soup_content
            for data in soup_content.find_all("span", {"class": "business-review-view__body-text"}):
                reviews.append(data.getText())
            self._logger.info(f"Successfully extracted {len(reviews)} review texts.")
            return reviews
        except Exception as e:
            self._logger.error(f"Error extracting review texts after scroll: {e}")
            return []

    def parse(self, writer: BaseWriter) -> None:
        if not isinstance(self.driver, BaseDriver):
            self._logger.error("Invalid driver type provided to YandexParser.")
            return

        try:
            self.driver.navigate(self._url, timeout=120)
            self.check_captcha()
        except Exception as e:
            self._logger.error(f"Failed to navigate to {self._url}: {e}")
            return

        page_source, soup_content = self._get_page_source_and_soup()

        data = {
            "name": self.get_name(soup_content),
            "address": self.get_address(soup_content),
            "company_url": self.get_company_url(soup_content),
            "company_id": self.get_company_id(soup_content),
            "website": self.get_website(soup_content),
            "opening_hours": self.get_opening_hours(soup_content),
            "goods": self.get_goods(soup_content),
            "rating": self.get_rating(soup_content),
            "reviews": self.get_reviews(),
        }


        self._logger.info(f"Parsed data for: {data.get('name')}")
