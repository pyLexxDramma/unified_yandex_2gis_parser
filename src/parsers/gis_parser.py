from __future__ import annotations
import json
import re
import logging
import time
import urllib.parse
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from src.drivers.base_driver import BaseDriver
from src.config.settings import AppConfig
from src.parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class GisParser(BaseParser):
    def __init__(self, driver: BaseDriver, settings: AppConfig):
        super().__init__(driver, settings)
        self._url: str = ""
        
        # Параметры прокрутки
        self._scroll_step: int = getattr(self._settings.parser, 'gis_scroll_step', 500)
        self._scroll_max_iter: int = getattr(self._settings.parser, 'gis_scroll_max_iter', 100)
        self._scroll_wait_time: float = getattr(self._settings.parser, 'gis_scroll_wait_time', 0.5)
        self._reviews_scroll_step: int = getattr(self._settings.parser, 'gis_reviews_scroll_step', 500)
        self._reviews_scroll_iterations_max: int = getattr(self._settings.parser, 'gis_reviews_scroll_max_iter', 100)
        self._reviews_scroll_iterations_min: int = getattr(self._settings.parser, 'gis_reviews_scroll_min_iter', 30)
        self._max_records: int = getattr(self._settings.parser, 'max_records', 1000)
        
        # Селекторы для карточек 2GIS (на основе анализа HTML)
        self._card_selectors: List[str] = getattr(self._settings.parser, 'gis_card_selectors', [
            'a[href*="/firm/"]',  # Основной селектор для ссылок на фирмы
            'a[href*="/station/"]',  # Для станций метро
        ])
        
        # Селекторы для прокручиваемого контейнера (на основе анализа HTML)
        # 2GIS использует контейнеры с классами типа _1rkbbi0x, но они могут меняться
        # Поэтому используем поиск по scrollHeight > clientHeight
        self._scrollable_element_selector: str = getattr(self._settings.parser, 'gis_scroll_container', 
                                                       '[class*="_1rkbbi0x"], [class*="scroll"], [class*="list"], [class*="results"]')

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

    def _scroll_to_load_all_cards(self, max_scrolls: Optional[int] = None, scroll_step: Optional[int] = None) -> int:
        """Прокручивает страницу поиска для загрузки всех карточек"""
        logger.info("Starting scroll to load all cards on 2GIS search page")
        
        previous_card_count = 0
        no_change_count = 0
        scroll_iterations = 0
        last_height = 0
        max_card_count = 0
        
        if max_scrolls is None:
            max_scrolls = self._scroll_max_iter
        if scroll_step is None:
            scroll_step = self._scroll_step
        
        logger.info(f"Scroll parameters: Max iterations={max_scrolls}, Scroll step={scroll_step}px, Wait time={self._scroll_wait_time}s")
        
        # Находим прокручиваемый элемент (2GIS использует контейнеры с динамическими классами)
        scrollable_element_selector = None
        try:
            # Сначала пробуем селекторы из конфига
            selector_json = json.dumps(self._scrollable_element_selector)
            find_scrollable_script = f"""
            var selectorStr = {selector_json};
            var selectors = selectorStr.split(',').map(s => s.trim());
            for (var i = 0; i < selectors.length; i++) {{
                var els = document.querySelectorAll(selectors[i]);
                for (var j = 0; j < els.length; j++) {{
                    var el = els[j];
                    if (el && el.scrollHeight > el.clientHeight && el.scrollHeight > 500) {{
                        // Проверяем, что внутри есть карточки
                        var cardsInside = el.querySelectorAll('a[href*="/firm/"], a[href*="/station/"]');
                        if (cardsInside.length > 0) {{
                            return {{
                                'selector': selectors[i],
                                'scrollHeight': el.scrollHeight,
                                'clientHeight': el.clientHeight,
                                'scrollTop': el.scrollTop,
                                'className': el.className || '',
                                'tagName': el.tagName,
                                'cardsInside': cardsInside.length
                            }};
                        }}
                    }}
                }}
            }}
            
            // Если не нашли через селекторы, ищем любой элемент с прокруткой и карточками внутри
            var allElements = document.querySelectorAll('*');
            for (var i = 0; i < allElements.length; i++) {{
                var el = allElements[i];
                var style = window.getComputedStyle(el);
                var hasScroll = el.scrollHeight > el.clientHeight && el.scrollHeight > 1000;
                var hasOverflow = style.overflow === 'auto' || style.overflowY === 'auto' || 
                                  style.overflow === 'scroll' || style.overflowY === 'scroll';
                var cardsInside = el.querySelectorAll('a[href*="/firm/"], a[href*="/station/"]');
                
                if (hasScroll && hasOverflow && cardsInside.length > 5) {{
                    // Создаем селектор из класса
                    var classes = el.className;
                    if (classes && typeof classes === 'string' && classes.trim()) {{
                        var firstClass = classes.split(' ')[0];
                        if (firstClass) {{
                            return {{
                                'selector': '.' + firstClass,
                                'scrollHeight': el.scrollHeight,
                                'clientHeight': el.clientHeight,
                                'scrollTop': el.scrollTop,
                                'className': classes,
                                'tagName': el.tagName,
                                'cardsInside': cardsInside.length
                            }};
                        }}
                    }}
                }}
            }}
            return null;
            """
            scrollable_info = self.driver.execute_script(find_scrollable_script)
            
            if scrollable_info and isinstance(scrollable_info, dict):
                scrollable_element_selector = scrollable_info.get('selector')
                cards_inside = scrollable_info.get('cardsInside', 0)
                logger.info(f"✓ Found scrollable element: selector='{scrollable_element_selector}', cards inside: {cards_inside}")
            else:
                logger.warning("Could not find scrollable element, will use window scroll")
                scrollable_element_selector = None
        except Exception as e:
            logger.error(f"Error finding scrollable element: {e}")
            scrollable_element_selector = None
        
        required_no_change = 10  # Количество итераций без изменений для остановки
        
        while scroll_iterations < max_scrolls:
            try:
                # Подсчитываем текущее количество карточек
                page_source, soup = self._get_page_source_and_soup()
                current_card_count = 0
                for selector in self._card_selectors:
                    found = soup.select(selector)
                    current_card_count = max(current_card_count, len(found))
                
                if current_card_count > max_card_count:
                    max_card_count = current_card_count
                    no_change_count = 0
                    logger.info(f"Scroll iteration {scroll_iterations + 1}: Found {current_card_count} cards (new max: {max_card_count})")
                else:
                    no_change_count += 1
                    if scroll_iterations % 5 == 0:
                        logger.info(f"Scroll iteration {scroll_iterations + 1}: {current_card_count} cards (no change: {no_change_count}/{required_no_change})")
                
                # Если количество карточек не меняется достаточно долго, останавливаемся
                if no_change_count >= required_no_change:
                    logger.info(f"Stopping scroll: no new cards found for {required_no_change} iterations")
                    break
                
                # Прокручиваем
                if scrollable_element_selector:
                    escaped_selector_json = json.dumps(scrollable_element_selector)
                    scroll_script = f"""
                    var selector = {escaped_selector_json};
                    var container = document.querySelector(selector);
                    if (container) {{
                        var maxScrollTop = container.scrollHeight - container.clientHeight;
                        var newScrollTop = Math.min(
                            container.scrollTop + {scroll_step},
                            maxScrollTop
                        );
                        container.scrollTop = newScrollTop;
                        return {{
                            'scrollHeight': container.scrollHeight,
                            'scrollTop': container.scrollTop,
                            'isAtBottom': container.scrollTop >= (maxScrollTop - 10)
                        }};
                    }}
                    return {{'error': 'Container not found'}};
                    """
                    scroll_result = self.driver.execute_script(scroll_script)
                    if scroll_result and isinstance(scroll_result, dict) and scroll_result.get('isAtBottom'):
                        logger.info("Reached bottom of scrollable container")
                        break
                else:
                    # Прокручиваем окно
                    current_height = self.driver.execute_script("return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);")
                    current_scroll_top = self.driver.execute_script("return window.pageYOffset || document.documentElement.scrollTop || 0;")
                    new_scroll_top = min(current_scroll_top + scroll_step, current_height)
                    self.driver.execute_script(f"window.scrollTo({{top: {new_scroll_top}, left: 0, behavior: 'auto'}});")
                    
                    if new_scroll_top >= current_height - 10:
                        logger.info("Reached bottom of page")
                        break
                
                time.sleep(self._scroll_wait_time)
                scroll_iterations += 1
                
            except Exception as e:
                logger.error(f"Error during scroll iteration {scroll_iterations + 1}: {e}")
                break
        
        logger.info(f"Scroll completed: {scroll_iterations} iterations, found {max_card_count} cards")
        return max_card_count
    
    def _get_links(self) -> List[str]:
        """Получает ссылки на карточки со страницы поиска 2GIS (возвращает список URL)
        Использует агрессивный поиск, аналогичный Yandex парсеру"""
        try:
            page_source, soup = self._get_page_source_and_soup()
            valid_urls = set()
            
            # МЕТОД 1: Прямой поиск всех ссылок на карточки в HTML (наиболее надежный)
            logger.info("Method 1: Direct search for all card links in HTML...")
            card_links = soup.select('a[href*="/firm/"], a[href*="/station/"]')
            logger.info(f"Found {len(card_links)} links with /firm/ or /station/ in href")
            
            for link in card_links:
                href = link.get('href', '')
                if href:
                    # Нормализуем URL
                    if not href.startswith('http'):
                        href = urllib.parse.urljoin("https://2gis.ru", href)
                    
                    # Проверяем, что это валидная ссылка на карточку
                    # 2GIS использует формат: /city/firm/ID или /city/station/ID
                    if re.match(r'.*/(firm|station)/\d+', href):
                        # Нормализуем URL (убираем параметры для дедупликации, но сохраняем базовый URL)
                        normalized_url = href.split('?')[0] if '?' in href else href
                        valid_urls.add(normalized_url)
            
            logger.info(f"Method 1 found {len(valid_urls)} unique card URLs")
            
            # МЕТОД 2: Поиск через элементы карточек (если нашли мало ссылок напрямую)
            # Сначала проверяем, сколько карточек найдено через селекторы
            cards_on_page = []
            seen_ids = set()
            for selector in self._card_selectors:
                found = soup.select(selector)
                for card in found:
                    card_id = id(card)
                    if card_id not in seen_ids:
                        seen_ids.add(card_id)
                        cards_on_page.append(card)
            
            logger.info(f"Found {len(cards_on_page)} card elements using selectors")
            
            # Если нашли мало ссылок напрямую, используем альтернативный метод через элементы
            if len(valid_urls) < len(cards_on_page) * 0.8:  # Если нашли меньше 80% карточек
                logger.info(f"Method 1 found {len(valid_urls)} URLs, but {len(cards_on_page)} card elements found. Using element-based search...")
                
                for card_element in cards_on_page:
                    card_url = None
                    
                    # Если элемент - это ссылка на карточку, обрабатываем её напрямую
                    if card_element.name == 'a' and card_element.get('href'):
                        href = card_element.get('href')
                        if '/firm/' in href or '/station/' in href:
                            card_url = href
                    else:
                        # Ищем ссылку в элементе карточки
                        link_selectors = [
                            'a[href*="/firm/"]',
                            'a[href*="/station/"]',
                            'a[class*="link"]',
                            'a[class*="title"]',
                            'a[class*="card"]',
                        ]
                        
                        for selector in link_selectors:
                            card_link_element = card_element.select_one(selector)
                            if card_link_element and card_link_element.get('href'):
                                href = card_link_element.get('href')
                                if '/firm/' in href or '/station/' in href:
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
                                        if '/firm/' in href or '/station/' in href:
                                            card_url = href
                                            break
                                if card_url:
                                    break
                                all_links = parent.find_all('a', href=lambda x: x and ('/firm/' in str(x) or '/station/' in str(x)))
                                if all_links:
                                    card_url = all_links[0].get('href')
                                    break
                                current = parent
                    
                    if card_url:
                        if not card_url.startswith('http'):
                            card_url = urllib.parse.urljoin("https://2gis.ru", card_url)
                        # Нормализуем URL
                        normalized_url = card_url.split('?')[0] if '?' in card_url else card_url
                        if re.match(r'.*/(firm|station)/\d+', normalized_url):
                            valid_urls.add(normalized_url)
                
                logger.info(f"Method 2 added URLs. Total unique URLs: {len(valid_urls)}")
            
            logger.info(f"✓ Collected {len(valid_urls)} unique card URLs total")
            return list(valid_urls)
            
        except Exception as e:
            logger.error(f"Error getting link elements: {e}", exc_info=True)
            return []

    def _get_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """Находит все ссылки на страницы пагинации в 2GIS
        
        Структура пагинации 2GIS:
        <div><div class="_l934xo5"><span class="_19xy60y">1</span></div>
        <a href="/izhevsk/search/.../page/2" class="_12164l30"><span class="_19xy60y">2</span></a>
        <a href="/izhevsk/search/.../page/3" class="_12164l30"><span class="_19xy60y">3</span></a></div>
        """
        pagination_urls = []
        try:
            # Метод 1: Ищем ссылки с /page/ в href (наиболее надежный для 2GIS)
            page_links = soup.select('a[href*="/page/"]')
            logger.debug(f"Found {len(page_links)} links with /page/ in href")
            
            for link in page_links:
                href = link.get('href', '')
                if href and '/page/' in href:
                    # Нормализуем URL
                    if not href.startswith('http'):
                        href = urllib.parse.urljoin("https://2gis.ru", href)
                    # Проверяем, что это ссылка на страницу поиска (не на карточку)
                    if '/search/' in href and '/page/' in href and '/firm/' not in href and '/station/' not in href:
                        # Проверяем, что это действительно номер страницы (не часть другого пути)
                        page_match = re.search(r'/page/(\d+)', href)
                        if page_match:
                            if href not in pagination_urls:
                                pagination_urls.append(href)
            
            # Метод 2: Ищем через классы пагинации (если метод 1 не сработал)
            if not pagination_urls:
                pagination_containers = soup.select('div[class*="_l934xo5"], div[class*="pagination"], div[class*="page"]')
                logger.debug(f"Found {len(pagination_containers)} pagination containers")
                for container in pagination_containers:
                    links = container.select('a[href*="/page/"], a[href*="page="]')
                    for link in links:
                        href = link.get('href', '')
                        if href:
                            if not href.startswith('http'):
                                href = urllib.parse.urljoin("https://2gis.ru", href)
                            if '/search/' in href and '/page/' in href:
                                page_match = re.search(r'/page/(\d+)', href)
                                if page_match:
                                    if href not in pagination_urls:
                                        pagination_urls.append(href)
            
            # Метод 3: Ищем все ссылки с номерами страниц в тексте
            if not pagination_urls:
                all_links = soup.find_all('a', href=True)
                logger.debug(f"Checking {len(all_links)} links for pagination")
                for link in all_links:
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)
                    # Проверяем, что это ссылка на страницу поиска и содержит номер страницы
                    if '/search/' in href and link_text.isdigit():
                        if not href.startswith('http'):
                            href = urllib.parse.urljoin("https://2gis.ru", href)
                        # Проверяем, что это не ссылка на карточку и содержит /page/
                        if '/firm/' not in href and '/station/' not in href and '/page/' in href:
                            page_match = re.search(r'/page/(\d+)', href)
                            if page_match:
                                if href not in pagination_urls:
                                    pagination_urls.append(href)
            
            # Сортируем URL по номеру страницы для правильного порядка
            def extract_page_number(url):
                match = re.search(r'/page/(\d+)', url)
                if match:
                    return int(match.group(1))
                return 0
            
            pagination_urls = sorted(set(pagination_urls), key=extract_page_number)
            logger.info(f"✓ Found {len(pagination_urls)} unique pagination pages: {[extract_page_number(u) for u in pagination_urls]}")
            
        except Exception as e:
            logger.error(f"Error getting pagination links: {e}", exc_info=True)
        
        return pagination_urls
    
    def _wait_requests_finished(self, timeout: int = 10) -> bool:
        """Ждет завершения всех запросов"""
        try:
            if hasattr(self.driver, 'tab') and hasattr(self.driver.tab, 'set_default_timeout'):
                self.driver.tab.set_default_timeout(timeout)
            result = self.driver.execute_script(
                'return typeof window.openHTTPs === "undefined" ? 0 : window.openHTTPs;')
            return result == 0
        except Exception as e:
            logger.error(f"Error waiting for requests to finish: {e}")
            return True

    def _get_page_source_and_soup(self) -> Tuple[str, BeautifulSoup]:
        """Получает исходный код страницы и парсит его в BeautifulSoup"""
        page_source = self.driver.get_page_source()
        soup = BeautifulSoup(page_source, "lxml")
        return page_source, soup
    
    def _parse_date_string(self, date_string: str) -> Optional[datetime]:
        """Парсит строку с датой в различных форматах (аналогично Yandex)"""
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
                '%Y-%m-%d %H:%M:%S',
                '%d.%m.%Y %H:%M',
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_string, fmt)
                except:
                    continue
            
            # Относительные даты
            relative_patterns = [
                (r'(сегодня|today)', 0),
                (r'(вчера|yesterday)', 1),
                (r'(позавчера)', 2),
                (r'(\d+)\s*(час|часа|часов)\s*(назад|ago)', 1/24),
                (r'(\d+)\s*(день|дня|дней)\s*(назад|ago)', 1),
                (r'(\d+)\s*(недел|недели|недель)\s*(назад|ago)', 7),
                (r'(\d+)\s*(месяц|месяца|месяцев)\s*(назад|ago)', 30),
            ]
            
            for pattern, days_offset in relative_patterns:
                match = re.search(pattern, date_string, re.IGNORECASE)
                if match:
                    if days_offset == 0:
                        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    elif days_offset < 1:
                        hours = int(match.group(1)) if match.groups() else 0
                        return datetime.now() - timedelta(hours=hours)
                    else:
                        days = int(match.group(1)) if match.groups() else int(days_offset)
                        return datetime.now() - timedelta(days=days)
            
            return None
        except Exception as e:
            logger.debug(f"Error parsing date string '{date_string}': {e}")
            return None
    
    def _scroll_to_load_all_reviews(self) -> None:
        """Прокручивает страницу отзывов для загрузки всех отзывов"""
        try:
            scroll_iterations = 0
            max_scrolls = self._reviews_scroll_iterations_max
            scroll_step = self._reviews_scroll_step
            no_change_count = 0
            required_no_change = 8
            last_review_count = 0
            
            while scroll_iterations < max_scrolls:
                # Подсчитываем текущее количество отзывов
                page_source, soup = self._get_page_source_and_soup()
                review_selectors = [
                    'div[class*="review"]',
                    'div[class*="Review"]',
                    'li[class*="review"]',
                    '[data-test="review"]',
                ]
                
                current_review_count = 0
                for selector in review_selectors:
                    found = soup.select(selector)
                    current_review_count = max(current_review_count, len(found))
                
                if current_review_count > last_review_count:
                    last_review_count = current_review_count
                    no_change_count = 0
                    logger.debug(f"Scroll reviews iteration {scroll_iterations + 1}: Found {current_review_count} reviews")
                else:
                    no_change_count += 1
                    if no_change_count >= required_no_change:
                        logger.debug(f"Stopping reviews scroll: no new reviews for {required_no_change} iterations")
                        break
                
                # Прокручиваем
                self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")
                time.sleep(0.5)
                scroll_iterations += 1
            
            logger.info(f"Reviews scroll completed: {scroll_iterations} iterations, found {last_review_count} reviews")
        except Exception as e:
            logger.warning(f"Error scrolling reviews: {e}")
    
    def _get_card_reviews_info(self) -> Dict[str, Any]:
        """Парсит отзывы из HTML страницы карточки 2GIS"""
        reviews_info = {'reviews_count': 0, 'positive_reviews': 0, 'negative_reviews': 0, 'texts': [], 'details': []}
        
        try:
            page_source, soup_content = self._get_page_source_and_soup()
            
            # Проверяем, есть ли ссылка на страницу отзывов
            review_link = soup_content.select_one('a[href*="/tab/reviews"], a[href*="/reviews"]')
            if review_link:
                review_url = review_link.get('href', '')
                if review_url:
                    if not review_url.startswith('http'):
                        review_url = urllib.parse.urljoin("https://2gis.ru", review_url)
                    logger.info(f"Navigating to reviews page: {review_url}")
                    try:
                        self.driver.navigate(review_url)
                        time.sleep(3)  # Ждем загрузки страницы отзывов
                        page_source, soup_content = self._get_page_source_and_soup()
                    except Exception as nav_error:
                        logger.warning(f"Could not navigate to reviews page: {nav_error}")
            
            # Прокручиваем страницу для загрузки всех отзывов
            self._scroll_to_load_all_reviews()
            
            # Получаем обновленный HTML после прокрутки
            page_source, soup_content = self._get_page_source_and_soup()
        except Exception as e:
            logger.error(f"Failed to get page source for reviews: {e}")
            return reviews_info
        
        # Ищем количество отзывов
        reviews_count_total = 0
        try:
            count_selectors = [
                '[class*="review"] [class*="count"]',
                '[class*="reviews"] [class*="count"]',
                '[data-test="reviews-count"]',
                'span[class*="count"]',
            ]
            
            for selector in count_selectors:
                count_elements = soup_content.select(selector)
                for elem in count_elements:
                    reviews_count_text = elem.get_text(strip=True)
                    matches = re.findall(r'(\d+)', reviews_count_text)
                    if matches:
                        potential_count = max([int(m) for m in matches])
                        if potential_count > reviews_count_total:
                            reviews_count_total = potential_count
                            logger.info(f"Found reviews count {reviews_count_total} using selector: {selector}")
        except Exception as e:
            logger.warning(f"Error extracting reviews count: {e}")
        
        # Парсим отзывы из HTML
        # 2GIS использует класс _1k5soqfl для карточек отзывов (динамический, может меняться)
        try:
            review_selectors = [
                'div._1k5soqfl',  # Основной класс карточки отзыва (на основе анализа)
                'div[class*="review"]',
                'div[class*="Review"]',
                'li[class*="review"]',
                'article[class*="review"]',
                '[data-test="review"]',
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
            
            # Дедупликация
            seen_reviews = set()
            
            for card in review_cards:
                try:
                    # Парсим рейтинг (2GIS использует SVG звезды)
                    rating_value = 0.0
                    # Ищем SVG элементы (звезды рейтинга)
                    svg_elements = card.select('svg')
                    if svg_elements:
                        # В 2GIS рейтинг отображается через SVG звезды
                        # Ищем родительский контейнер со звездами
                        star_containers = card.select('[class*="_1fkin5c"], [class*="_1hxi5gz"], [class*="star"], [class*="rating"]')
                        for star_container in star_containers:
                            # Считаем количество SVG элементов (звезд) в контейнере
                            svgs_in_container = star_container.select('svg')
                            # В 2GIS обычно 5 звезд, рейтинг = количество заполненных
                            # Проверяем, есть ли атрибуты, указывающие на заполненность
                            filled_count = 0
                            for svg in svgs_in_container:
                                # Проверяем fill, stroke или другие атрибуты
                                fill_attr = svg.get('fill') or (svg.select_one('path') and svg.select_one('path').get('fill'))
                                # Проверяем, есть ли path с fill
                                path = svg.select_one('path')
                                if path:
                                    path_fill = path.get('fill', '')
                                    # Если fill не пустой и не прозрачный, считаем звезду заполненной
                                    if path_fill and path_fill not in ['none', 'transparent', '']:
                                        filled_count += 1
                                    # Также проверяем через style
                                    style = path.get('style', '')
                                    if 'fill' in style and 'none' not in style.lower() and 'transparent' not in style.lower():
                                        filled_count += 1
                                else:
                                    # Если нет path, но есть SVG, считаем заполненной
                                    if fill_attr and fill_attr not in ['none', 'transparent']:
                                        filled_count += 1
                            
                            # Если нашли заполненные звезды, используем это значение
                            if filled_count > 0 and filled_count <= 5:
                                rating_value = float(filled_count)
                                logger.debug(f"Found rating {rating_value} from {filled_count} filled stars in container")
                                break
                        
                        # Если не нашли через контейнеры, пробуем другой подход
                        if rating_value == 0.0:
                            # Считаем все SVG элементы как потенциальные звезды
                            # В 2GIS обычно все звезды отображаются, но заполнены только часть
                            # Попробуем найти паттерн: если есть 5 SVG, это может быть рейтинг
                            if len(svg_elements) == 5:
                                # Пробуем определить, сколько из них заполнено
                                filled_svgs = 0
                                for svg in svg_elements:
                                    path = svg.select_one('path')
                                    if path:
                                        path_fill = path.get('fill', '')
                                        if path_fill and path_fill not in ['none', 'transparent', '']:
                                            filled_svgs += 1
                                        else:
                                            # Проверяем через style
                                            style = path.get('style', '')
                                            if style and 'fill' in style and 'none' not in style.lower():
                                                filled_svgs += 1
                                            else:
                                                # Если нет fill, но есть SVG, считаем заполненной (обычно так в 2GIS)
                                                filled_svgs += 1
                                    else:
                                        filled_svgs += 1
                                
                                if filled_svgs > 0 and filled_svgs <= 5:
                                    rating_value = float(filled_svgs)
                                    logger.debug(f"Found rating {rating_value} from {filled_svgs} filled stars (total 5 SVG)")
                            elif len(svg_elements) > 0 and len(svg_elements) <= 5:
                                # Если меньше 5 SVG, это может быть рейтинг
                                rating_value = float(len(svg_elements))
                                logger.debug(f"Found rating {rating_value} from {len(svg_elements)} SVG elements")
                    
                    # Если не нашли через SVG, пробуем другие методы
                    if rating_value == 0.0:
                        rating_selectors = [
                            '[class*="rating"]',
                            '[class*="star"]',
                            '[data-rating]',
                            '[data-test="rating"]',
                        ]
                        
                        for rating_selector in rating_selectors:
                            rating_elements = card.select(rating_selector)
                            for rating_element in rating_elements:
                                # Пробуем data-rating
                                if rating_element.get('data-rating'):
                                    try:
                                        rating_value = float(rating_element.get('data-rating'))
                                        if 1.0 <= rating_value <= 5.0:
                                            break
                                    except:
                                        pass
                                
                                # Пробуем текст
                                rating_text = rating_element.get_text(strip=True)
                                matches = re.findall(r'([1-5](?:[.,]\d+)?)', rating_text)
                                if matches:
                                    try:
                                        rating_value = float(matches[0].replace(',', '.'))
                                        if 1.0 <= rating_value <= 5.0:
                                            break
                                    except:
                                        pass
                            
                            if rating_value > 0:
                                break
                    
                    # Парсим текст отзыва (2GIS использует класс _49x36f для текста отзыва)
                    review_text = ""
                    text_selectors = [
                        'div._49x36f',  # Основной класс текста отзыва (на основе анализа)
                        'a._1msln3t',  # Ссылка с текстом отзыва
                        '[class*="_49x36f"]',
                        '[class*="text"]',
                        '[class*="comment"]',
                        '[class*="content"]',
                        '[data-test="review-text"]',
                    ]
                    
                    for text_selector in text_selectors:
                        text_elements = card.select(text_selector)
                        for text_element in text_elements:
                            review_text = text_element.get_text(separator=' ', strip=True)
                            review_text = ' '.join(review_text.split())
                            # Фильтруем слишком короткие или служебные тексты
                            if review_text and len(review_text) > 10 and not review_text.startswith('Полезно'):
                                break
                        if review_text and len(review_text) > 10:
                            break
                    
                    # Если не нашли через селекторы, берем самый длинный текстовый блок
                    if not review_text or len(review_text) < 10:
                        all_text_elements = card.select('p, div, span, a')
                        longest_text = ""
                        for text_elem in all_text_elements:
                            text = text_elem.get_text(separator=' ', strip=True)
                            # Пропускаем служебные тексты
                            if (len(text) > len(longest_text) and 
                                len(text) > 20 and len(text) < 2000 and
                                not any(skip in text.lower() for skip in ['полезно', 'подтверждён', 'официальный ответ', 'отзыв', 'звезд'])):
                                longest_text = text
                        
                        if longest_text:
                            review_text = longest_text[:1000]
                    
                    # Парсим автора (2GIS использует классы _wrdavn или _16s5yj36 для имени)
                    author_name = ""
                    author_selectors = [
                        'span._wrdavn',  # Основной класс имени автора (на основе анализа)
                        'span._16s5yj36',  # Альтернативный класс
                        '[class*="_wrdavn"]',
                        '[class*="_16s5yj36"]',
                        '[class*="author"]',
                        '[class*="user"]',
                        '[class*="name"]',
                        '[data-test="author"]',
                    ]
                    
                    for author_selector in author_selectors:
                        author_elements = card.select(author_selector)
                        for author_element in author_elements:
                            author_text = author_element.get_text(strip=True)
                            if author_text and len(author_text) > 2 and len(author_text) < 100:
                                skip_phrases = ['день', 'недел', 'месяц', 'год', 'назад', 'сегодня', 'вчера', 
                                               'по умолчанию', 'подписаться', 'уровня', 'уровень', 'отзыв', 'отзывов',
                                               'официальный ответ', 'магнит', 'аптека']
                                if not any(skip in author_text.lower() for skip in skip_phrases):
                                    author_name = author_text
                                    # Очищаем имя автора
                                    author_name = re.sub(r'Знаток города\s+\d+\s+уровня?', '', author_name, flags=re.IGNORECASE)
                                    author_name = re.sub(r'Подписаться|Отписаться', '', author_name, flags=re.IGNORECASE)
                                    author_name = re.sub(r'\d+\s*отзыв', '', author_name, flags=re.IGNORECASE)
                                    author_name = ' '.join(author_name.split())
                                    if '\n' in author_name:
                                        author_name = author_name.split('\n')[0].strip()
                                    author_name = re.sub(r'\s+\d+\s*$', '', author_name)
                                    author_name = author_name.strip()
                                    if author_name and len(author_name) > 1:
                                        break
                        if author_name and len(author_name) > 1:
                            break
                    
                    # Парсим дату отзыва (2GIS использует класс _a5f6uz для даты отзыва)
                    review_date = None
                    date_selectors = [
                        'div._a5f6uz',  # Основной класс даты отзыва (на основе анализа)
                        '[class*="_a5f6uz"]',
                        'time[datetime]',
                        '[datetime]',
                        '[class*="date"]',
                        '[class*="time"]',
                        '[data-date]',
                        '[data-test="date"]',
                    ]
                    
                    for date_selector in date_selectors:
                        date_elems = card.select(date_selector)
                        for date_elem in date_elems:
                            if date_elem.get('datetime'):
                                date_text = date_elem.get('datetime')
                                review_date = self._parse_date_string(date_text)
                                if review_date:
                                    break
                            
                            if not review_date:
                                for attr_name in ['data-date', 'data-time']:
                                    if date_elem.get(attr_name):
                                        date_text = date_elem.get(attr_name)
                                        review_date = self._parse_date_string(date_text)
                                        if review_date:
                                            break
                                if review_date:
                                    break
                            
                            if not review_date:
                                date_text = date_elem.get_text(strip=True)
                                if date_text and len(date_text) < 50:
                                    # Пропускаем даты ответов компании
                                    if not any(skip in date_text.lower() for skip in ['автор', 'отзыв', 'рейтинг', 'звезд', 'официальный ответ']):
                                        review_date = self._parse_date_string(date_text)
                                        if review_date:
                                            break
                        if review_date:
                            break
                    
                    # Ищем ответ компании (2GIS использует класс _sgs1pz или _nqaxddm для блока ответа)
                    response_date = None
                    has_response = False
                    response_selectors = [
                        'div._sgs1pz',  # Основной класс блока ответа (на основе анализа)
                        'div._nqaxddm',  # Альтернативный класс
                        '[class*="_sgs1pz"]',
                        '[class*="_nqaxddm"]',
                        '[class*="response"]',
                        '[class*="answer"]',
                        '[class*="company"]',
                        '[data-test="response"]',
                    ]
                    
                    response_block = None
                    for resp_selector in response_selectors:
                        response_block = card.select_one(resp_selector)
                        if response_block:
                            # Проверяем, что это действительно ответ компании
                            response_text = response_block.get_text(strip=True).lower()
                            if 'официальный ответ' in response_text or 'ответ' in response_text:
                                has_response = True
                                break
                    
                    if response_block:
                        # Ищем дату ответа (класс _1evjsdb для даты ответа)
                        response_date_selectors = [
                            'div._1evjsdb',  # Класс даты ответа (на основе анализа)
                            '[class*="_1evjsdb"]',
                            'time[datetime]',
                            '[datetime]',
                            '[class*="date"]',
                        ]
                        
                        for date_selector in response_date_selectors:
                            date_elems = response_block.select(date_selector)
                            for date_elem in date_elems:
                                if date_elem.get('datetime'):
                                    date_text = date_elem.get('datetime')
                                    response_date = self._parse_date_string(date_text)
                                    if response_date:
                                        break
                                
                                if not response_date:
                                    date_text = date_elem.get_text(strip=True)
                                    if date_text and len(date_text) < 50:
                                        # Ищем паттерн даты в тексте ответа
                                        date_match = re.search(r'\d{1,2}\s*(январ|феврал|март|апрел|май|июн|июл|август|сентябр|октябр|ноябр|декабр)\s+\d{4}', date_text, re.IGNORECASE)
                                        if date_match:
                                            response_date = self._parse_date_string(date_match.group(0))
                                            if response_date:
                                                break
                            if response_date:
                                break
                    
                    # Сохраняем оригинальный текст
                    original_review_text = review_text if review_text else ""
                    
                    # Фильтрация
                    if review_text and any(skip in review_text.lower() for skip in ['оцените это место', 'по умолчанию']):
                        continue
                    
                    if rating_value == 0.0 and (not review_text or review_text == "Без текста" or len(review_text.strip()) < 5):
                        continue
                    
                    # Дедупликация
                    normalized_text = review_text.strip().lower() if review_text else ""
                    normalized_author = author_name.strip().lower() if author_name else ""
                    text_hash = hashlib.md5(normalized_text.encode('utf-8')).hexdigest()[:12] if normalized_text else ""
                    review_key = f"{text_hash}_{rating_value}_{normalized_author[:20]}"
                    
                    if review_key in seen_reviews:
                        continue
                    
                    seen_reviews.add(review_key)
                    
                    final_review_text = original_review_text.strip() if original_review_text and original_review_text.strip() and original_review_text != "Без текста" else review_text.strip() if review_text else ""
                    
                    response_date_str = ""
                    if response_date:
                        if response_date.hour == 0 and response_date.minute == 0 and response_date.second == 0:
                            response_date_str = response_date.strftime('%Y-%m-%d')
                        else:
                            response_date_str = response_date.strftime('%Y-%m-%d %H:%M:%S')
                    
                    reviews_info['details'].append({
                        'review_rating': rating_value,
                        'review_text': final_review_text,
                        'review_author': author_name if author_name else "",
                        'review_date': review_date.strftime('%Y-%m-%d') if review_date else "",
                        'has_response': has_response,
                        'response_date': response_date_str
                    })
                    
                    # Подсчет позитивных/негативных
                    if rating_value > 0.0:
                        if rating_value >= 4.0:
                            reviews_info['positive_reviews'] += 1
                            logger.debug(f"✓ Positive review: rating={rating_value}, author={author_name[:20] if author_name else 'N/A'}")
                        else:
                            reviews_info['negative_reviews'] += 1
                            logger.debug(f"✗ Negative review: rating={rating_value}, author={author_name[:20] if author_name else 'N/A'}")
                    else:
                        logger.warning(f"⚠ Review with zero or invalid rating: rating={rating_value}, author={author_name[:20] if author_name else 'N/A'}, text={final_review_text[:50] if final_review_text else 'N/A'}")
                
                except Exception as e:
                    logger.warning(f"Error processing review card: {e}", exc_info=True)
                    continue
            
            # Вычисляем среднее время ответа
            response_times = []
            for review_detail in reviews_info['details']:
                if review_detail.get('has_response') and review_detail.get('review_date') and review_detail.get('response_date'):
                    try:
                        review_date_str = review_detail.get('review_date')
                        response_date_str = review_detail.get('response_date')
                        
                        if ' ' in review_date_str:
                            review_date = datetime.strptime(review_date_str, '%Y-%m-%d %H:%M:%S')
                        else:
                            review_date = datetime.strptime(review_date_str, '%Y-%m-%d')
                        
                        if ' ' in response_date_str:
                            response_date = datetime.strptime(response_date_str, '%Y-%m-%d %H:%M:%S')
                        else:
                            response_date = datetime.strptime(response_date_str, '%Y-%m-%d')
                        
                        time_diff = response_date - review_date
                        if time_diff.total_seconds() > 0:
                            days = time_diff.days + (time_diff.seconds / 86400.0)
                            response_times.append(days)
                    except Exception as e:
                        logger.warning(f"Error calculating response time: {e}")
                        continue
            
            if response_times:
                avg_response_time = sum(response_times) / len(response_times)
                reviews_info['avg_response_time_days'] = round(avg_response_time, 2)
                reviews_info['response_times_count'] = len(response_times)
                logger.info(f"✓ Calculated average response time: {avg_response_time:.2f} days from {len(response_times)} reviews")
            else:
                reviews_info['avg_response_time_days'] = 0.0
                reviews_info['response_times_count'] = 0
            
            reviews_info['reviews_count'] = len(reviews_info['details'])
            if reviews_info['reviews_count'] == 0 and reviews_count_total > 0:
                reviews_info['reviews_count'] = reviews_count_total
            
            logger.info(f"Reviews summary: total={reviews_info['reviews_count']}, positive={reviews_info['positive_reviews']}, negative={reviews_info['negative_reviews']}")
        
        except Exception as e:
            logger.error(f"Error processing reviews: {e}", exc_info=True)
        
        return reviews_info
    
    def _extract_address_from_page(self, soup: BeautifulSoup) -> str:
        """Извлекает адрес из HTML страницы карточки"""
        address = ""
        address_selectors = [
            '[class*="address"]',
            '[data-test="address"]',
            '[itemprop="address"]',
            'span[class*="street"]',
            'div[class*="location"]',
        ]
        
        for selector in address_selectors:
            address_elements = soup.select(selector)
            for elem in address_elements:
                address_text = elem.get_text(strip=True)
                if address_text and len(address_text) > 5:
                    address = address_text
                    break
            if address:
                break
        
        return address
    
    def _extract_phone_from_page(self, soup: BeautifulSoup) -> str:
        """Извлекает телефон из HTML страницы карточки"""
        phone = ""
        phone_selectors = [
            '[class*="phone"]',
            '[data-test="phone"]',
            '[itemprop="telephone"]',
            'a[href^="tel:"]',
            '[class*="contact"]',
        ]
        
        for selector in phone_selectors:
            phone_elements = soup.select(selector)
            for elem in phone_elements:
                # Проверяем href для tel: ссылок
                href = elem.get('href', '')
                if href and href.startswith('tel:'):
                    phone = href.replace('tel:', '').strip()
                    if phone:
                        break
                
                # Проверяем текст элемента
                phone_text = elem.get_text(strip=True)
                # Ищем паттерн телефона (цифры, скобки, дефисы)
                phone_match = re.search(r'[\d\s\(\)\-\+]+', phone_text)
                if phone_match:
                    potential_phone = phone_match.group(0).strip()
                    # Проверяем, что это похоже на телефон (минимум 7 цифр)
                    digits = re.sub(r'\D', '', potential_phone)
                    if len(digits) >= 7:
                        phone = potential_phone
                        break
            if phone:
                break
        
        return phone
    
    def _get_item_data_from_response(self, response_data: Dict[str, Any], card_url: str = "") -> Optional[Dict[str, Any]]:
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
            
            # Парсим адрес и отзывы из HTML
            address = ""
            reviews_data = {'reviews_count': 0, 'positive_reviews': 0, 'negative_reviews': 0, 'details': []}
            
            try:
                page_source, soup = self._get_page_source_and_soup()
                address = self._extract_address_from_page(soup)
                # Извлекаем телефон из HTML, если API не вернул
                if not phones:
                    phone_from_html = self._extract_phone_from_page(soup)
                    if phone_from_html:
                        phones = [phone_from_html]
                reviews_data = self._get_card_reviews_info()
            except Exception as e:
                logger.warning(f"Error parsing HTML for card {card_url}: {e}")
            
            # Используем среднее время ответа из отзывов, если доступно
            if reviews_data.get('avg_response_time_days', 0) > 0:
                avg_response_time_days = reviews_data.get('avg_response_time_days', 0)
                logger.info(f"Using calculated avg response time: {avg_response_time_days:.2f} days from {reviews_data.get('response_times_count', 0)} reviews")
            
            # Подсчитываем отвеченные/неотвеченные отзывы
            detailed_reviews_list = reviews_data.get('details', [])
            answered_reviews_count = sum(1 for r in detailed_reviews_list if r.get('has_response', False))
            unanswered_reviews_count = len(detailed_reviews_list) - answered_reviews_count
            
            # Конвертируем среднее время ответа в месяцы (для 2GIS)
            avg_response_time_months = ""
            if avg_response_time_days and isinstance(avg_response_time_days, (int, float)) and avg_response_time_days > 0:
                avg_response_time_months = round(avg_response_time_days / 30.0, 2)
            
            return {
                'card_name': name,
                'card_address': address,
                'card_rating': rating,
                'card_reviews_count': reviews_data.get('reviews_count', reviews_count),
                'card_website': website,
                'card_phone': phones[0] if phones else '',
                'card_rubrics': "; ".join(rubrics) if rubrics else '',
                'card_response_status': 'YES' if answered_count > 0 or answered_reviews_count > 0 else 'NO',
                'card_answered_reviews_count': answered_reviews_count,
                'card_unanswered_reviews_count': unanswered_reviews_count,
                'card_avg_response_time': avg_response_time_months if avg_response_time_months else "",
                'card_reviews_positive': reviews_data.get('positive_reviews', 0),
                'card_reviews_negative': reviews_data.get('negative_reviews', 0),
                'card_reviews_texts': "",
                'detailed_reviews': detailed_reviews_list,
            }
        except Exception as e:
            logger.error(f"Error processing item data from response: {e}")
            return None

    def get_url_pattern(self) -> str:
        return r"https://2gis\.ru/.*"

    def parse(self, url: str) -> Dict[str, Any]:
        logger.info(f"Starting 2GIS parser for URL: {url}")
        self._url = url
        
        try:
            logger.info(f"Navigating to URL: {url}")
            self.driver.navigate(url)
            logger.info("Page navigated successfully")
            
            logger.info("Injecting XHR counter script")
            self.driver.execute_script(self._add_xhr_counter_script())
            logger.info("XHR counter script injected")
        except Exception as e:
            logger.error(f"Error during initial navigation: {e}", exc_info=True)
            raise
        
        card_data_list = []
        aggregated_info = {
            'search_query_name': url.split('/search/')[1].split('?')[0].replace('+',
                                                                                ' ') if '/search/' in url else "2gisSearch",
            'total_cards_found': 0,
            'aggregated_rating': 0.0,
            'aggregated_reviews_count': 0,
            'aggregated_positive_reviews': 0,
            'aggregated_negative_reviews': 0,
            'aggregated_answered_reviews_count': 0,
            'aggregated_unanswered_reviews_count': 0,
            'aggregated_avg_response_time': 0.0,
        }
        
        def _update_aggregated_data(card_data: Dict[str, Any]) -> None:
            """Обновляет агрегированные данные на основе данных карточки"""
            try:
                # Рейтинг (среднее арифметическое)
                rating_str = card_data.get('card_rating', '')
                if rating_str:
                    try:
                        rating_value = float(rating_str)
                        if aggregated_info['aggregated_rating'] == 0.0:
                            aggregated_info['aggregated_rating'] = rating_value
                        else:
                            # Вычисляем среднее арифметическое всех рейтингов
                            total_cards = aggregated_info['total_cards_found']
                            if total_cards > 0:
                                current_sum = aggregated_info['aggregated_rating'] * total_cards
                                new_sum = current_sum + rating_value
                                aggregated_info['aggregated_rating'] = new_sum / (total_cards + 1)
                    except (ValueError, TypeError):
                        pass
                
                # Количество отзывов
                reviews_count = card_data.get('card_reviews_count', 0)
                if isinstance(reviews_count, (int, float)):
                    aggregated_info['aggregated_reviews_count'] += int(reviews_count)
                
                # Позитивные отзывы
                positive = card_data.get('card_reviews_positive', 0)
                if isinstance(positive, (int, float)):
                    aggregated_info['aggregated_positive_reviews'] += int(positive)
                
                # Негативные отзывы
                negative = card_data.get('card_reviews_negative', 0)
                if isinstance(negative, (int, float)):
                    aggregated_info['aggregated_negative_reviews'] += int(negative)
                
                # Отвеченные/неотвеченные отзывы
                detailed_reviews = card_data.get('detailed_reviews', [])
                answered_count = sum(1 for r in detailed_reviews if r.get('has_response', False))
                unanswered_count = len(detailed_reviews) - answered_count
                aggregated_info['aggregated_answered_reviews_count'] += answered_count
                aggregated_info['aggregated_unanswered_reviews_count'] += unanswered_count
                
                # Среднее время ответа (в днях, потом конвертируем в месяцы)
                avg_response_time = card_data.get('card_avg_response_time', '')
                if avg_response_time and isinstance(avg_response_time, (int, float)) and avg_response_time > 0:
                    # Собираем все времена ответа для вычисления общего среднего
                    if 'response_times_list' not in aggregated_info:
                        aggregated_info['response_times_list'] = []
                    # Добавляем время ответа этой карточки (если есть несколько отзывов с ответами)
                    if detailed_reviews:
                        for review in detailed_reviews:
                            if review.get('has_response') and review.get('review_date') and review.get('response_date'):
                                try:
                                    review_date_str = review.get('review_date')
                                    response_date_str = review.get('response_date')
                                    
                                    if ' ' in review_date_str:
                                        review_date = datetime.strptime(review_date_str, '%Y-%m-%d %H:%M:%S')
                                    else:
                                        review_date = datetime.strptime(review_date_str, '%Y-%m-%d')
                                    
                                    if ' ' in response_date_str:
                                        response_date = datetime.strptime(response_date_str, '%Y-%m-%d %H:%M:%S')
                                    else:
                                        response_date = datetime.strptime(response_date_str, '%Y-%m-%d')
                                    
                                    time_diff = response_date - review_date
                                    if time_diff.total_seconds() > 0:
                                        days = time_diff.days + (time_diff.seconds / 86400.0)
                                        aggregated_info['response_times_list'].append(days)
                                except Exception:
                                    pass
            except Exception as e:
                logger.warning(f"Error updating aggregated data: {e}")
        
        try:
            logger.info("Waiting for requests to finish...")
            self._wait_requests_finished()
            logger.info("Requests finished")
            
            # Собираем все URL карточек со всех страниц
            all_card_urls = set()
            processed_pages = set()
            current_page_url = url
            
            # ШАГ 1: Обрабатываем первую страницу и находим все страницы пагинации
            logger.info("Step 1: Processing first page and finding pagination...")
            self.driver.navigate(current_page_url)
            self._wait_requests_finished()
            time.sleep(2)
            
            # Прокручиваем первую страницу до конца
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
            
            # Собираем карточки с первой страницы
            page_source, soup = self._get_page_source_and_soup()
            first_page_urls = self._get_links()
            all_card_urls.update(first_page_urls)
            logger.info(f"✓ Collected {len(first_page_urls)} card URLs from page 1. Total so far: {len(all_card_urls)}")
            processed_pages.add(current_page_url)
            
            # Находим все ссылки на страницы пагинации
            # Прокручиваем вниз, чтобы увидеть кнопки пагинации
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            page_source, soup = self._get_page_source_and_soup()
            pagination_urls = self._get_pagination_links(soup, current_page_url)
            
            if pagination_urls:
                logger.info(f"✓ Found {len(pagination_urls)} additional pagination pages")
            else:
                logger.info("No pagination found - only one page of results")
            
            # ШАГ 2: Обрабатываем каждую страницу пагинации
            for page_num, page_url in enumerate(pagination_urls, start=2):
                if len(all_card_urls) >= self._max_records:
                    logger.info(f"Reached max records limit ({self._max_records}). Stopping pagination.")
                    break
                
                if page_url in processed_pages:
                    logger.info(f"Page {page_num} already processed, skipping...")
                    continue
                
                logger.info(f"Step 2.{page_num}: Processing page {page_num}...")
                try:
                    self.driver.navigate(page_url)
                    self._wait_requests_finished()
                    time.sleep(2)
                    
                    # Прокручиваем страницу до конца
                    page_source, soup = self._get_page_source_and_soup()
                    page_card_count = self._scroll_to_load_all_cards()
                    logger.info(f"Scroll completed for page {page_num}. Found {page_card_count} cards.")
                    time.sleep(3)
                    
                    # Собираем карточки с этой страницы
                    page_source, soup = self._get_page_source_and_soup()
                    page_urls = self._get_links()
                    all_card_urls.update(page_urls)
                    logger.info(f"✓ Collected {len(page_urls)} card URLs from page {page_num}. Total so far: {len(all_card_urls)}")
                    processed_pages.add(page_url)
                    
                except Exception as e:
                    logger.error(f"Error processing page {page_num} ({page_url}): {e}", exc_info=True)
                    continue
            
            # Преобразуем в список и сортируем для консистентности
            card_urls = list(all_card_urls)
            logger.info(f"✓ Total collected {len(card_urls)} unique card URLs from {len(processed_pages)} pages")
            
            if not card_urls:
                logger.warning(f"No firm/station links found on any page for URL: {url}.")
                return {
                    'cards': [],
                    'aggregated_info': aggregated_info
                }
            
            # ШАГ 3: Парсим все карточки по очереди БЕЗ возврата на страницу поиска
            logger.info(f"Step 3: Starting to parse {len(card_urls)} cards (max: {self._max_records})...")
            processed_urls = set()
            for card_url in card_urls:
                if len(card_data_list) >= self._max_records:
                    logger.info(f"Reached max records limit ({self._max_records}). Processed {len(card_data_list)} cards total.")
                    break
                try:
                    if not card_url or card_url in processed_urls:
                        continue
                    if not card_url.startswith('http'):
                        card_url = urllib.parse.urljoin("https://2gis.ru", card_url)
                    if not re.match(r'.*/(firm|station)/.*', card_url):
                        continue
                    processed_urls.add(card_url)
                    cards_processed = len(card_data_list)
                    logger.info(f"Parsing card {cards_processed + 1}/{len(card_urls)}: {card_url}")
                    self.driver.navigate(card_url)
                    self._wait_requests_finished(timeout=20)
                    time.sleep(2)  # Дополнительное ожидание для загрузки страницы
                    response = self.driver.wait_response(r'https://catalog\.api\.2gis\..*/items/byid', timeout=15)
                    parsed_card_data = None
                    
                    if response:
                        response_body = self.driver.get_response_body(response)
                        try:
                            item_data_dict = json.loads(response_body)
                            parsed_card_data = self._get_item_data_from_response(item_data_dict, card_url)
                        except json.JSONDecodeError:
                            logger.warning(f"Could not decode JSON from API response for {card_url}")
                        except Exception as e:
                            logger.error(f"Error processing API response data for {card_url}: {e}")
                    else:
                        logger.warning(f"No API response found for card URL: {card_url}, trying HTML parsing only")
                    
                    # Если не получили данные из API, пробуем парсить только HTML
                    if not parsed_card_data:
                        try:
                            page_source, soup = self._get_page_source_and_soup()
                            # Парсим базовые данные из HTML
                            name_elem = soup.select_one('[class*="name"], [data-test="name"], h1')
                            name = name_elem.get_text(strip=True) if name_elem else ""
                            address = self._extract_address_from_page(soup)
                            phone = self._extract_phone_from_page(soup)
                            reviews_data = self._get_card_reviews_info()
                            
                            if name:
                                detailed_reviews_list = reviews_data.get('details', [])
                                answered_reviews_count = sum(1 for r in detailed_reviews_list if r.get('has_response', False))
                                unanswered_reviews_count = len(detailed_reviews_list) - answered_reviews_count
                                
                                # Конвертируем среднее время ответа в месяцы
                                avg_response_time_months = ""
                                avg_response_time_days = reviews_data.get('avg_response_time_days', 0)
                                if avg_response_time_days and isinstance(avg_response_time_days, (int, float)) and avg_response_time_days > 0:
                                    avg_response_time_months = round(avg_response_time_days / 30.0, 2)
                                
                                parsed_card_data = {
                                    'card_name': name,
                                    'card_address': address,
                                    'card_rating': "",
                                    'card_reviews_count': reviews_data.get('reviews_count', 0),
                                    'card_website': "",
                                    'card_phone': phone,
                                    'card_rubrics': "",
                                    'card_response_status': 'YES' if answered_reviews_count > 0 else 'NO',
                                    'card_answered_reviews_count': answered_reviews_count,
                                    'card_unanswered_reviews_count': unanswered_reviews_count,
                                    'card_avg_response_time': avg_response_time_months if avg_response_time_months else "",
                                    'card_reviews_positive': reviews_data.get('positive_reviews', 0),
                                    'card_reviews_negative': reviews_data.get('negative_reviews', 0),
                                    'card_reviews_texts': "",
                                    'detailed_reviews': detailed_reviews_list,
                                }
                        except Exception as e:
                            logger.error(f"Error parsing HTML for card {card_url}: {e}")
                    
                    if parsed_card_data:
                        card_data_list.append(parsed_card_data)
                        _update_aggregated_data(parsed_card_data)
                        logger.info(f"✓ Successfully processed card {len(card_data_list)}/{len(card_urls)}: {parsed_card_data.get('card_name', 'Unknown')}")
                    else:
                        logger.warning(f"Could not extract data from card: {card_url}")
                except Exception as e:
                    logger.error(f"Error processing card element with URL {card_url}: {e}", exc_info=True)
                    continue
            
            logger.info(f"✓ Completed parsing. Processed {len(card_data_list)}/{len(card_urls)} cards successfully.")
            
            aggregated_info['total_cards_found'] = len(card_data_list)
            
            # Вычисляем среднее время ответа из всех собранных времен
            if 'response_times_list' in aggregated_info and aggregated_info['response_times_list']:
                avg_response_time_days = sum(aggregated_info['response_times_list']) / len(aggregated_info['response_times_list'])
                # Конвертируем в месяцы для 2GIS (1 месяц = 30 дней)
                avg_response_time_months = avg_response_time_days / 30.0
                aggregated_info['aggregated_avg_response_time'] = round(avg_response_time_months, 2)
                del aggregated_info['response_times_list']  # Удаляем временный список
            else:
                aggregated_info['aggregated_avg_response_time'] = 0.0
            
            # Округляем агрегированные значения
            if aggregated_info['aggregated_rating'] > 0:
                aggregated_info['aggregated_rating'] = round(aggregated_info['aggregated_rating'], 2)
        except Exception as e:
            logger.error(f"Error during 2GIS parsing for URL {url}: {e}", exc_info=True)
        return {'aggregated_info': aggregated_info, 'cards_data': card_data_list}
