from __future__ import annotations
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from selenium.webdriver import Chrome, ChromeOptions as SeleniumChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.remote.webelement import WebElement

from src.drivers.base_driver import BaseDriver
from src.config.settings import Settings

logger = logging.getLogger(__name__)

from urllib.parse import urlparse
import tempfile


def extract_credentials_from_proxy_url(proxy_url: str) -> tuple:
    """Извлекает логин и пароль из URL прокси."""
    parsed_url = urlparse(proxy_url)
    if '@' in parsed_url.netloc:
        credentials = parsed_url.netloc.split('@')[0]
        if ':' in credentials:
            username, password = credentials.split(':', 1)
            return username, password
    return None, None


def create_proxy_auth_extension(proxy_host: str, proxy_port: int, username: str, password: str) -> str:
    """Создает расширение Chrome для прокси с аутентификацией."""
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = """
    var config = {
            mode: "fixed_servers",
            rules: {
              singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
              },
              bypassList: ["localhost"]
            }
          };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """ % (proxy_host, proxy_port, username, password)

    # Создаем временную директорию для расширения
    temp_dir = tempfile.mkdtemp()
    extension_dir = os.path.join(temp_dir, "proxy_auth_extension")
    os.makedirs(extension_dir, exist_ok=True)
    
    # Записываем файлы расширения
    manifest_path = os.path.join(extension_dir, "manifest.json")
    background_path = os.path.join(extension_dir, "background.js")
    
    with open(manifest_path, 'w', encoding='utf-8') as f:
        f.write(manifest_json)
    
    with open(background_path, 'w', encoding='utf-8') as f:
        f.write(background_js)
    
    logger.info(f"Proxy auth extension created at: {extension_dir}")
    return extension_dir


class SeleniumTab:
    def __init__(self, driver: "SeleniumDriver"):
        self._driver = driver
        self._default_timeout = 10

    def set_default_timeout(self, timeout: int):
        self._default_timeout = timeout

    def wait_for_element(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> Optional[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            if not self._driver or not self._driver.driver:
                logger.error("WebDriver not initialized for wait_for_element.")
                return None
            return WebDriverWait(self._driver.driver, wait_timeout).until(
                EC.presence_of_element_located(locator)
            )
        except TimeoutException:
            logger.warning(f"Timeout waiting for element {locator}.")
            return None
        except WebDriverException as e:
            logger.error(f"WebDriverException in wait_for_element with {locator}: {e}", exc_info=True)
            return None

    def wait_for_elements(self, locator: Tuple[str, str], timeout: Optional[int] = None) -> List[WebElement]:
        try:
            wait_timeout = timeout if timeout is not None else self._default_timeout
            if not self._driver or not self._driver.driver:
                logger.error("WebDriver not initialized for wait_for_elements.")
                return []
            WebDriverWait(self._driver.driver, wait_timeout).until(
                EC.presence_of_all_elements_located(locator)
            )
            return self._driver.driver.find_elements(*locator)
        except TimeoutException:
            logger.warning(f"Timeout waiting for elements {locator}.")
            return []
        except WebDriverException as e:
            logger.error(f"WebDriverException in wait_for_elements with {locator}: {e}", exc_info=True)
            return []


class SeleniumDriver(BaseDriver):
    def __init__(self, settings: Settings, proxy: Optional[str] = None):
        self.settings = settings
        self.proxy = proxy
        self.driver: Optional[Chrome] = None
        self._tab: Optional[SeleniumTab] = None
        self._is_running = False
        self.current_url: Optional[str] = None

        self._tab = SeleniumTab(self)

    def _initialize_driver(self):
        import sys
        options = SeleniumChromeOptions()
        
        # Настройка headless режима
        if self.settings.chrome.headless:
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            logger.info("Chrome running in headless mode.")
        else:
            logger.info("Chrome running in visible mode (headless=false).")
        
        # Важно: отключаем обнаружение автоматизации для более естественного поведения
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Устанавливаем реальный User-Agent
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Настройки для тихого режима (только логирование, не влияет на видимость окна или JavaScript)
        # Важно: silent_browser не должен отключать JavaScript!
        if self.settings.chrome.silent_browser:
            options.add_argument("--log-level=3")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            logger.info("Chrome running in silent mode (reduced logging, JavaScript still enabled).")
        else:
            logger.info("Chrome running in verbose mode (full logging enabled).")
        
        # Максимизация окна, если указано
        if self.settings.chrome.start_maximized and not self.settings.chrome.headless:
            options.add_argument("--start-maximized")
            logger.info("Chrome will start maximized.")

        if self.settings.chrome.chromedriver_path:
            os.environ["webdriver.chrome.driver"] = self.settings.chrome.chromedriver_path
        else:
            pass

        if self.proxy:
            # Проверяем, есть ли логин и пароль в URL прокси
            parsed_proxy = urlparse(self.proxy)
            username, password = extract_credentials_from_proxy_url(self.proxy)
            
            if username and password:
                # Прокси с аутентификацией - используем расширение
                logger.info(f"Using proxy with authentication: {parsed_proxy.hostname}:{parsed_proxy.port}")
                try:
                    proxy_host = parsed_proxy.hostname
                    proxy_port = parsed_proxy.port or (8080 if parsed_proxy.scheme == 'http' else 443)
                    extension_dir = create_proxy_auth_extension(proxy_host, proxy_port, username, password)
                    options.add_argument(f"--load-extension={extension_dir}")
                    logger.info(f"Proxy auth extension loaded from: {extension_dir}")
                    # НЕ добавляем --proxy-server при использовании расширения, оно само настроит прокси
                except Exception as e:
                    logger.error(f"Failed to create proxy auth extension: {e}. Trying without auth...")
                    # Fallback: пробуем без аутентификации
                    proxy_host = parsed_proxy.hostname
                    proxy_port = parsed_proxy.port or (8080 if parsed_proxy.scheme == 'http' else 443)
                    options.add_argument(f"--proxy-server={parsed_proxy.scheme}://{proxy_host}:{proxy_port}")
            else:
                # Прокси без аутентификации
                logger.info(f"Using proxy without authentication: {self.proxy}")
                options.add_argument(f"--proxy-server={self.proxy}")
        else:
            # Explicitly disable proxy to avoid system proxy settings
            options.add_argument("--no-proxy-server")
            options.add_argument("--proxy-bypass-list=*")
            # Disable proxy auto-detection
            options.add_argument("--disable-proxy-certificate-handler")
            # Additional options to prevent proxy usage
            options.add_argument("--disable-extensions")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)

        try:
            # Всегда используем ChromeDriverManager для автоматического подбора версии
            # Это гарантирует совместимость с установленной версией Chrome
            logger.info("Using ChromeDriverManager to automatically download compatible ChromeDriver...")
            logger.info("This may take a moment to download ChromeDriver if needed...")
            sys.stdout.flush()
            chromedriver_path = ChromeDriverManager().install()
            logger.info(f"ChromeDriverManager downloaded/verified ChromeDriver at: {chromedriver_path}")
            service = Service(chromedriver_path)
            logger.info("ChromeDriverManager setup completed.")
            
            logger.info("Creating Chrome WebDriver instance...")
            service_path = service.executable_path if hasattr(service, 'executable_path') else getattr(service, 'path', 'N/A')
            logger.info(f"Service executable path: {service_path}")
            logger.info(f"Service executable exists: {os.path.exists(service_path) if service_path != 'N/A' else 'N/A'}")
            
            # Проверяем, что ChromeDriver файл существует и доступен
            if service_path != 'N/A' and not os.path.exists(service_path):
                raise FileNotFoundError(f"ChromeDriver executable not found at: {service_path}")
            
            try:
                # Пробуем создать драйвер с таймаутом через threading
                import threading
                import sys
                
                driver_created = threading.Event()
                driver_result = [None]
                error_result = [None]
                
                def create_driver_thread():
                    try:
                        logger.info("Thread: Starting Chrome() call...")
                        sys.stdout.flush()
                        driver_result[0] = Chrome(service=service, options=options)
                        logger.info("Thread: Chrome() call completed.")
                        sys.stdout.flush()
                    except Exception as e:
                        error_result[0] = e
                        logger.error(f"Thread: Error in Chrome() call: {e}", exc_info=True)
                    finally:
                        driver_created.set()
                
                logger.info("Calling Chrome(service=service, options=options)...")
                logger.info("This may take a few seconds...")
                sys.stdout.flush()
                
                thread = threading.Thread(target=create_driver_thread, daemon=True)
                thread.start()
                
                # Ждем максимум 60 секунд
                if driver_created.wait(timeout=60):
                    if error_result[0]:
                        raise error_result[0]
                    self.driver = driver_result[0]
                    if not self.driver:
                        raise Exception("Chrome WebDriver instance is None after creation")
                    logger.info("Chrome() call completed successfully.")
                else:
                    raise TimeoutException("Chrome WebDriver creation timed out after 60 seconds. Chrome may be blocked or not responding.")
            except WebDriverException as e:
                logger.error(f"WebDriverException during Chrome creation: {e}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"Unexpected error during Chrome creation: {e}", exc_info=True)
                raise
            
            if not self.driver:
                raise Exception("Chrome WebDriver instance is None after creation")
            
            logger.info("Chrome WebDriver instance created successfully.")
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(5)
            
            # Максимизация окна программно, если не через аргумент
            if self.settings.chrome.start_maximized and not self.settings.chrome.headless:
                try:
                    self.driver.maximize_window()
                    logger.info("Chrome window maximized.")
                except Exception as e:
                    logger.warning(f"Could not maximize window: {e}")
            
            logger.info("Selenium WebDriver instance created.")
        except WebDriverException as e:
            logger.error(f"WebDriverException during initialization: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"General error during WebDriver initialization: {e}", exc_info=True)
            raise

    def start(self) -> None:
        if not self._is_running:
            try:
                logger.info("=" * 60)
                logger.info("Starting SeleniumDriver initialization...")
                logger.info(f"ChromeDriver path from config: {self.settings.chrome.chromedriver_path}")
                logger.info(f"Headless mode: {self.settings.chrome.headless}")
                logger.info("=" * 60)
                self._initialize_driver()
                self._is_running = True
                logger.info("SeleniumDriver started successfully.")
            except Exception as e:
                logger.error(f"Error starting SeleniumDriver: {e}", exc_info=True)
                raise
        else:
            logger.warning("SeleniumDriver is already running.")

    def stop(self) -> None:
        if self._is_running and self.driver:
            try:
                self.driver.quit()
                self._is_running = False
                self.driver = None
                self.current_url = None
                logger.info("SeleniumDriver stopped.")
            except WebDriverException as e:
                logger.error(f"WebDriverException during stop: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error stopping SeleniumDriver: {e}", exc_info=True)
        elif not self._is_running:
            logger.warning("SeleniumDriver is not running.")
        else:
            logger.warning("SeleniumDriver state is inconsistent (running but driver is None).")

    def navigate(self, url: str, referer: Optional[str] = None, timeout: int = 60) -> None:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        
        try:
            self.driver.get(url)
            self.current_url = self.driver.current_url
            logger.info(f"Navigated to: {url}")
        except WebDriverException as e:
            error_msg = str(e).lower()
            # Проверяем, связана ли ошибка с прокси
            if 'proxy' in error_msg or 'err_no_supported_proxies' in error_msg or 'net::err_proxy' in error_msg:
                logger.error(f"❌ Proxy error while navigating to {url}: {e}")
                logger.warning("⚠️  Proxy appears to be unavailable or misconfigured. Consider disabling proxy.")
                if self.proxy:
                    logger.warning(f"Current proxy: {self.proxy}")
            else:
                logger.error(f"WebDriverException navigating to {url}: {e}", exc_info=True)
            if self.driver: 
                try:
                    self.current_url = self.driver.current_url
                except:
                    pass
            raise

    def get_page_source(self) -> str:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            return self.driver.page_source
        except WebDriverException as e:
            logger.error(f"WebDriverException getting page source: {e}", exc_info=True)
            return ""

    def execute_script(self, script: str, *args) -> Any:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            return self.driver.execute_script(script, *args)
        except WebDriverException as e:
            error_msg = str(e).lower()
            # Проверяем, связана ли ошибка с потерянной сессией
            if 'invalid session id' in error_msg or 'session' in error_msg:
                logger.error(f"❌ Browser session lost while executing script: {e}")
                logger.warning("⚠️  Browser session is invalid. Driver may need to be restarted.")
                # Не возвращаем None для числовых операций - возвращаем 0
                if 'return' in script.lower() and ('scroll' in script.lower() or 'height' in script.lower() or 'offset' in script.lower()):
                    logger.debug(f"Returning 0 for scroll/height script due to lost session")
                    return 0
            else:
                logger.error(f"WebDriverException executing script: {e}", exc_info=True)
            return None

    def perform_click(self, element: Any) -> None:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")

        locator = None
        web_element = None

        if isinstance(element, WebElement):
            web_element = element
        elif isinstance(element, dict):
            locator_type = element.get('locator_type', 'xpath')
            locator_value = element.get('locator_value', '')
            if locator_value:
                locator = (locator_type, locator_value)
                web_element = self.tab.wait_for_element(locator)
            else:
                logger.warning(f"Locator value is empty for element dict: {element}")
        else:
            logger.warning(f"Unsupported element type for click: {type(element)}. Element: {element}")
            return

        if web_element:
            try:
                web_element.click()
                logger.info(f"Clicked element (locator: {locator if locator else 'WebElement'}).")
            except WebDriverException as e:
                logger.error(
                    f"WebDriverException clicking element (locator: {locator if locator else 'WebElement'}): {e}",
                    exc_info=True)
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", web_element)
                    web_element.click()
                    logger.info(
                        f"Clicked element after scrollIntoView (locator: {locator if locator else 'WebElement'}).")
                except WebDriverException as e_scroll:
                    logger.error(f"WebDriverException clicking element after scrollIntoView: {e_scroll}", exc_info=True)
        else:
            logger.warning(f"Could not find or click element. Locator: {locator}. Element data: {element}")

    def wait_for_url(self, url_pattern: str, timeout: int = 30) -> bool:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            WebDriverWait(self.driver, timeout).until(EC.url_contains(url_pattern))
            self.current_url = self.driver.current_url
            logger.info(f"URL contains '{url_pattern}' found. Current URL: {self.current_url}")
            return True
        except TimeoutException:
            self.current_url = self.driver.current_url
            logger.warning(f"Timeout waiting for URL containing '{url_pattern}'. Current URL: {self.current_url}")
            return False
        except WebDriverException as e:
            logger.error(f"WebDriverException waiting for URL containing '{url_pattern}': {e}", exc_info=True)
            return False

    def wait_for_element(self, locator: Tuple[str, str], timeout: int = 30) -> Any:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        return self.tab.wait_for_element(locator, timeout)

    def get_element_by_locator(self, locator: Tuple[str, str]) -> Any:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            return self.tab.wait_for_element(locator)
        except Exception as e:
            logger.error(f"Error getting element by locator {locator}: {e}", exc_info=True)
            return None

    def get_elements_by_locator(self, locator: Tuple[str, str]) -> List[Any]:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        return self.tab.wait_for_elements(locator)

    def get_responses(self, url_pattern: Optional[str] = None, timeout: int = 10) -> List[Dict[str, Any]]:
        logger.warning("get_responses is not fully implemented. Returning empty list.")
        return []

    def wait_response(self, url_pattern: str, timeout: int = 10) -> Optional[Dict[str, Any]]:
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")

        script = f"""
        var callback = arguments[arguments.length - 1];
        var urlRegex = new RegExp("{url_pattern}");
        var originalXhrOpen = XMLHttpRequest.prototype.open;

        XMLHttpRequest.prototype.open = function(method, url, async, user, pass) {{
            if (urlRegex.test(url)) {{
                var xhr = this;
                xhr.addEventListener('load', function() {{
                    if (xhr.readyState === 4) {{
                        callback({{
                            url: url,
                            responseBody: xhr.responseText,
                            status: xhr.status
                        }});
                    }}
                }});
                 xhr.addEventListener('error', function() {{
                    callback({{
                        url: url,
                        responseBody: null,
                        status: 'error'
                    }});
                 }});
            }}
            originalXhrOpen.apply(this, arguments);
        }};
        """
        try:
            response_data = self.driver.execute_script(script)
            if response_data:
                logger.info(f"Response captured for URL pattern '{url_pattern}'.")
                return response_data
            else:
                logger.warning(
                    f"No response captured via execute_script for URL pattern '{url_pattern}' within script execution time.")
                return None
        except WebDriverException as e:
            logger.error(f"WebDriverException during wait_response for '{url_pattern}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"General error during wait_response for '{url_pattern}': {e}", exc_info=True)
            return None

    def get_response_body(self, response: Any) -> str:
        if isinstance(response, dict) and 'responseBody' in response:
            return response['responseBody']
        return ""

    def get_current_url(self) -> Optional[str]:
        if self._is_running and self.driver:
            self.current_url = self.driver.current_url
        return self.current_url

    def add_blocked_requests(self, requests: List[str]):
        logger.warning("add_blocked_requests not implemented.")
        pass

    def add_start_script(self, script: str):
        if self._is_running and self.driver:
            try:
                self.driver.execute_script(script)
                logger.info("Start script executed.")
            except WebDriverException as e:
                logger.error(f"WebDriverException executing start script: {e}", exc_info=True)
        else:
            logger.warning("WebDriver not running or not initialized for add_start_script.")
        pass

    def clear_requests(self):
        logger.warning("clear_requests not implemented.")
        pass

    def set_default_timeout(self, timeout: int):
        if not self._is_running or not self.driver:
            raise RuntimeError(f"{self.__class__.__name__} is not running or driver not initialized.")
        try:
            self.driver.set_page_load_timeout(timeout)
            logger.info(f"Default timeout set to {timeout} seconds.")
        except WebDriverException as e:
            logger.error(f"WebDriverException setting default timeout: {e}", exc_info=True)
            raise

    @property
    def tab(self) -> SeleniumTab:
        if self._tab is None:
            self._tab = SeleniumTab(self)
        return self._tab
