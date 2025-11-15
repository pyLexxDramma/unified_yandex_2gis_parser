import sys
import os
import pathlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S')
logger = logging.getLogger(__name__)

print(f"Current working directory: {os.getcwd()}")
print(f"sys.path: {sys.path}")

try:
    logger.info("Attempting to import src.config.settings...")
    from src.config.settings import settings

    logger.info("Successfully imported src.config.settings!")
    logger.info(f"Project root from settings: {settings.project_root}")
    logger.info(f"ChromeDriver path from settings: {settings.chrome.chromedriver_path}")
except ModuleNotFoundError as e:
    logger.error(f"ModuleNotFoundError: Could not import src.config.settings. {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred during import of src.config.settings: {e}", exc_info=True)

try:
    logger.info("Attempting to import src.drivers.selenium_driver...")
    from src.drivers.selenium_driver import SeleniumDriver

    logger.info("Successfully imported src.drivers.selenium_driver!")
except ModuleNotFoundError as e:
    logger.error(f"ModuleNotFoundError: Could not import src.drivers.selenium_driver. {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred during import of src.drivers.selenium_driver: {e}", exc_info=True)

try:
    logger.info("Attempting to import src.parsers.gis_parser...")
    from src.parsers.gis_parser import GisParser

    logger.info("Successfully imported src.parsers.gis_parser!")
except ModuleNotFoundError as e:
    logger.error(f"ModuleNotFoundError: Could not import src.parsers.gis_parser. {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred during import of src.parsers.gis_parser: {e}", exc_info=True)

try:
    logger.info("Attempting to import src.parsers.yandex_parser...")
    from src.parsers.yandex_parser import YandexParser

    logger.info("Successfully imported src.parsers.yandex_parser!")
except ModuleNotFoundError as e:
    logger.error(f"ModuleNotFoundError: Could not import src.parsers.yandex_parser. {e}")
except Exception as e:
    logger.error(f"An unexpected error occurred during import of src.parsers.yandex_parser: {e}", exc_info=True)
