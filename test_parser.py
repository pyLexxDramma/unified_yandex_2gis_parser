import asyncio
import logging

from src.config.settings import Settings
from src.config.models import AppConfig
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser
from src.parsers.yandex_parser import YandexParser
from src.storage.csv_writer import CSVWriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_parser(parser_name: str, url: str, output_filename: str):
    settings = Settings()
    app_config = AppConfig()

    driver = None
    writer = None
    try:
        driver = SeleniumDriver(settings)

        if parser_name == "gis":
            parser = GisParser(driver, app_config)
        elif parser_name == "yandex":
            parser = YandexParser(driver, app_config)
        else:
            raise ValueError(f"Unknown parser: {parser_name}")

        writer = CSVWriter(app_config)
        writer._output_filename = output_filename

        logger.info(f"Starting manual test for {parser_name} with URL: {url}")

        with writer:
            parsed_data = await parser.parse(url)  # Assuming parse can be awaited or adapted if async
            if parsed_data:
                for record in parsed_data:
                    writer.write(record)
            logger.info(f"Manual test completed for {parser_name}. Data saved to {writer._file_path}")

    except Exception as e:
        logger.error(f"Error during manual parser test: {e}", exc_info=True)
    finally:
        if driver:
            driver.close()


if __name__ == "__main__":

    print("Uncomment the desired test case in test_parser.py and run it.")