from __future__ import annotations

from pydantic import BaseModel
from typing import Literal, Optional

class ChromeSettings(BaseModel):
    headless: bool = False
    from __future__ import annotations
    from typing import Literal
    from pydantic import BaseModel, Field
    import pathlib

    class ChromeSettings(BaseModel):
        headless: bool = False
        chromedriver_path: str = "C:/Users/lexxd/Downloads/chromedriver_win32/chromedriver.exe"  # Обновленный путь
        silent_browser: bool = True

    class ParserOptions(BaseModel):
        retries: int = 3
        timeout: float = 10.0

    class WriterOptions(BaseModel):
        output_dir: str = "./output"
        format: Literal["csv", "json"] = "csv"

    class AppConfig(BaseModel):
        root_directory: str = "/"
        environment: Literal["development", "production"] = "development"
        log_level: Literal["debug", "info", "warning", "error"] = "info"

    class Settings(BaseModel):
        chrome: ChromeSettings = ChromeSettings()
        parser: ParserOptions = ParserOptions()
        writer: WriterOptions = WriterOptions()
        app_config: AppConfig = AppConfig()
        project_root: str = "."

    settings = Settings()