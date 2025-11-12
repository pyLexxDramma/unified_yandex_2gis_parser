from __future__ import annotations

import json
import os
import pathlib
from typing import Optional, Dict, Any

from dotenv import load_dotenv, find_dotenv
from pydantic import ValidationError
import psutil
from .models import AppConfig, ChromeOptions, ParserOptions, WriterOptions, LogOptions, _default_memory_limit, \
    _default_max_records


def get_project_root() -> pathlib.Path:
    """Find the project root directory."""
    current_path = pathlib.Path(__file__).resolve()
    for _ in range(5):
        if (current_path / '.git').exists() or \
                (current_path / 'config.json').exists() or \
                (current_path / '.env').exists():
            return current_path
        current_path = current_path.parent
    return pathlib.Path(__file__).resolve().parent


PROJECT_ROOT = get_project_root()
CONFIG_FILE_PATH = PROJECT_ROOT / "config.json"
ENV_FILE_PATH = PROJECT_ROOT / ".env"


class Settings:
    """Singleton class to manage application settings."""
    _instance: Optional[Settings] = None
    _config: Optional[AppConfig] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
            cls._load_settings()
        return cls._instance

    @classmethod
    def _load_settings(cls) -> None:
        """Load settings from .env, config.json and validate them."""
        env_path = find_dotenv()
        if env_path:
            load_dotenv(dotenv_path=env_path)
            print(f"Loaded .env from: {env_path}")
        else:
            print("No .env file found.")

        config_data: Dict[str, Any] = {}
        try:
            with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            print(f"Loaded config from: {CONFIG_FILE_PATH}")
        except FileNotFoundError:
            print(f"Configuration file not found at {CONFIG_FILE_PATH}. Using default settings.")
        except json.JSONDecodeError:
            print(f"Invalid JSON in configuration file {CONFIG_FILE_PATH}. Using default settings.")
        except Exception as e:
            print(f"An unexpected error occurred while loading config file: {e}. Using default settings.")

        try:
            cls._config = AppConfig(**config_data)
            cls._config.model_validate(cls._config)
            print("Configuration loaded and validated successfully.")
        except ValidationError as e:
            print(f"Configuration validation error: {e}. Using default settings.")
            cls._config = AppConfig()
        except Exception as e:
            print(f"An unexpected error occurred during configuration loading: {e}. Using default settings.")
            cls._config = AppConfig()

    @classmethod
    def get_config(cls) -> AppConfig:
        """Return the application configuration."""
        if cls._config is None:
            cls._load_settings()
        return cls._config

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        """Get a specific configuration value by key (e.g., 'chrome.headless')."""
        if cls._config is None:
            cls._load_settings()
            if cls._config is None:
                return default

        try:
            config_dict = cls._config.model_dump() if hasattr(cls._config, 'model_dump') else cls._config.dict()
            keys = key.split('.')
            value = config_dict
            for k in keys:
                value = value[k]
            return value
        except (KeyError, AttributeError, TypeError):
            return default


settings = Settings()
