from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Dict, List, Optional, Union, Tuple

from dotenv import load_dotenv, find_dotenv
from pydantic import ValidationError

from .models import AppConfig, ChromeOptions, ParserOptions, WriterOptions, LogOptions


def load_settings(
        config_file: Optional[str] = None,
        env_file: Optional[str] = None
) -> AppConfig:
    project_root = pathlib.Path(__file__).resolve().parent.parent.parent

    if config_file is None:
        config_file = project_root / "config.json"
    if env_file is None:
        env_file = project_root / ".env"

    settings_data: Dict[str, Any] = {}

    try:
        load_dotenv(dotenv_path=env_file)
    except Exception as e:
        print(f"Could not load .env file from {env_file}: {e}")

    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                settings_data = json.load(f)
        except FileNotFoundError:
            print(f"Config file not found at {config_file}. Using default settings.")
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from config file: {config_file}")
            settings_data = {}
        except Exception as e:
            print(f"Error reading config file {config_file}: {e}")
            settings_data = {}
    else:
        print(f"Config file not found at {config_file}. Using default settings.")

    try:
        app_config = AppConfig.model_validate(settings_data)
        return app_config
    except ValidationError as e:
        print(f"Settings validation error: {e}")
        raise


try:
    settings = load_settings()
except Exception as e:
    print(f"Failed to initialize settings: {e}")
    settings = AppConfig()
    print("Using default settings due to initialization error.")
