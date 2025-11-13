import sys
import os
import pathlib

print(f"Current working directory: {os.getcwd()}")
print(f"sys.path: {sys.path}")

try:
    print("Attempting to import src.config.settings...")
    from src.config.settings import settings
    print("Successfully imported src.config.settings!")
    print(f"Project root from settings: {settings.project_root}")
except ModuleNotFoundError as e:
    print(f"ModuleNotFoundError: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")