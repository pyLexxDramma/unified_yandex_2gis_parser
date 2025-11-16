from __future__ import annotations
import logging
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class FileWriterOptions(BaseModel):
    encoding: str = 'utf-8-sig'
    verbose: bool = True
    format: str = "csv"
    output_dir: str = "./output"


class FileWriter(BaseModel):
    _options: FileWriterOptions
    _file_path: Optional[str] = None
    _wrote_count: int = 0

    def __init__(self, options: FileWriterOptions):
        self._options = options
        self._file_path = None
        self._wrote_count = 0

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if not self._file_path:
            raise ValueError("File path is not set. Use set_file_path() or ensure it's provided.")

        output_dir = os.path.dirname(self._file_path)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"Created output directory: {output_dir}")
            except OSError as e:
                logger.error(f"Failed to create output directory {output_dir}: {e}", exc_info=True)
                raise

        logger.info(f"Opening file for writing: {self._file_path} with encoding {self._options.encoding}")

    def close(self):
        logger.info(f"File writing closed. Wrote {self._wrote_count} records.")

    def set_file_path(self, file_path: str):
        self._file_path = file_path

    def write(self, data: Dict[str, Any]):
        raise NotImplementedError("Subclasses must implement the write method.")
