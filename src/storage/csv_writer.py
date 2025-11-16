from __future__ import annotations
import csv
import logging
import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from src.storage.file_writer import FileWriter, FileWriterOptions
from src.config.settings import AppConfig

logger = logging.getLogger(__name__)


class CSVOptions(BaseModel):
    add_rubrics: bool = True
    add_comments: bool = True
    columns_per_entity: int = Field(3, gt=0, le=5)
    remove_empty_columns: bool = True
    remove_duplicates: bool = True
    join_char: str = '; '
    output_filename: str = 'output.csv'


class CSVWriter(FileWriter):
    def __init__(self, settings: AppConfig):
        writer_opts = settings.app_config.writer
        csv_opts = writer_opts.csv

        file_writer_options = FileWriterOptions(
            encoding=writer_opts.encoding,
            verbose=writer_opts.verbose,
            format=writer_opts.format,
            output_dir=writer_opts.output_dir
        )
        super().__init__(options=file_writer_options)
        self.csv_options = csv_opts
        self._fieldnames: Optional[List[str]] = None
        self._header_written: bool = False

    def set_file_path(self, file_path: str):
        self._file_path = file_path

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

        try:
            self.file_handle = open(self._file_path, 'w', newline='', encoding=self._options.encoding)
            self.writer = csv.writer(self.file_handle)
            logger.info(f"CSV file opened for writing: {self._file_path} with encoding {self._options.encoding}")
        except Exception as e:
            logger.error(f"Error opening CSV file {self._file_path}: {e}", exc_info=True)
            raise

    def close(self):
        if hasattr(self, 'file_handle') and self.file_handle:
            self.file_handle.close()
            logger.info(f"CSV file closed. Wrote {self._wrote_count} records.")

    def write(self, data: Dict[str, Any]):
        if not self.writer:
            logger.error("CSV writer not initialized. Call open() or ensure proper initialization.")
            return

        if self._fieldnames is None:
            self._fieldnames = list(data.keys())
            if not self._header_written:
                self.writer.writerow(self._fieldnames)
                self._header_written = True

        row = [data.get(field) for field in self._fieldnames]
        self.writer.writerow(row)
        self._wrote_count += 1
