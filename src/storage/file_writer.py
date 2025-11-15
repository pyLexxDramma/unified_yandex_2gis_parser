from __future__ import annotations
import abc
import logging
import os
from typing import Any, Optional

from src.config.settings import AppConfig

logger = logging.getLogger(__name__)


class FileWriter(abc.ABC):
    def __init__(self, settings: AppConfig):
        self.settings = settings
        self._file: Optional[Any] = None
        self._file_path: Optional[str] = None
        self._wrote_count: int = 0
        self.writer_options = settings.writer
        self._output_filename = self.get_output_filename()
        if not self._output_filename:
            raise ValueError("Output filename is not specified in configuration.")
        project_root = getattr(settings, 'project_root', os.getcwd())
        self._file_path = os.path.join(project_root, self._output_filename)
        output_dir = os.path.dirname(self._file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
        if os.path.exists(self._file_path):
            logger.warning(f"Output file already exists and will be overwritten: {self._file_path}")

    @staticmethod
    def get_output_filename() -> str:
        raise NotImplementedError("Subclasses must implement get_output_filename()")

    @abc.abstractmethod
    def write(self, data: Any) -> None:
        pass

    def __enter__(self) -> 'FileWriter':
        self.encoding = self.writer_options.encoding
        try:
            self._file = open(self._file_path, 'w', encoding=self.encoding, newline='')
            self._wrote_count = 0
            logger.info(f"Opened file for writing: {self._file_path} with encoding {self.encoding}")
            return self
        except Exception as e:
            logger.error(f"Failed to open file {self._file_path}: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._file:
            self._file.close()
            logger.info(f"Closed file: {self._file_path}")

    def __repr__(self) -> str:
        classname = self.__class__.__name__
        return f"<{classname} path='{self._file_path}' wrote={self._wrote_count}>"

    def _open_file(self, path: str, mode: str, encoding: Optional[str] = None, newline: Optional[str] = None):
        if encoding is None:
            encoding = self.encoding
        if newline is None:
            newline = ''
        return open(path, mode, encoding=encoding, newline=newline)
