from __future__ import annotations

import csv
import os
import shutil
import re
import json
from typing import Any, Dict, List, Optional

from src.storage.file_writer import FileWriter
from src.config.settings import AppConfig


class CSVWriter(FileWriter):
    def __init__(self, settings: AppConfig):
        super().__init__(settings)
        self._file_path = os.path.join(self.settings.project_root, self.writer_options.csv.output_filename)
        if not self._file_path.lower().endswith('.csv'):
            self._file_path += '.csv'

        self.csv_options = self.writer_options.csv
        self._temp_data: List[Dict[str, Any]] = []
        self._data_mapping: Dict[str, Any] = {}
        self._complex_mapping: Dict[str, str] = {}

    @staticmethod
    def get_output_filename() -> str:
        return 'output.csv'

    def write(self, data: Dict[str, Any]) -> None:
        if not data:
            return

        if not self._data_mapping and not self._temp_data:
            self._data_mapping = {k: k for k in data.keys()}

        self._temp_data.append(data)
        self._wrote_count += 1

    def _process_data(self) -> List[Dict[str, Any]]:
        if not self._temp_data:
            return []

        initial_data_mapping = self._data_mapping.copy()

        if self.csv_options.remove_empty_columns:
            self._temp_data, self._data_mapping = self._remove_empty_columns(self._temp_data, self._data_mapping)

        if self.csv_options.remove_duplicates:
            self._temp_data = self._remove_duplicates(self._temp_data)

        return self._temp_data

    def _remove_empty_columns(self, data: List[Dict[str, Any]], mapping: Dict[str, str]) -> Tuple[
        List[Dict[str, Any]], Dict[str, str]]:
        temp_csv_path = os.path.join(os.path.dirname(self._file_path), "temp_empty_check.csv")
        current_columns = list(mapping.values())

        try:
            with open(temp_csv_path, 'w', encoding=self.encoding, newline='') as f_temp_csv:
                csv_writer = csv.DictWriter(f_temp_csv, fieldnames=current_columns)
                csv_writer.writeheader()
                csv_writer.writerows(data)
        except Exception as e:
            logger.error(f"Error writing temporary data to CSV for empty column check: {e}")
            return data, mapping

        complex_columns_source = self._complex_mapping.keys()
        complex_columns_count: Dict[str, int] = {}
        potential_complex_csv_columns = [
            col for col in current_columns if re.match('|'.join(f'^{x}_\\d+$' for x in complex_columns_source), col)
        ]
        for col in potential_complex_csv_columns:
            complex_columns_count[col] = 0

        filled_columns_in_data = set()

        try:
            with open(temp_csv_path, 'r', encoding=self.encoding, newline='') as f_csv:
                csv_reader = csv.DictReader(f_csv)
                for row in csv_reader:
                    for column_name in row:
                        if row[column_name] != '':
                            filled_columns_in_data.add(column_name)
                            if column_name in complex_columns_count:
                                complex_columns_count[column_name] += 1
        except Exception as e:
            logger.error(f"Error reading temporary CSV for empty column check: {e}")
            return data, mapping

        os.remove(temp_csv_path)

        new_mapping: Dict[str, str] = {}
        columns_to_keep_in_data = set()

        for original_field, csv_header in mapping.items():
            if csv_header in filled_columns_in_data:
                new_mapping[original_field] = csv_header
                columns_to_keep_in_data.add(csv_header)
            elif csv_header in potential_complex_csv_columns and complex_columns_count[csv_header] > 0:
                new_mapping[original_field] = csv_header
                columns_to_keep_in_data.add(csv_header)
            elif csv_header not in potential_complex_csv_columns:
                pass

        processed_data: List[Dict[str, Any]] = []
        for row in data:
            new_row = {k: v for k, v in row.items() if k in columns_to_keep_in_data}
            for source_field, header in mapping.items():
                if header.endswith('_1') and f'{source_field}_1' in new_row and f'{source_field}_2' not in new_row:
                    if header in columns_to_keep_in_data:
                        new_mapping[source_field] = re.sub(r'\s*_\d+$', '', header)
                        new_row[new_mapping[source_field]] = new_row.pop(header)
                        columns_to_keep_in_data.remove(header)
                        columns_to_keep_in_data.add(new_mapping[source_field])

            processed_data.append(new_row)

        final_mapping: Dict[str, str] = {}
        for original_field, csv_header in mapping.items():
            if csv_header in columns_to_keep_in_data:
                final_mapping[original_field] = csv_header

        renamed_fields_map = {}
        for source_field, header in mapping.items():
            if header.endswith(
                    '_1') and f'{source_field}_1' in columns_to_keep_in_data and f'{source_field}_2' not in columns_to_keep_in_data:
                new_header = re.sub(r'\s*_\d+$', '', header)
                if source_field in final_mapping:
                    final_mapping[source_field] = new_header
                    renamed_fields_map[header] = new_header

        final_processed_data: List[Dict[str, Any]] = []
        for row in processed_data:
            new_row = {}
            for k, v in row.items():
                if k in renamed_fields_map:
                    new_row[renamed_fields_map[k]] = v
                else:
                    new_row[k] = v
            final_processed_data.append(new_row)

        correct_final_mapping = {}
        for original_field, header in mapping.items():
            if header in columns_to_keep_in_data:
                if header in renamed_fields_map:
                    correct_final_mapping[original_field] = renamed_fields_map[header]
                else:
                    correct_final_mapping[original_field] = header

        return final_processed_data, correct_final_mapping

    def _remove_duplicates(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen_records = set()
        unique_data = []
        for row in data:
            row_str = json.dumps(row, sort_keys=True)
            if row_str not in seen_records:
                seen_records.add(row_str)
                unique_data.append(row)
        return unique_data

    def __enter__(self) -> CSVWriter:
        try:
            if not self._data_mapping and self._temp_data:
                self._data_mapping = {k: k for k in self._temp_data[0].keys()}

            processed_data, final_mapping = self._remove_empty_columns(self._temp_data, self._data_mapping)
            self._temp_data = processed_data
            self._data_mapping = final_mapping

            csv_headers = list(self._data_mapping.values())

            self._file = open(self._file_path, 'w', encoding=self.encoding, newline='')
            self._writer = csv.DictWriter(self._file, fieldnames=csv_headers)
            self._writer.writeheader()
            self._wrote_count = 0
            logger.info(f"Opened CSV file for writing: {self._file_path} with encoding {self.encoding}")
            logger.info(f"CSV Headers: {csv_headers}")

            self._writer.writerows(self._temp_data)
            self._wrote_count += len(self._temp_data)
            self._temp_data = []

            return self

        except Exception as e:
            logger.error(f"Failed to open CSV file {self._file_path} or write header: {e}")
            if self._file:
                self._file.close()
                self._file = None
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._file:
            self._file.close()
            logger.info(f"Closed CSV file: {self._file_path}")
        pass
