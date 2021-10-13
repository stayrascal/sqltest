from typing import List
from typing import Tuple

import pandas as pd

from sqltest.datasource.reader import DatasetReader


class ExcelDatasetReader(DatasetReader):
    def __init__(self, data_path: str, engine="openpyxl"):
        self._data_file = pd.ExcelFile(data_path, engine=engine)
        self.source_sheets = [
            source
            for source in self._data_file.sheet_names
            if source.startswith("source--")
        ]
        self.target_sheets = [
            source
            for source in self._data_file.sheet_names
            if source.startswith("target--")
        ]

    def read_source_data(self) -> List[Tuple[str, pd.DataFrame]]:
        for sheet in self.source_sheets:
            yield from self._parse_table(sheet)

    def read_target_data(self) -> List[Tuple[str, pd.DataFrame]]:
        for sheet in self.target_sheets:
            yield from self._parse_table(sheet)

    def _parse_table(self, sheet):
        table_name = self._data_file.parse(sheet, header=None).iloc[0].values[0]
        data = self._data_file.parse(sheet, skiprows=1)
        yield table_name, data
