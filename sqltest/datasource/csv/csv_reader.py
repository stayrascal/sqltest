from os import listdir
from os.path import isfile, join
from typing import List, Tuple

import pandas as pd

from sqltest.datasource.reader import DatasetReader


class CsvDatasetReader(DatasetReader):

    def __init__(self, data_path: str, source_key="source", target_key="target"):
        self._data_path = data_path
        self._source_key = source_key
        self._target_key = target_key

    def read_source_data(self) -> List[Tuple[str, pd.DataFrame]]:
        return self._read_csv(join(self._data_path, self._source_key))

    def read_target_data(self) -> List[Tuple[str, pd.DataFrame]]:
        return self._read_csv(join(self._data_path, self._target_key))

    @staticmethod
    def _read_csv(path: str) -> List[Tuple[str, pd.DataFrame]]:
        csv_files = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith('.csv')]
        for f in csv_files:
            yield f.split('.csv')[0], pd.read_csv(join(path, f))
        # return [(f.split('.csv')[0], pd.read_csv(join(path, f))) for f in csv_files]


if __name__ == '__main__':
    reader = CsvDatasetReader(
        data_path='/Users/wuzhiping/workspace/tools/sql-tester/tests/data/cases/spark_etl_sql_test_demo')
    for t, df in reader.read_source_data():
        print(f"table: {t}")
        print(df.head())
