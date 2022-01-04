from typing import List
from typing import Tuple

import pandas as pd

from sqltest.datasource.reader import DatasetReader
from sqltest.parser.sql_reader import SqlFileReader
from sqltest.parser.strategy import ExecutionEnvironment
from sqltest.parser.strategy import ExtractConstantStrategy
from sqltest.parser.strategy import ExtractSQLStrategy
from sqltest.parser.strategy import VerifyVariableStrategy


class SqlEngine:
    def __init__(self, env_variables: dict, parse_strategies=None):
        if parse_strategies is None:
            self._sql_parse_strategies = [
                ExtractSQLStrategy(),
                ExtractConstantStrategy(),
                VerifyVariableStrategy(),
            ]
        else:
            self.parse_strategies = parse_strategies

        self._execution_env = ExecutionEnvironment(env_variables)

    def run(self, dataset_reader: DatasetReader, sql_file_path: str):
        self.source_dataset: List[Tuple[str, pd.DataFrame]] = list(
            dataset_reader.read_source_data()
        )
        self.target_dataset: List[Tuple[str, pd.DataFrame]] = list(
            dataset_reader.read_target_data()
        )
        statements = SqlFileReader().compile_sql_file(
            sql_file_path, self._execution_env, self._sql_parse_strategies
        )
        self._execute(statements)

    def _execute(self, statements: List[str]):
        raise NotImplementedError("This method is not implemented.")

    def get_target_tables(self) -> List[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError("This method is not implemented.")

    def get_target_dataset(self) -> List[Tuple[str, pd.DataFrame]]:
        return self.target_dataset

    def verify_target_dataset(self):
        raise NotImplementedError("This method is not implemented.")

    def query_table(self, table_name: str):
        raise NotImplementedError("This method is not implemented.")
