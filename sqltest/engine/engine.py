from typing import List, Tuple

import pandas as pd

from sqltest.datasource.reader import DatasetReader
from sqltest.parser.sql_reader import SqlFileReader
from sqltest.parser.strategy import ExecutionEnvironment, ExtractConstantStrategy, ExtractSQLStrategy, \
    VerifyVariableStrategy


class SqlEngine(object):
    def __init__(self, env_variables: dict):
        self._sql_parse_strategies = [ExtractSQLStrategy(), ExtractConstantStrategy(), VerifyVariableStrategy()]
        self._execution_env = ExecutionEnvironment(env_variables)

    def run(self, dataset_reader: DatasetReader, sql_file_path: str):
        self.source_dataset = dataset_reader.read_source_data()
        self.target_dataset = dataset_reader.read_target_data()
        statements = SqlFileReader().compile_sql_file(sql_file_path, self._execution_env, self._sql_parse_strategies)
        self._execute(statements)

    def _execute(self, statements: List[str]):
        raise NotImplementedError("This method is not implemented.")

    def get_target_tables(self) -> List[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError("This method is not implemented.")

    def get_target_dataset(self) -> List[Tuple[str, pd.DataFrame]]:
        return self.target_dataset

    def verify_target_dataset(self):
        raise NotImplementedError("This method is not implemented.")