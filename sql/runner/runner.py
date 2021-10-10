from typing import List, Tuple

import pandas as pd

from sql.datasource.reader import DatasetReader
from sql.parser.sql_reader import SqlFileReader
from sql.parser.strategy import ExecutionEnvironment, ExtractConstantStrategy, ExtractSQLStrategy, \
    VerifyVariableStrategy


class SqlRunner(object):
    def __init__(self, env_variables: dict):
        self._sql_parse_strategies = [ExtractSQLStrategy(), ExtractConstantStrategy(), VerifyVariableStrategy()]
        self._execution_env = ExecutionEnvironment(env_variables)

    def run(self, dataset_reader: DatasetReader, sql_file_path: str):
        source_dataset = dataset_reader.read_source_data()
        target_dataset = dataset_reader.read_target_data()
        statements = SqlFileReader().compile_sql_file(sql_file_path, self._execution_env, self._sql_parse_strategies)
        self._run(source_dataset, target_dataset, statements)

    def _run(self,
             source_dataset: List[Tuple[str, pd.DataFrame]],
             target_dataset: List[Tuple[str, pd.DataFrame]],
             statements: List[str]):
        raise NotImplemented("This method is not implemented.")
