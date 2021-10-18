from unittest import TestCase

from sqltest.parser.sql_reader import SqlFileReader
from sqltest.parser.strategy import ExecutionEnvironment
from sqltest.parser.strategy import ExtractConstantStrategy
from sqltest.parser.strategy import ExtractSQLStrategy
from sqltest.parser.strategy import VerifyVariableStrategy
from tests import PROJECT_PATH


class TestSQLReader(TestCase):
    def test_should_parse_sql_file_succeed(self):
        reader = SqlFileReader()
        sql_file_path = f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql"
        env = ExecutionEnvironment(
            {
                "env": "dev",
                "target_data_path": f"{PROJECT_PATH}/tests/data/tables",
            }
        )
        strategies = [
            ExtractSQLStrategy(),
            ExtractConstantStrategy(),
            VerifyVariableStrategy(),
        ]
        statements = reader.compile_sql_file(sql_file_path, env, strategies)
        self.assertEqual(len(list(statements)), 2)
        self.assertEqual(
            env.get_variable_val("target_table"), "dw_dws_dev.top1_student_by_subject"
        )

    def test_should_return_empty_list_if_no_parser_strategy_provided(self):
        reader = SqlFileReader()
        sql_file_path = f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql"
        statements = reader.compile_sql_file(sql_file_path, None, [])
        self.assertEqual(len(list(statements)), 0)
