from unittest import TestCase

from sqltest.datasource.excel import ExcelDatasetReader
from sqltest.datasource.reader import DatasetReader
from sqltest.engine import SparkEngine
from sqltest.engine.engine import SqlEngine
from sqltest.marker import excel_reader
from sqltest.marker import spark_engine
from tests import PROJECT_PATH
from tests.integrations import SPARK


class TestExcelDataSource(TestCase):
    def test_excel_data_source_demo(self):
        environments = {
            "env": "dev",
            "target_data_path": f"{PROJECT_PATH}/tests/data/tables",
        }

        reader = ExcelDatasetReader(
            data_path=f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx"
        )
        sql_file_path = f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql"

        engine = SparkEngine(SPARK, environments)
        engine.run(reader, sql_file_path)
        engine.verify_target_dataset()

    @excel_reader(
        data_path=f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx"
    )
    @spark_engine(
        spark=SPARK,
        sql_path=f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql",
        env={"env": "dev", "target_data_path": f"{PROJECT_PATH}/tests/data/tables"},
    )
    def test_excel_with_decorate(self, reader: DatasetReader, engine: SqlEngine):
        engine.verify_target_dataset()

    @spark_engine(
        spark=SPARK,
        sql_path=f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql",
        reader=ExcelDatasetReader(
            f"{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx"
        ),
        env={"env": "dev", "target_data_path": f"{PROJECT_PATH}/tests/data/tables"},
    )
    def test_excel_with_engine_decorate(self, engine: SqlEngine):
        engine.verify_target_dataset()

    @spark_engine(
        spark=SPARK,
        sql_path=f"{PROJECT_PATH}/tests/data/cases/spark_deuplicate_excel_demo/demo.sql",
        reader=ExcelDatasetReader(
            f"{PROJECT_PATH}/tests/data/cases/spark_deuplicate_excel_demo/spark_etl_demo.xlsx"
        ),
        env={"env": "dev", "target_data_path": f"{PROJECT_PATH}/tests/data/tables"},
    )
    def test_duplicated_case_excel(self, engine: SqlEngine):
        engine.verify_target_dataset()
