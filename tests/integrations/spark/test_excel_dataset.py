from unittest import TestCase

from sqltest.datasource import ExcelDatasetReader
from sqltest.engine.spark import SparkEngine
from tests import PROJECT_PATH, SPARK


class TestExcelDataSource(TestCase):
    def test_excel_data_source_demo(self):
        environments = {
            'env': 'dev',
            'target_data_path': f'{PROJECT_PATH}/tests/data/tables'
        }
        reader = ExcelDatasetReader(
            data_path=f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx')
        sql_file_path = f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql'

        SPARK.sql("CREATE DATABASE IF NOT EXISTS dw_dim_dev")
        SPARK.sql("CREATE DATABASE IF NOT EXISTS dw_dwd_dev")
        SPARK.sql("CREATE DATABASE IF NOT EXISTS dw_dws_dev")
        engine = SparkEngine(SPARK, environments)
        engine.run(reader, sql_file_path)
        engine.verify_target_dataset()
