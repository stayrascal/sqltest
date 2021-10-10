from unittest import TestCase

from sqltest.datasource import CsvDatasetReader
from sqltest.engine.spark import SparkEngine
from tests import PROJECT_PATH, SPARK


class TestCsvDataSource(TestCase):
    def test_csv_data_source_demo(self):
        environments = {
            'env': 'dev',
            'target_data_path': f'{PROJECT_PATH}/tests/data/tables'
        }
        reader = CsvDatasetReader(
            data_path=f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_demo')
        sql_file_path = f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_demo/spark_etl_demo.sql'

        engine = SparkEngine(SPARK, environments)
        engine.run(reader, sql_file_path)
        engine.verify_target_dataset()
