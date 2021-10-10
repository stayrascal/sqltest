from unittest import TestCase

from sql.datasource import CsvDatasetReader
from sql.runner.spark import SparkRunner
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
        runner = SparkRunner(SPARK, environments)
        runner.run(reader, sql_file_path)
