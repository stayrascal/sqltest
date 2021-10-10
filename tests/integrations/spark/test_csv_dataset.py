from unittest import TestCase

from sql.datasource import CsvDatasetReader
from sql.runner.spark import SparkRunner
from tests import SPARK


class TestCsvDataSource(TestCase):
    def test_csv_data_source_demo(self):
        environments = {
            'env': 'dev',
            'target_data_path': '/Users/wuzhiping/workspace/tools/sql-tester/tests/data/tables'
        }
        reader = CsvDatasetReader(
            data_path='/Users/wuzhiping/workspace/tools/sql-tester/tests/data/cases/spark_etl_sql_test_demo')
        sql_file_path = '/Users/wuzhiping/workspace/tools/sql-tester/tests/data/cases/spark_etl_sql_test_demo/spark_etl_demo.sql'
        runner = SparkRunner(SPARK, environments)
        runner.run(reader, sql_file_path)
