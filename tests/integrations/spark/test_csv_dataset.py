from unittest import TestCase

from sql.datasource import CsvDatasetReader
from sql.runner.spark import SparkRunner
from tests import SPARK


class TestCsvDataSource(TestCase):
    def test_csv_data_source_demo(self):
        reader = CsvDatasetReader(
            data_path='/Users/wuzhiping/workspace/tools/sql-tester/tests/data/cases/spark_etl_sql_test_demo')
        runner = SparkRunner(SPARK)
        runner.run(reader)
