# sqltest

The `sqltest` framework makes it easy to write test cases for testing complicated ETL processing logic.
What you need to do is prepare your source & target dataset with CSV format or Excel format, and also prepare your ETL SQL.
- We only support CSV source dataset format currently, but we plan to implement more formats. e.g Excel
- And also, we are planing to support more SQL engines, e.g. Spark, Flink.

An example of ETL sql test:
```
class TestExcelDataSource(TestCase):
    def test_excel_data_source_demo(self):
        environments = {
            'env': 'dev',
            'target_data_path': f'{PROJECT_PATH}/tests/data/tables'
        }

        reader = ExcelDatasetReader(
            data_path=f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx')
        sql_file_path = f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql'

        engine = SparkEngine(SPARK, environments)
        engine.run(reader, sql_file_path)
        engine.verify_target_dataset()

    @excel_reader(data_path=f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx')
    @spark_engine(spark=SPARK,
                  sql_path=f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql',
                  env={'env': 'dev', 'target_data_path': f'{PROJECT_PATH}/tests/data/tables'})
    def test_excel_with_decorate(self, reader: DatasetReader, engine: SqlEngine):
        engine.verify_target_dataset()

    @spark_engine(spark=SPARK,
                  sql_path=f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql',
                  reader=ExcelDatasetReader(
                      f'{PROJECT_PATH}/tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx'),
                  env={'env': 'dev', 'target_data_path': f'{PROJECT_PATH}/tests/data/tables'})
    def test_excel_with_engine_decorate(self, engine: SqlEngine):
        engine.verify_target_dataset()
```