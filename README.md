# sqltest
[![pypi](https://img.shields.io/pypi/v/sqltest.svg)](https://pypi.org/project/sqltest/)
[![python](https://img.shields.io/pypi/pyversions/sqltest.svg)](https://pypi.org/project/sqltest/)
[![codecov](https://codecov.io/gh/stayrascal/sqltest/branch/main/graph/badge.svg)](https://codecov.io/gh/stayrascal/sqltest)
[![main](https://github.com/stayrascal/sqltest/workflows/main/badge.svg)](https://github.com/stayrascal/sqltest/actions?query=workflow%3Amain)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/stayrascal/sqltest/main.svg)](https://results.pre-commit.ci/latest/github/stayrascal/sqltest/main)
[![Downloads](https://static.pepy.tech/personalized-badge/sqltest?period=month&units=international_system&left_color=black&right_color=orange&left_text=PyPI%20downloads%20per%20month)](https://pepy.tech/project/sqltest)
[![License](https://img.shields.io/pypi/l/sqltest.svg)](https://github.com/stayrascal/sqltest/blob/master/LICENSE)
[![Package Status](https://img.shields.io/pypi/status/sqltest.svg)](https://pypi.org/project/sqltest/)
[![codestyle](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)


The ``sqltest`` framework makes it easy to write test cases for testing complicated ETL processing logic.
What you need to do is prepare your source & target dataset with CSV format or Excel format, and also prepare your ETL SQLs.

## Installing
Install and update using [pip](!https://pip.pypa.io/en/stable/getting-started/)
```
$ pip install sqltest
```

## An Simple Example
1. Prepare your ETL SQL file, for example: [spark_etl_demo.sql](tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.sql).
2. Prepare your source dataset and target dataset, refer [Dataset preparation](#data_preparation) check more detail.
3. Write your test cases follow by the below examples.
```
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
```
4. Run you test cases.

## <span id="data_preparation">Dataset Preparation</span>
Currently, we also support two kinds of dataset reader, and we need to follow specific pattern to prepare source data and target data.
### <span id="csv_dataset_reader">CSV Dataset Reader</span>
1. There will be a `source` and a `target` folder under specific dataset folder, click [spark_etl_sql_test_csv_demo](tests/data/cases/spark_etl_sql_test_csv_demo) to check the example detail.
2. Under `source` or `target`, you can create your source/target datasets defined in ETL SQL file, each dataset stands for a table, and we will used the csv file name as the table name, so please double check if the file name is match with table name in the SQL file.
3. Read dataset, there are two kinds of use scenarios
- Creating a `reader` object to read dataset by `CsvDatasetReader(data_path="{dataset_folder}")`
- Using an annotation `@csv_reader` based on test function
```
@csv_reader(data_path="{dataset_folder}")
def test_case(reader: DatasetReader):
    pass
```

### Excel Dataset Reader
1. Different with [CSV Dataset Reader](#csv_dataset_reader), there is only one excel file which will include source datasets and target datasets.
2. Within the Excel file, each sheet stands for a table, the sheet whose name starts with `source--` stands for source dataset/table, `target--` stands for target dataset/table. Different with CSV file, we are not use sheet name as the table name, because Excel has length limitation of sheet name, so we store the table name in the first row & first column, click [spark_etl_demo.xlsx](tests/data/cases/spark_etl_sql_test_excel_demo/spark_etl_demo.xlsx) to get more detail.
3. There also two kinds of use scenarios to read data
- Creating a `reader` object to read dataset by `ExcelDatasetReader(data_path="{excel_file_path}")`
- Using an annotation `@csv_reader` based on test function
```
@excel_reader(data_path="{excel_file_path}")
def test_case(reader: DatasetReader):
    pass
```

## SQL Engine
Currently, we only support spark engine, we have plan to support other SQL engine, e.g. Flink.


Bugs/Requests
-------------

Please use the [GitHub issue tracker](https://github.com/stayrascal/sqltest/issues) to submit bugs or request features.
