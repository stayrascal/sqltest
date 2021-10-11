import re
from typing import List, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *

from sqltest.engine.engine import SqlEngine
from sqltest.tools.dataframe_comparator import assert_frame_equal


def pandas_to_spark(pandas_df: pd.DataFrame, spark: SparkSession) -> DataFrame:
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)


def equivalent_type(f):
    if f == 'datetime64[ns]':
        return TimestampType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except:
        typo = StringType()
    return StructField(string, typo)


class SparkEngine(SqlEngine):
    def __init__(self, spark: SparkSession, env_variables: dict):
        super().__init__(env_variables)
        self._spark = spark

    def verify_target_dataset(self):
        for (table, dataset) in self.target_dataset:
            result_df = self._query_table(table)
            assert_frame_equal(result_df, dataset, sort_keys=dataset.columns.values.tolist())

    def _execute(self, statements: List[str]):
        self._register_temp_view(self.source_dataset)
        self._execute_sql(statements)

    def _execute_sql(self, statements: List[str]):
        for stat in statements:
            self._create_database_if_not_exist(stat)
            stat = self._inline_data_source_table(stat)
            self._spark.sql(stat)

    def get_target_tables(self) -> List[Tuple[str, pd.DataFrame]]:
        for (table, dataset) in self.target_dataset:
            yield table, self._query_table(table)

    def _query_table(self, table):
        return self._spark.sql(f"SELECT * FROM {table}").toPandas()

    def _register_temp_view(self, datasets: List[Tuple[str, pd.DataFrame]]):
        [pandas_to_spark(dataset, self._spark).createOrReplaceTempView(table.replace(".", "_")) for (table, dataset) in
         datasets]

    def _verify_target_dataset(self, target_dataset):
        for (table, dataset) in target_dataset:
            result_df = self._query_table(table)
            assert_frame_equal(result_df, dataset, sort_keys=dataset.columns.values.tolist())

    def _inline_data_source_table(self, statement):
        if bool(re.match('(^INSERT|^SELECT)', statement.strip(), re.I)):
            raw_full_table_names = self._extract_source_tables(statement)
            for full_table_name in raw_full_table_names:
                new_table_name = full_table_name.replace(".", "_")
                statement = statement.replace(full_table_name, new_table_name)
            return statement
        else:
            return statement

    @staticmethod
    def _extract_source_tables(statement):
        for table in re.findall("FROM\s+(.*?) ", statement, flags=re.I):
            yield table

        for table in re.findall('JOIN\s+(.*?)\s+.*\s+ON', statement, flags=re.I):
            yield table

    def _create_database_if_not_exist(self, statement):
        for db in re.findall("^CREATE\s+[TABLE|IF NOT EXISTS]+\s+(.*?)\.", statement.strip(), flags=re.I):
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
