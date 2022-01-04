import logging
import re
from typing import List
from typing import Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import TimestampType

from sqltest.engine.engine import SqlEngine
from sqltest.tools.dataframe_comparator import assert_frame_equal

LOG = logging.getLogger("spark_engine")


def pandas_to_spark(pandas_df: pd.DataFrame, spark: SparkSession) -> DataFrame:
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)


def equivalent_type(f):
    if f == "datetime64[ns]":
        return TimestampType()
    elif f == "int64":
        return LongType()
    elif f == "int32":
        return IntegerType()
    elif f == "float64":
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except Exception:
        typo = StringType()
    return StructField(string, typo)


class SparkEngine(SqlEngine):
    def __init__(self, spark: SparkSession, env_variables: dict):
        super().__init__(env_variables)
        self._spark = spark
        self._new_tables = []

    def verify_target_dataset(self):
        for (table, dataset) in self.target_dataset:
            result_df = self.query_table(table)
            assert_frame_equal(
                result_df, dataset, sort_keys=dataset.columns.values.tolist()
            )

    def get_target_tables(self) -> List[Tuple[str, pd.DataFrame]]:
        for (table, dataset) in self.target_dataset:
            yield table, self.query_table(table)

    def query_table(self, table_name: str):
        df = self._spark.sql(f"SELECT * FROM {table_name}")
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                df = df.withColumn(field.name, col(field.name).cast(DoubleType()))
        return df.toPandas()

    def _execute(self, statements: List[str]):
        self._register_temp_view(self.source_dataset)
        self._execute_sql(statements)

    def _execute_sql(self, statements: List[str]):
        for stat in statements:
            self._create_database_if_not_exist(stat)
            stat, new_tables = self._inline_data_source_table(stat)
            self._prepare_data_if_need(new_tables)
            self._spark.sql(stat)

    def _register_temp_view(self, datasets: List[Tuple[str, pd.DataFrame]]):
        for (table, dataset) in datasets:
            temp_table_name = table.replace(".", "_")
            pandas_to_spark(dataset, self._spark).createOrReplaceTempView(
                temp_table_name
            )
            LOG.info(f"Register temporary view {temp_table_name}")

    def _inline_data_source_table(self, statement):
        raw_full_table_names = self._extract_source_tables(statement)
        new_tables = []
        for full_table_name in raw_full_table_names:
            if full_table_name not in self._new_tables:
                LOG.debug(f"raw statement:\n{statement}")
                new_table_name = full_table_name.replace(".", "_")
                statement = statement.replace(full_table_name, new_table_name)
                LOG.info(
                    f"Replace table full name from {full_table_name} to {new_table_name}"
                )
                LOG.debug(f"target statement:\n{statement}")
            else:
                new_tables.append(full_table_name)
        return statement, new_tables

    @staticmethod
    def _extract_source_tables(statement):
        yield from re.findall(r"FROM\s+(.*?)\)?\s+", statement, flags=re.I)

        yield from re.findall(r"JOIN\s+(.*?)\s+.*\s+ON", statement, flags=re.I)

    def _create_database_if_not_exist(self, statement):
        for table in re.findall(
            r"^CREATE\s+[TABLE|TABLE\s+IF\s+NOT\s+EXISTS]+\s+(.*?)\s",
            statement.strip(),
            flags=re.I,
        ):
            if len(table.split(".")) > 1:
                self._new_tables.append(table)

                db = table.split(".")[0]
                self._spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
                LOG.info(f"Try to create database: {db}")

    def _prepare_data_if_need(self, new_tables):
        for (table, _) in self.source_dataset:
            if table in new_tables:
                temp_table_name = table.replace(".", "_")
                LOG.info(
                    f"Target table {table} exists in source table, will insert data at first."
                )
                self._spark.sql(
                    f"INSERT OVERWRITE {table} SELECT * FROM {temp_table_name}"
                )
