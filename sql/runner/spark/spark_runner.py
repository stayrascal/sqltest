from typing import List, Tuple

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *

from sql.runner.runner import SqlRunner
from sql.tools.dataframe_comparator import assert_frame_equal


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


class SparkRunner(SqlRunner):
    def __init__(self, spark: SparkSession, env_variables: dict):
        super().__init__(env_variables)
        self._spark = spark

    def _run(self, source_dataset: List[Tuple[str, pd.DataFrame]], target_dataset: List[Tuple[str, pd.DataFrame]],
             statements: List[str]):
        self._register_temp_view(source_dataset)
        self._execute_sql(statements)
        self._verify_target_dataset(target_dataset)

    def _register_temp_view(self, datasets: List[Tuple[str, pd.DataFrame]]):
        [pandas_to_spark(dataset, self._spark).createOrReplaceTempView(table) for (table, dataset) in datasets]

    def _execute_sql(self, statements: List[str]):
        for stat in statements:
            self._spark.sql(stat)

    def _verify_target_dataset(self, target_dataset):
        for (table, dataset) in target_dataset:
            result_df = self._spark.sql(f'SELECT * FROM {table}').toPandas()
            assert_frame_equal(result_df, dataset, sort_keys=dataset.columns.values.tolist())
