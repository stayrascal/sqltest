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
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def _run(self, source_dataset: List[Tuple[str, pd.DataFrame]], target_dataset: List[Tuple[str, pd.DataFrame]]):
        self._register_temp_view(source_dataset)
        self._execute_sql()
        self._verify_target_dataset(target_dataset)

    def _register_temp_view(self, datasets: List[Tuple[str, pd.DataFrame]]):
        [pandas_to_spark(dataset, self._spark).createOrReplaceTempView(table) for (table, dataset) in datasets]

    def _execute_sql(self):
        self._spark.sql("DROP TABLE IF EXISTS top_1_student_by_subject")
        ddl = """
        CREATE TABLE IF NOT EXISTS top_1_student_by_subject
        (
            subject STRING,
            student_id STRING,
            student_gender STRING,
            student_age INT,
            score INT
        ) USING PARQUET
        """
        dml = """
        INSERT OVERWRITE TABLE top_1_student_by_subject
        SELECT  temp.subject,
                temp.student_id,
                s.gender as student_gender,
                s.age as student_age,
                temp.score
        FROM students s JOIN
        (SELECT  student_id, 
                    subject, 
                    score,
                    dense_rank() OVER (PARTITION BY subject ORDER BY score DESC) as rank 
            FROM subject_scores) temp ON s.id = temp.student_id
        WHERE temp.rank = 1
        """
        self._spark.sql(ddl)
        self._spark.sql(dml)

    def _verify_target_dataset(self, target_dataset):
        for (table, dataset) in target_dataset:
            result_df = self._spark.sql(f'SELECT * FROM {table}').toPandas()
            assert_frame_equal(result_df, dataset, sort_keys=['subject'])
            # np.array_equal(result_df.values, dataset.values)
