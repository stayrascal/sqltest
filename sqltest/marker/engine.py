from pyspark.sql import SparkSession

from sqltest.datasource.reader import DatasetReader
from sqltest.engine import SparkEngine


def spark_engine(
    spark: SparkSession, sql_path: str, reader: DatasetReader = None, env: dict = None
):
    def wrapper(func):
        def decorate(*args):
            engine = SparkEngine(spark, env)
            dataset_reader = retrieve_reader(args)
            if not dataset_reader:
                raise Exception("DatasetReader is not defined.")

            engine.run(dataset_reader, sql_path)

            func(*args, engine)

        def retrieve_reader(args):
            if not reader:
                for arg in args:
                    if isinstance(arg, DatasetReader):
                        return arg
            else:
                return reader

        return decorate

    return wrapper
