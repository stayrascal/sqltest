import os

from pyspark.sql import SparkSession

PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

SPARK = (SparkSession
         .builder
         .appName("IntegrationTests")
         .config('spark.default.parallelism', 10)
         .master('local[*]')
         .getOrCreate())
