from pyspark.sql import SparkSession

SPARK = (
    SparkSession.builder.appName("IntegrationTests")
    .config("spark.default.parallelism", 10)
    .master("local[*]")
    .getOrCreate()
)
