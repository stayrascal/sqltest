# sqltest

The `sqltest` framework makes it easy to write test cases for testing complicated ETL processing logic.
What you need to do is prepare your source & target dataset with CSV format or Excel format, and also prepare your ETL SQL.
- We only support CSV source dataset format currently, but we plan to implement more formats. e.g Excel
- And also, we are planing to support more SQL engines, e.g. Spark, Flink.