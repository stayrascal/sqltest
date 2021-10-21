from unittest import TestCase

from sqltest.parser import SparkSqlExtractor
from sqltest.parser.catalog import Field


class TestSqlExtractor(TestCase):
    def test_should_extract_table_succeed_with_normalize_sql(self):
        extractor = SparkSqlExtractor()
        create_table_ddl = """
            CREATE TABLE IF NOT EXISTS db_name.tb_name
            (
                subject        STRING,
                student_id     INT ,
                student_gender STRING,
                student_age    INT,
                score          INT
            )
                USING PARQUET
                PARTITIONED BY (subject, `student_age`)
                TBLPROPERTIES ("foo"="bar", "val"="value")
                LOCATION 'target_data_path/db_name/tb_name'
            ;
        """
        table = extractor.extract_table(create_table_ddl)

        self.assertEqual(table.name, "tb_name")
        self.assertEqual(table.db, "db_name")
        self.assertEqual(
            list(table.fields),
            [
                Field("subject", "STRING"),
                Field("student_id", "INT"),
                Field("student_gender", "STRING"),
                Field("student_age", "INT"),
                Field("score", "INT"),
            ],
        )
        self.assertEqual(list(table.partitions), ["subject", "student_age"])

    def test_should_extract_table_succeed_with_non_normalize_sql(self):
        extractor = SparkSqlExtractor()
        create_table_ddl = """
            CREATE TABLE IF NOT EXISTS db_name.tb_name
            (
                 subject        STRING
                , student_id     INT
                ,student_gender STRING
                , student_age    INT
                ,score          INT
            )
                USING PARQUET
                PARTITIONED BY (subject)
                TBLPROPERTIES ('foo'='bar', 'val'='value')
                LOCATION 'target_data_path/db_name/tb_name'
            ;
        """

        table = extractor.extract_table(create_table_ddl)

        self.assertEqual(table.name, "tb_name")
        self.assertEqual(table.db, "db_name")
        self.assertEqual(
            list(table.fields),
            [
                Field("subject", "STRING"),
                Field("student_id", "INT"),
                Field("student_gender", "STRING"),
                Field("student_age", "INT"),
                Field("score", "INT"),
            ],
        )
        self.assertEqual(list(table.partitions), ["subject"])
