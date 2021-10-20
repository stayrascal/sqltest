from unittest import TestCase

from sqltest.parser.catalog import Field
from sqltest.parser.exceptions import VariableNotFound
from sqltest.parser.operation import CreateTableOperation
from sqltest.parser.strategy import ExecutionEnvironment
from sqltest.parser.strategy import ExtractConstantStrategy
from sqltest.parser.strategy import ExtractSQLStrategy
from sqltest.parser.strategy import GenerateTableSchemaFromDmlStrategy
from sqltest.parser.strategy import VerifyVariableStrategy


class TestSQLParseStrategy(TestCase):
    def test_should_throw_exception_if_variable_not_set(self):
        strategy = VerifyVariableStrategy()
        context = ExecutionEnvironment()
        self.assertRaises(
            VariableNotFound, strategy.convert, "--variable=test", context
        )

    def test_should_not_throw_exception_if_variable_set(self):
        strategy = VerifyVariableStrategy()
        context = ExecutionEnvironment({"test": "test"})
        try:
            strategy.convert("--variable=test", context)
        except VariableNotFound:
            self.fail("strategy.convert raise VariableNotFound unexpectedly!")

    def test_should_extract_constant_to_variable(self):
        strategy = ExtractConstantStrategy()
        context = ExecutionEnvironment({"env": "dev"})
        strategy.convert("--const const_key = const_val_${env}", context)

        self.assertEqual(context.get_variable_val("const_key"), "const_val_dev")

    def test_should_replace_variable(self):
        strategy = ExtractSQLStrategy()
        context = ExecutionEnvironment({"env": "dev"})
        operation = strategy.convert(
            "CREATE TABLE IF NOT EXISTS test_table_${env};", context
        )

        self.assertEqual(
            operation.as_summary_str(), "CREATE TABLE IF NOT EXISTS test_table_dev"
        )

    def test_should_extract_table_succeed_with_normalize_sql(self):
        strategy = GenerateTableSchemaFromDmlStrategy()
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

        operation = strategy.convert(
            create_table_ddl, context=ExecutionEnvironment({"env": "dev"})
        )

        self.assertTrue(isinstance(operation, CreateTableOperation))
        table = operation.get_entity()
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

    def test_should_extract_table_succeed_with_nonnormalize_sql(self):
        strategy = GenerateTableSchemaFromDmlStrategy()
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

        operation = strategy.convert(
            create_table_ddl, context=ExecutionEnvironment({"env": "dev"})
        )

        self.assertTrue(isinstance(operation, CreateTableOperation))
        table = operation.get_entity()
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
