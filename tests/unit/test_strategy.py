from unittest import TestCase

from sqltest.parser.exceptions import VariableNotFound
from sqltest.parser.strategy import ExecutionEnvironment
from sqltest.parser.strategy import ExtractConstantStrategy
from sqltest.parser.strategy import ExtractSQLStrategy
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
