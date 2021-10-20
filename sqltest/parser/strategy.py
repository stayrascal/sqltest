import re
from abc import ABC
from typing import List

from sqltest.parser.catalog import Field
from sqltest.parser.catalog import Partition
from sqltest.parser.catalog import Table
from sqltest.parser.exceptions import VariableNotFound
from sqltest.parser.operation import CreateTableOperation
from sqltest.parser.operation import NothingOperation
from sqltest.parser.operation import Operation
from sqltest.parser.operation import RunnableOperation


class ExecutionEnvironment:
    def __init__(self, env=None):
        if env is None:
            env = {}
        self._env = env

    def check_variable(self, var: str) -> str:
        if var in self._env:
            return var
        else:
            raise VariableNotFound(var)

    def get_variable_val(self, var) -> str:
        return self._env.get(self.check_variable(var))

    def set_variable(self, key: str, val: str):
        self._env[key] = val


class ParseStrategy:
    def match(self, statement: str) -> bool:
        raise NotImplementedError("this method is not implemented.")

    def convert(self, statement: str, context: ExecutionEnvironment) -> Operation:
        raise NotImplementedError("this method is not implemented.")

    def get_hints(self) -> List[str]:
        raise NotImplementedError("this method is not implemented.")


class AbstractRegexParseStrategy(ParseStrategy, ABC):
    def __init__(self, pattern):
        self.pattern = pattern

    def match(self, statement: str) -> bool:
        return bool(re.search(self.pattern, statement.strip()))

    @staticmethod
    def overwrite_variables(statement: str, context: ExecutionEnvironment) -> str:
        for var in re.findall(r"\${(\w*)}", statement):
            val = context.get_variable_val(var)
            statement = statement.replace(f"${{{var}}}", val)
        return statement


class VerifyVariableStrategy(AbstractRegexParseStrategy):
    def __init__(self):
        super().__init__("--variable")

    def convert(self, statement: str, context: ExecutionEnvironment) -> Operation:
        context.check_variable(statement.strip().split("variable=")[1])
        return NothingOperation()

    def get_hints(self) -> List[str]:
        return ["--variable=xxx"]


class ExtractConstantStrategy(AbstractRegexParseStrategy):
    def __init__(self):
        super().__init__("--const")

    def convert(self, statement: str, context: ExecutionEnvironment) -> Operation:
        statement = self.overwrite_variables(statement, context)

        statement = statement.split(self.pattern)[1].strip()
        const_key = statement.split("=")[0].strip()
        const_val = statement.split("=")[1].strip()
        context.set_variable(const_key, const_val)
        return NothingOperation()

    def get_hints(self) -> List[str]:
        return ["--const var=val", "--const var=val_${other_var}"]


class ExtractSQLStrategy(AbstractRegexParseStrategy):
    def __init__(self):
        super().__init__("[CREATE|INSERT|SELECT|DROP|SET]")

    def convert(self, statement: str, context: ExecutionEnvironment) -> Operation:
        statement = self.overwrite_variables(statement, context)
        return RunnableOperation(statement.split(";")[0])

    def get_hints(self) -> List[str]:
        return ["CREATE TABLE IF NOT EXISTS xxx"]


class GenerateTableSchemaFromDmlStrategy(AbstractRegexParseStrategy):
    def __init__(self):
        super().__init__(r"^CREATE\s+TABLE")

    def convert(self, statement: str, context: ExecutionEnvironment) -> Operation:
        statement = self.overwrite_variables(statement, context)
        db_name, table_name = self._extract_table_name(statement)
        fields, partition_keys, properties = self._extract_fields(statement)
        return CreateTableOperation(
            statement,
            Table(table_name, None, db_name, fields, partition_keys, properties),
        )

    def get_hints(self) -> List[str]:
        pass

    @staticmethod
    def _extract_table_name(statement) -> (str, str):
        tables = re.findall(
            r"^CREATE\s+[TABLE|TABLE IF NOT EXISTS]+\s+(.*?)\s",
            statement.strip(),
            flags=re.I,
        )
        assert len(tables) == 1
        tb_full_name: str = tables[0]
        temp = tb_full_name.split(".")
        return ("default", temp[0]) if len(temp) == 1 else (temp[0], temp[1])

    def _extract_fields(self, statement) -> (List[Field], List[Partition], dict):
        fields = self._extract_columns(statement)
        partitions = self._extract_partition_keys(statement)
        tb_props = self._extract_table_properties(statement)
        return fields, partitions, tb_props

    @staticmethod
    def _extract_columns(statement) -> List[Field]:
        for name, data_type in re.findall(
            r"\s*[,|\s]*([\w_]+)\s+([\w]+)\s*[,|\b)]", statement.strip(), flags=re.I
        ):
            yield Field(name, data_type)

    @staticmethod
    def _extract_partition_keys(statement) -> List[str]:
        for parts in re.findall(
            r"PARTITIONED\s+BY\s+\((.*)\)", statement.strip(), flags=re.I
        ):
            for part in parts.replace("`", "").split(","):
                yield part.strip()

    @staticmethod
    def _extract_table_properties(statement) -> dict:
        return {}
