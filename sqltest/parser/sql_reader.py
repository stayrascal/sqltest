from typing import List

from sqltest.parser.strategy import ExecutionEnvironment
from sqltest.parser.strategy import ParseStrategy

COMMENT_PREFIX = "--"
MASK = "--.*$" ""
BEGINNING_MASK = "^(\\s)*--.*$"


def is_comment_statement(line: str):
    return line.lstrip().startswith(COMMENT_PREFIX)


def is_end_of_statement(line: str):
    return line.replace(MASK, "").rstrip().endswith(";")


def normalize(buffer: List[str]):
    return "\n".join([line.replace(BEGINNING_MASK, "") for line in buffer])


class SqlFileReader:
    @staticmethod
    def _read_sql_statements(path: str) -> List[str]:
        with open(path, encoding="utf8") as file:
            lines = file.readlines()
            buffer = []
            for line in lines:
                if is_comment_statement(line):
                    yield line
                elif is_end_of_statement(line):
                    buffer.append(line)
                    yield normalize(buffer)
                    buffer.clear()
                else:
                    buffer.append(line)
            if len(buffer):
                yield normalize(buffer)

    def compile_sql_file(
        self, path: str, env: ExecutionEnvironment, strategies: List[ParseStrategy]
    ) -> List[str]:
        statements = self._read_sql_statements(path)
        for statement in statements:
            for strategy in strategies:
                if strategy.match(statement):
                    operation = strategy.convert(statement, env)
                    if operation.executable():
                        yield operation.as_summary_str()
