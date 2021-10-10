from typing import List

from sql.parser.strategy import ExecutionEnvironment, ExtractConstantStrategy, ExtractSQLStrategy, \
    VerifyVariableStrategy

COMMENT_PREFIX = '--'
MASK = "--.*$"''
BEGINNING_MASK = "^(\\s)*--.*$"


def is_comment_statement(line: str):
    return line.lstrip().startswith(COMMENT_PREFIX)


def is_end_of_statement(line: str):
    return line.replace(MASK, "").rstrip().endswith(";")


def normalize(buffer: List[str]):
    return "\n".join([line.replace(BEGINNING_MASK, "") for line in buffer])


class SqlFileReader(object):

    @staticmethod
    def read_sql_statements(path: str) -> List[str]:
        with open(path) as file:
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

    @staticmethod
    def compile_sql_statements(statements: List[str]) -> List[str]:
        strategies = [ExtractSQLStrategy(), ExtractConstantStrategy(), VerifyVariableStrategy()]

        environments = {
            'env': 'dev',
            'data_date': '2021-10-01',
            'target_path_data_platform': '/Users/wuzhiping/workspace/tools/sql-tester/data'
        }

        for statement in statements:
            for strategy in strategies:
                if strategy.match(statement):
                    operation = strategy.convert(statement, ExecutionEnvironment(environments))
                    if operation.executable():
                        yield operation.as_summary_str()


if __name__ == '__main__':
    statements = SqlFileReader.read_sql_statements(
        '/Users/wuzhiping/Desktop/Temasek/data-glue-applications/glue/job/sessionm/dwb/sql/t_fact_user_basic_point_earn_transaction.sql')

    sqls = SqlFileReader.compile_sql_statements(statements)
    cnt = 1
    for sql in sqls:
        print(cnt)
        print(sql)
        cnt = cnt + 1
