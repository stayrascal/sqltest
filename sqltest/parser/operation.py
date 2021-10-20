from abc import ABC

from sqltest.parser.catalog import Table


class Operation:
    def as_summary_str(self) -> str:
        raise NotImplementedError("this method is not implemented.")

    def executable(self) -> bool:
        raise NotImplementedError("this method is not implemented.")

    def get_entity(self) -> object:
        raise NotImplementedError("this method is not implemented.")


class NothingOperation(Operation):
    def get_entity(self) -> object:
        return ""

    def executable(self) -> bool:
        return False

    def as_summary_str(self) -> str:
        return ""


class VerifyVariableOperation(NothingOperation):
    def __init__(self, variable):
        self.variable = variable

    def as_summary_str(self) -> str:
        return f"--variable={self.variable}"


class RunnableOperation(Operation, ABC):
    def __init__(self, sql: str):
        self._sql = sql

    def as_summary_str(self) -> str:
        return self._sql

    def executable(self) -> bool:
        return True


class CreateTableOperation(RunnableOperation):
    def get_entity(self) -> object:
        return self._table

    def __init__(self, sql: str, table: Table):
        super().__init__(sql)
        self._table = table
        self._sql = sql

    def as_summary_str(self) -> str:
        return self._sql
