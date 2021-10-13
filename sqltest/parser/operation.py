class Operation:
    def as_summary_str(self) -> str:
        raise NotImplementedError("this method is not implemented.")

    def executable(self) -> bool:
        raise NotImplementedError("this method is not implemented.")


class NothingOperation(Operation):
    def executable(self) -> bool:
        return False

    def as_summary_str(self) -> str:
        return ""


class VerifyVariableOperation(NothingOperation):
    def __init__(self, variable):
        self.variable = variable

    def as_summary_str(self) -> str:
        return f"--variable={self.variable}"


class RunnableOperation(Operation):
    def __init__(self, sql: str):
        self._sql = sql

    def as_summary_str(self) -> str:
        return self._sql

    def executable(self) -> bool:
        return True
