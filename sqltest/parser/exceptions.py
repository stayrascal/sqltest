class VariableNotFound(Exception):
    def __init__(self, var):
        super().__init__(f"Variable:{var} not found.")
