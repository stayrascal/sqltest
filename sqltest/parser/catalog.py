from typing import List


class Field:
    def __init__(self, name, data_type, desc=None):
        self.desc = desc
        self.data_type = data_type
        self.name = name

    def __eq__(self, other):
        return (
            self.desc == other.desc
            and self.data_type == other.data_type
            and self.name == other.name
        )


class Partition(Field):
    def __init__(self, name, data_type, desc):
        super().__init__(name, data_type, desc)


class Table:
    def __init__(
        self,
        name,
        desc,
        db,
        fields: List[Field],
        partitions: List[str],
        properties: dict = None,
    ):
        self.properties = properties
        self.partitions = partitions
        self.fields = fields
        self.db = db
        self.desc = desc
        self.name = name
