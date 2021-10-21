import re
from typing import List

from sqltest.parser.catalog import Field
from sqltest.parser.catalog import Partition
from sqltest.parser.catalog import Table


class SqlExtractor:
    def extract_table(self, statement) -> Table:
        raise NotImplementedError("this method is not implemented.")


class SparkSqlExtractor(SqlExtractor):
    def extract_table(self, statement) -> Table:
        db_name, table_name = self._extract_table_name(statement)
        fields = self._extract_columns(statement)
        partitions = self._extract_partition_keys(statement)
        tb_props = self._extract_table_properties(statement)
        return Table(table_name, None, db_name, fields, partitions, tb_props)

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
