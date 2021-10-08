from typing import List, Tuple

import pandas as pd

from sql.datasource.reader import DatasetReader


class SqlRunner(object):

    def run(self, dataset_reader: DatasetReader):
        source_dataset = dataset_reader.read_source_data()
        target_dataset = dataset_reader.read_target_data()
        self._run(source_dataset, target_dataset)

    def _run(self, source_dataset: List[Tuple[str, pd.DataFrame]], target_dataset: List[Tuple[str, pd.DataFrame]]):
        raise NotImplemented("This method is not implemented.")
