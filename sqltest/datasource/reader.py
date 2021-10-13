from typing import List
from typing import Tuple

import pandas as pd


class DatasetReader:
    def read_source_data(self) -> List[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError("This method is not implemented.")

    def read_target_data(self) -> List[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError("This method is not implemented.")
