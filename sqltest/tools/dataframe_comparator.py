import logging

import pandas as pd

pd.set_option("display.max_columns", None)
LOG = logging.getLogger("comparator")


def assert_frame_equal(left: pd.DataFrame, right: pd.DataFrame, sort_keys=None):
    if sort_keys is None:
        sort_keys = []
    if len(sort_keys):
        left = left.sort_values(by=sort_keys).reset_index(drop=True)
        right = right.sort_values(by=sort_keys).reset_index(drop=True)

    LOG.debug(f"Query Result: {left.dtypes} \n{left.head()}")
    LOG.debug(f"Target Result:{right.dtypes} \n{right.head()}")
    pd.testing.assert_frame_equal(
        left,
        right,
        check_dtype=False,
        check_index_type=False,
        check_column_type=False,
        check_frame_type=False,
    )
