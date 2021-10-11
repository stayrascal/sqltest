from pandas import DataFrame
from pandas import testing


def assert_frame_equal(left: DataFrame, right: DataFrame, sort_keys=None):
    if sort_keys is None:
        sort_keys = []
    if len(sort_keys):
        left = left.sort_values(by=sort_keys).reset_index(drop=True)
        right = right.sort_values(by=sort_keys).reset_index(drop=True)

    testing.assert_frame_equal(left, right, check_dtype=False, check_index_type=False)
