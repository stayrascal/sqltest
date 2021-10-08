import numpy as np
from pandas import DataFrame


def assert_frame_equal(left: DataFrame, right: DataFrame, sort_keys=None):
    if sort_keys is None:
        sort_keys = []
    if sort_keys:
        left = left.sort_values(by=sort_keys)
        right = right.sort_values(by=sort_keys)
    np.array_equal(left, right)
