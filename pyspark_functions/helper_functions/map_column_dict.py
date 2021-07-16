from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql.types import *


def map_column_dict(existing_col, dictionary, new_col):
    """
    Tihs function maps labels in the existing column in Spark dataframe
    according to their values in dictionary to a new column.

    Args:
        existing_col (string): Name of existing column
        dictionary (dictionary): Dictionary for label mapping.
        new_col (string): Name of new column where the new labels are mapped to.

    """
    mapping_expr = F.create_map([F.lit(x) for x in chain(*dictionary.items())])
    return mapping_expr.getItem(F.col(existing_col)).alias(new_col)
