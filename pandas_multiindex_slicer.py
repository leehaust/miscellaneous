#!/usr/bin/env python
# written while working at TrailStone Renewable Energy Management

from typing import Union

import pandas as pd


def get_level_loc(level_name, idx):
    return idx.names.index(level_name)


def index_name_slice(slices: dict, idx: pd.Index):
    nlevels = idx.nlevels
    if nlevels > 1:
        sliced_levels = [get_level_loc(level_name, idx) for level_name in
                         slices.keys()]
        values_by_level = {get_level_loc(level_name, idx): values if
                           isinstance(values, list) else [values]
                           for level_name, values in slices.items()}
    else:
        sliced_levels = [0]
        values = slices.values()
        if len(values) > 1:
            raise ValueError('Cannot slice a single index on more that one level name')
        values_by_level = list(values)
    slicer = []
    for level in range(nlevels):
        if level in sliced_levels:
            slicer.append(values_by_level[level])
        else:
            slicer.append(slice(None))
    return tuple(slicer)


def slx(df: Union[pd.DataFrame, pd.Series], rows=None, columns=None):
    """

    Parameters
    ----------
    df: pd.DataFrame
        A pandas.DataFrame having named pd.Index or pd.MultiIndex on rows and columns
    rows: dict
        A dict keyed on levels names to slice with values as lists or a string
    columns: dict
        A dict keyed on levels names to slice with values as lists or a string

    Returns
    -------
    pd.DataFrame
        slx(df, rows, columns) is equivalent to df.loc[rows, columns], where rows
         and columns are like a pd.IndexSlice by name

    """
    if rows is not None:
        row_slicer = index_name_slice(rows, df.index)
    else:
        row_slicer = slice(None)

    if isinstance(df, pd.DataFrame):

        if columns is not None:
            column_slicer = index_name_slice(columns, df.columns)
        else:
            column_slicer = slice(None)

        sliced = df.loc[row_slicer, column_slicer]

    elif isinstance(df, pd.Series):

        sliced = df.loc[row_slicer]

    return sliced
