import pytest

import numpy as np
import pandas as pd

from pandas_multiindex_slicer import slx


@pytest.fixture
def mi():
    end_time = pd.Timestamp.today().round('H') - pd.Timedelta(hours=1)
    start_time = end_time - pd.Timedelta(hours=8759)

    level_0_values = pd.date_range(start_time, end_time, freq='H')  # 8760 hours
    level_1_values = [f'location{x}' for x in range(10)]  # 10 locations

    idx = pd.MultiIndex.from_product(
        iterables=[level_0_values, level_1_values],
        names=['hour_beginning', 'location']
    )
    return pd.DataFrame(
        data=np.random.random(len(idx)),
        index=idx,
        columns=['value']
    )


def test_slx(mi):
    mi_slice = slx(mi, {'location': ['location1', 'location5']})
    assert len(mi_slice) == 8760 * 2
    assert 'location' in mi_slice.index.names
    assert mi_slice.index.nlevels == 2

    mi_slice = slx(mi, {'location': ['location1', 'location5'],
                        'hour_beginning': (pd.Timestamp.today().round('H') -
                                           pd.Timedelta(hours=1))
                        })
    assert len(mi_slice) == 2
