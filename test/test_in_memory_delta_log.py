import unittest

import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc as tsvc
import thalesians.tsvc.delta_logs.in_memory_delta_log as mem
import thalesians.tsvc.revision_caches.in_memory_revision_cache as revcaches
import thalesians.tsvc.ts_impls.pandas_ts_impl as pdtsimpl

class TestInMemoryDeltaLog(unittest.TestCase):
    def test_basic(self):
        delta_log = mem.InMemoryDeltaLog()
        time_series_impl = pdtsimpl.PandasTimeSeriesImpl()
        revision_cache = revcaches.InMemoryRevisionCache()
        vc = tsvc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache)
        vc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        vc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50]))
        vc.insert_rows(pd.DataFrame({'a': [5.], 'b': [2.2]}, index=[5]), index=0)
        self.assertEqual(list(vc.get_revisions()), [0, 1, 2])
        pdt.assert_frame_equal(vc.fetch_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(vc.fetch_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(vc.fetch_revision(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(vc.fetch_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(vc.fetch_revision(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(vc.fetch_revision(2), pd.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}, index=[5, 10, 20, 30, 40, 50]))
