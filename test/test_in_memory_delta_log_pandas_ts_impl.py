import unittest

import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc.vc as vc
import thalesians.tsvc.delta_logs.in_memory_delta_log as mem
import thalesians.tsvc.revision_caches.in_memory_revision_cache as revcaches
import thalesians.tsvc.ts_impls.pandas_ts_impl as pandas_ts_impl

class TestInMemoryDeltaLogModinTSImpl(unittest.TestCase):
    def test_basic(self):
        delta_log = mem.InMemoryDeltaLog()
        time_series_impl = pandas_ts_impl.PandasTimeSeriesImpl()
        revision_cache = revcaches.InMemoryRevisionCache()
        tsvc = vc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache)
        tsvc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        tsvc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50]))
        tsvc.insert_rows(pd.DataFrame({'a': [5.], 'b': [2.2]}, index=[5]), index=0)
        tsvc.update_rows(pd.DataFrame({'a': [200., 300., 400.], 'b': [21., 21., 21.]}, index=[20, 30, 40]), index=2)
        tsvc.delete_rows(1, 2)
        self.assertEqual(list(tsvc.get_revisions()), [0, 1, 2, 3, 4])
        pdt.assert_frame_equal(tsvc.fetch_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_revision(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_revision(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_revision(2), pd.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}, index=[5, 10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_revision(3), pd.DataFrame({'a': [5., 10., 200., 300., 400., 50.], 'b': [2.2, 2.3, 21., 21., 21., 2.2]}, index=[5, 10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_revision(4), pd.DataFrame({'a': [5., 300., 400., 50.], 'b': [2.2, 21., 21., 2.2]}, index=[5, 30, 40, 50]))
