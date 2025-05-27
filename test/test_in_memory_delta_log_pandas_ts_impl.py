import unittest

import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc.vc as vc
import thalesians.tsvc.delta_logs.in_memory_delta_log as mem
import thalesians.tsvc.meta_data_caches.in_memory_meta_data_cache as meta_data_caches
import thalesians.tsvc.revision_caches.in_memory_revision_cache as revision_caches
import thalesians.tsvc.ts_impls.pandas_ts_impl as pandas_ts_impl

class TestInMemoryDeltaLogModinTSImpl(unittest.TestCase):
    def test_basic(self):
        delta_log = mem.InMemoryDeltaLog()
        time_series_impl = pandas_ts_impl.PandasTimeSeriesImpl()
        revision_cache = revision_caches.InMemoryRevisionCache()
        meta_data_cache = meta_data_caches.InMemoryMetaDataCache()
        tsvc = vc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache, meta_data_cache=meta_data_cache)
        self.assertEqual(tsvc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20])), 0)
        self.assertEqual(tsvc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50])), 1)
        self.assertEqual(tsvc.insert_rows(pd.DataFrame({'a': [5.], 'b': [2.2]}, index=[5]), index=0), 2)
        self.assertEqual(tsvc.update_rows(pd.DataFrame({'a': [200., 300., 400.], 'b': [21., 21., 21.]}, index=[20, 30, 40]), index=2), 3)
        self.assertEqual(tsvc.delete_rows(1, 2), 4)
        self.assertEqual(tsvc.update_rows(pd.DataFrame({'a': [3000., 4000.]}, index=[30, 40]), columns=['a'], index=1), 5)
        self.assertEqual(tsvc.insert_meta_data({'description': 'Test time series', 'author': 'someone'}), 6)
        self.assertEqual(tsvc.update_meta_data({'description': 'Something more informative'}), 7)
        self.assertEqual(tsvc.reorder_meta_data(['author', 'description']), 8)
        self.assertEqual(tsvc.delete_meta_data(['description']), 9)
        self.assertEqual(tsvc.append_columns({'c': [100., 200., 300., 400.]}), 10)
        self.assertEqual(tsvc.delete_columns(['b']), 11)
        self.assertEqual(tsvc.reorder_columns(['c', 'a']), 12)
        self.assertEqual(list(tsvc.get_revisions()), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(2), pd.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}, index=[5, 10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(3), pd.DataFrame({'a': [5., 10., 200., 300., 400., 50.], 'b': [2.2, 2.3, 21., 21., 21., 2.2]}, index=[5, 10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(4), pd.DataFrame({'a': [5., 300., 400., 50.], 'b': [2.2, 21., 21., 2.2]}, index=[5, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(5), pd.DataFrame({'a': [5., 3000., 4000., 50.], 'b': [2.2, 21., 21., 2.2]}, index=[5, 30, 40, 50]))
        self.assertEqual(tsvc.fetch_meta_data(5), {})
        self.assertEqual(tsvc.fetch_meta_data(6), {'description': 'Test time series', 'author': 'someone'})
        self.assertEqual(tsvc.fetch_meta_data(7), {'description': 'Something more informative', 'author': 'someone'})
        self.assertEqual(tsvc.fetch_meta_data(8), {'author': 'someone', 'description': 'Something more informative'})
        self.assertNotEqual(tsvc.fetch_meta_data(8), {'description': 'Something more informative', 'author': 'someone'})
        self.assertEqual(tsvc.fetch_meta_data(9), {'author': 'someone'})
        pdt.assert_frame_equal(tsvc.fetch_time_series(10), pd.DataFrame({'a': [5., 3000., 4000., 50.], 'b': [2.2, 21., 21., 2.2], 'c': [100., 200., 300., 400.]}, index=[5, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(11), pd.DataFrame({'a': [5., 3000., 4000., 50.], 'c': [100., 200., 300., 400.]}, index=[5, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(12), pd.DataFrame({'c': [100., 200., 300., 400.], 'a': [5., 3000., 4000., 50.]}, index=[5, 30, 40, 50]))
        self.assertEqual(tsvc.fetch_meta_data(12), {'author': 'someone'})
        
