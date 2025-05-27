import unittest

import polars as pl
import polars.testing as pt

import thalesians.tsvc.vc as vc
import thalesians.tsvc.delta_logs.in_memory_delta_log as mem
import thalesians.tsvc.meta_data_caches.in_memory_meta_data_cache as meta_data_caches
import thalesians.tsvc.revision_caches.in_memory_revision_cache as revision_caches
import thalesians.tsvc.ts_impls.polars_ts_impl as polars_ts_impl

class TestInMemoryDeltaLogPolarsTSImpl(unittest.TestCase):
    def test_basic(self):
        delta_log = mem.InMemoryDeltaLog()
        time_series_impl = polars_ts_impl.PolarsTimeSeriesImpl()
        revision_cache = revision_caches.InMemoryRevisionCache()
        meta_data_cache = meta_data_caches.InMemoryMetaDataCache()
        tsvc = vc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache, meta_data_cache=meta_data_cache)
        tsvc.insert_rows(pl.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}))
        tsvc.insert_rows(pl.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}))
        tsvc.insert_rows(pl.DataFrame({'a': [5.], 'b': [2.2]}), index=0)
        tsvc.update_rows(pl.DataFrame({'a': [200., 300., 400.], 'b': [21., 21., 21.]}), index=2)
        tsvc.delete_rows(1, 2)
        self.assertEqual(list(tsvc.get_revisions()), [0, 1, 2, 3, 4])
        pt.assert_frame_equal(tsvc.fetch_time_series(0), pl.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(0), pl.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(1), pl.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(0), pl.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(1), pl.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(2), pl.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(3), pl.DataFrame({'a': [5., 10., 200., 300., 400., 50.], 'b': [2.2, 2.3, 21., 21., 21., 2.2]}))
        pt.assert_frame_equal(tsvc.fetch_time_series(4), pl.DataFrame({'a': [5., 300., 400., 50.], 'b': [2.2, 21., 21., 2.2]}))
