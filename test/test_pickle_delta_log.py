import os
import tempfile
import unittest

import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc.vc as vc
import thalesians.tsvc.delta_logs.pickle_delta_log as pickledl
import thalesians.tsvc.meta_data_caches.pickle_meta_data_cache as meta_data_caches
import thalesians.tsvc.revision_caches.pickle_revision_cache as revision_caches
import thalesians.tsvc.ts_impls.pandas_ts_impl as pdtsimpl

class TestPickleDeltaLog(unittest.TestCase):
    def test_basic(self):
        temp_dir = tempfile.TemporaryDirectory(delete=False)
        delta_log_dir_path = temp_dir.name
        delta_log = pickledl.PickleDeltaLog(dir_path=delta_log_dir_path)
        time_series_impl = pdtsimpl.PandasTimeSeriesImpl()
        revision_cache_dir_path = os.path.join(delta_log_dir_path, 'revision_cache')
        os.makedirs(revision_cache_dir_path, exist_ok=True)
        revision_cache = revision_caches.PickleRevisionCache(dir_path=revision_cache_dir_path)
        meta_data_caches_dir_path = os.path.join(delta_log_dir_path, 'meta_data_cache')
        os.makedirs(meta_data_caches_dir_path, exist_ok=True)
        meta_data_cache = meta_data_caches.PickleMetaDataCache(dir_path=meta_data_caches_dir_path)
        tsvc = vc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache, meta_data_cache=meta_data_cache)
        tsvc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        tsvc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50]))
        tsvc.insert_rows(pd.DataFrame({'a': [5.], 'b': [2.2]}, index=[5]), index=0)
        tsvc.update_rows(pd.DataFrame({'a': [200., 300., 400.], 'b': [21., 21., 21.]}, index=[20, 30, 40]), index=2)
        tsvc.delete_rows(1, 2)
        self.assertEqual(list(tsvc.get_revisions()), [0, 1, 2, 3, 4])
        pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(2), pd.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}, index=[5, 10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(3), pd.DataFrame({'a': [5., 10., 200., 300., 400., 50.], 'b': [2.2, 2.3, 21., 21., 21., 2.2]}, index=[5, 10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.fetch_time_series(4), pd.DataFrame({'a': [5., 300., 400., 50.], 'b': [2.2, 21., 21., 2.2]}, index=[5, 30, 40, 50]))
