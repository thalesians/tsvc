import tempfile
import unittest

import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc.store.pickle_store

class TestPickleDeltaLog(unittest.TestCase):
    def test_basic(self):
        temp_dir = tempfile.TemporaryDirectory(delete=False)        
        store_dir_path = temp_dir.name
        store = thalesians.tsvc.store.pickle_store.PickleStore(dir_path=store_dir_path)
        store.add("TS1")
        tsvc = store["TS1"]
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
