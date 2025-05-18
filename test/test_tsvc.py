import unittest

import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc

class TestTsvc(unittest.TestCase):
    def test_tsvc(self):
        tsvc = thalesians.tsvc.InMemoryPandasVersionControl()
        tsvc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        tsvc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50]))
        tsvc.insert_rows(pd.DataFrame({'a': [5.], 'b': [2.2]}, index=[5]), index=0)
        pdt.assert_frame_equal(tsvc.get_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.get_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.get_revision(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.get_revision(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
        pdt.assert_frame_equal(tsvc.get_revision(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
        pdt.assert_frame_equal(tsvc.get_revision(2), pd.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}, index=[5, 10, 20, 30, 40, 50]))
