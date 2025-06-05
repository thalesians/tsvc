# thalesians.tsvc - Time Series Version Control

A lightweight, efficient version control system specifically designed for time series data.

## Overview

`thalesians.tsvc` provides a specialized solution for versioning time series datasets with a delta-based approach. This Python library offers fine-grained tracking of row-level and column-level changes, complete with automatic audit trails and efficient bidirectional versioning capabilities.

## Key features

* **Delta-based architecture:** Records only the changes between versions rather than complete snapshots.
* **Row-level operations:** Precisely tracks insertions, updates, and deletions at the individual record level.
* **Column-level operations:** Precisely tracks addition, renaming, deletion, and reordering of columns.
* **Metadata operations:** Precisely tracks changes to the time series metadata in the form of key-value pairs.
* **Bidirectional versioning:** Each change stores its inverse for efficient undo/redo capabilities.
* **Flexible persistence:** Choice between format-agnostic in-memory storage or on-disk serialization.
* **Pythonic design:** Clean, intuitive API that follows Python conventions.

## Why `thalesians.tsvc`?

Time series data presents unique versioning challenges that general-purpose VCS tools aren't optimized to handle:

* High-frequency updates to specific portions of data;
* Need for row-level and column-level (rather than file-level) change tracking;
* Performance requirements for large datasets;
* Specialized audit needs for financial and scientific contexts.

`thalesians.tsvc` addresses these challenges with a purpose-built solution that integrates seamlessly with Python data analysis workflows.

## Installation

<pre>
pip install thalesians.tsvc
</pre>

## Minimal(ish) example

<pre>
import pandas as pd
import pandas.testing as pdt

import thalesians.tsvc.vc as vc
import thalesians.tsvc.delta_logs.in_memory_delta_log as mem
import thalesians.tsvc.meta_data_caches.in_memory_meta_data_cache as meta_data_caches
import thalesians.tsvc.revision_caches.in_memory_revision_cache as revision_caches
import thalesians.tsvc.ts_impls.pandas_ts_impl as pandas_ts_impl

delta_log = mem.InMemoryDeltaLog()
time_series_impl = pandas_ts_impl.PandasTimeSeriesImpl()
revision_cache = revision_caches.InMemoryRevisionCache()
meta_data_cache = meta_data_caches.InMemoryMetaDataCache()
tsvc = vc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache, meta_data_cache=meta_data_cache)
tsvc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
tsvc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50]))
tsvc.insert_rows(pd.DataFrame({'a': [5.], 'b': [2.2]}, index=[5]), index=0)
tsvc.update_rows(pd.DataFrame({'a': [200., 300., 400.], 'b': [21., 21., 21.]}, index=[20, 30, 40]), index=2)
tsvc.delete_rows(1, 2)
tsvc.update_rows(pd.DataFrame({'a': [3000., 4000.]}, index=[30, 40]), columns=['a'], index=1)
tsvc.insert_meta_data({'description': 'Test time series', 'author': 'someone'})
tsvc.update_meta_data({'description': 'Something more informative'})
tsvc.reorder_meta_data(['author', 'description'])
tsvc.delete_meta_data(['description'])
tsvc.append_columns({'c': [100., 200., 300., 400.]})
tsvc.delete_columns(['b'])
tsvc.reorder_columns(['c', 'a'])

assert list(tsvc.get_revisions()) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
pdt.assert_frame_equal(tsvc.fetch_time_series(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(0), pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
pdt.assert_frame_equal(tsvc.fetch_time_series(1), pd.DataFrame({'a': [10., 20., 30., 40., 50.], 'b': [2.3, 2.1, 2.1, 2.1, 2.2]}, index=[10, 20, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(2), pd.DataFrame({'a': [5., 10., 20., 30., 40., 50.], 'b': [2.2, 2.3, 2.1, 2.1, 2.1, 2.2]}, index=[5, 10, 20, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(3), pd.DataFrame({'a': [5., 10., 200., 300., 400., 50.], 'b': [2.2, 2.3, 21., 21., 21., 2.2]}, index=[5, 10, 20, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(4), pd.DataFrame({'a': [5., 300., 400., 50.], 'b': [2.2, 21., 21., 2.2]}, index=[5, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(5), pd.DataFrame({'a': [5., 3000., 4000., 50.], 'b': [2.2, 21., 21., 2.2]}, index=[5, 30, 40, 50]))
assert tsvc.fetch_meta_data(5) == {}
assert tsvc.fetch_meta_data(6) == {'description': 'Test time series', 'author': 'someone'}
assert tsvc.fetch_meta_data(7) == {'description': 'Something more informative', 'author': 'someone'}
assert tsvc.fetch_meta_data(8) == {'author': 'someone', 'description': 'Something more informative'}
assert tsvc.fetch_meta_data(9) == {'author': 'someone'}
pdt.assert_frame_equal(tsvc.fetch_time_series(10), pd.DataFrame({'a': [5., 3000., 4000., 50.], 'b': [2.2, 21., 21., 2.2], 'c': [100., 200., 300., 400.]}, index=[5, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(11), pd.DataFrame({'a': [5., 3000., 4000., 50.], 'c': [100., 200., 300., 400.]}, index=[5, 30, 40, 50]))
pdt.assert_frame_equal(tsvc.fetch_time_series(12), pd.DataFrame({'c': [100., 200., 300., 400.], 'a': [5., 3000., 4000., 50.]}, index=[5, 30, 40, 50]))
assert tsvc.fetch_meta_data(12) == {'author': 'someone'}

print('Code ran successfully.')
</pre>
