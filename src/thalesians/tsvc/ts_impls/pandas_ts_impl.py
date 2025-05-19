import pandas as pd

import thalesians.tsvc.ts_impls as tsimpls

class PandasTimeSeriesImpl(tsimpls.TimeSeriesImpl):
    def __init__(self):
        pass
    
    def fetch_data_copy(self, data, index=0, count=None):
        return data.iloc[index:index+(len(data) if count is None else count)]

    def apply_insert_rows_delta(self, data, delta):
        if data is None:
            data = delta.subdata_after
        else:
            first = data.iloc[:delta.effective_index()]
            second = delta.subdata_after
            third = data.iloc[delta.effective_index():]
            data = pd.concat([first, second, third])
        return data

    def apply_update_rows_delta(self, data, delta):
        data.iloc[delta.effective_index():delta.effective_index() + len(delta.subdata_after)] = delta.subdata_after
        return data

    def apply_delete_rows_delta(self, data, delta):
        del data.iloc[delta.effective_index():delta.effective_index() + delta.count]
        return data
