import pandas as pd

import thalesians.tsvc.ts_impls as tsimpls

class PandasTimeSeriesImpl(tsimpls.TimeSeriesImpl):
    def __init__(self):
        pass
    
    def fetch_data_copy(self, data, index=0, count=None):
        return data.iloc[index:index+(len(data) if count is None else count)].copy(deep=True)

    def apply_insert_rows_delta(self, data, delta):
        if data is None:
            data = delta.subdata_after.copy(deep=True)
        else:
            first = data.iloc[:delta.effective_index()]
            second = delta.subdata_after
            third = data.iloc[delta.effective_index():]
            data = pd.concat([first, second, third])
        return self.fetch_data_copy(data)

    def apply_update_rows_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        data.iloc[delta.effective_index():delta.effective_index() + len(delta.subdata_after)] = delta.subdata_after
        return self.fetch_data_copy(data)

    def apply_delete_rows_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        first = data.iloc[:delta.effective_index()]
        second = data.iloc[delta.effective_index() + delta.count:]
        return self.fetch_data_copy(pd.concat([first, second]))
