import copy

import pandas as pd

import thalesians.tsvc.meta_data.dict_meta_data as dict_meta_data
import thalesians.tsvc.ts_impls as tsimpls

class PandasTimeSeriesImpl(tsimpls.TimeSeriesImpl):
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
        columns = delta.columns if delta.columns is not None else data.columns
        column_indices = [data.columns.get_loc(col) for col in columns]
        data.iloc[delta.effective_index():delta.effective_index() + len(delta.subdata_after), column_indices] = delta.subdata_after
        return self.fetch_data_copy(data)

    def apply_delete_rows_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        first = data.iloc[:delta.effective_index()]
        second = data.iloc[delta.effective_index() + delta.count:]
        return self.fetch_data_copy(pd.concat([first, second]))

    def apply_append_columns_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        for column, column_data in delta.columns_to_append.items():
            data[column] = copy.deepcopy(column_data)
        return self.fetch_data_copy(data)
    
    def apply_delete_columns_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        for column in delta.columns_to_delete:
            data.drop(column, axis=1, inplace=True)
        return self.fetch_data_copy(data)
    
    def apply_rename_columns_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        data.rename(columns=delta.columns_mapping, inplace=True)
        return self.fetch_data_copy(data)
    
    def apply_reorder_columns_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        data = data[delta.column_ordering_after]
        return self.fetch_data_copy(data)
    
    def apply_insert_meta_data_delta(self, meta_data, delta):
        if meta_data is None:
            meta_data = dict_meta_data.DictMetaData()
        for key, value in delta.meta_data.items():
            assert key not in meta_data, f"Key '{key}' already exists in meta data"
            meta_data[key] = value
        return meta_data
    
    def apply_update_meta_data_delta(self, meta_data, delta):
        for key, value in delta.meta_data_after.items():
            assert key in meta_data, f"Key '{key}' does not exist in meta data"
            meta_data[key] = value
        return meta_data
    
    def apply_delete_meta_data_delta(self, meta_data, delta):
        for key in delta.meta_data_to_delete.keys():
            assert key in meta_data, f"Key '{key}' does not exist in meta data"
            del meta_data[key]
        return meta_data
    
    def apply_reorder_meta_data_delta(self, meta_data, delta):
        meta_data.reorder(delta.meta_data_ordering_after)
        return meta_data
