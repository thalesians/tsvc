import copy

import polars as pl

import thalesians.tsvc.ts_impls as tsimpls

class PolarsTimeSeriesImpl(tsimpls.TimeSeriesImpl):
    def fetch_data_copy(self, data, index=0, count=None):
        return copy.deepcopy(data[index:index+(len(data) if count is None else count)])

    def apply_insert_rows_delta(self, data, delta):
        if data is None:
            data = copy.deepcopy(delta.subdata_after)
        else:
            first = data[:delta.effective_index()]
            second = delta.subdata_after
            third = data[delta.effective_index():]
            data = pl.concat([first, second, third])
        return self.fetch_data_copy(data)

    def apply_update_rows_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        replacement = copy.deepcopy(delta.subdata_after)
        index = delta.effective_index()
        count = len(delta.subdata_after)
        columns_to_update = list(data.columns) if delta.columns is None else delta.columns
        for column in data.columns:
            if column not in columns_to_update:
                replacement = replacement.with_columns(
                    data[column].slice(index, count).alias(column)
                )
        replacement = replacement.select(data.columns)
        return self.fetch_data_copy(pl.concat([
            data[:index],
            replacement,
            data[index + count:]
        ]))

    def apply_delete_rows_delta(self, data, delta):
        data = self.fetch_data_copy(data)
        first = data[:delta.effective_index()]
        second = data[delta.effective_index() + delta.count:]
        return self.fetch_data_copy(pl.concat([first, second]))

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
        for key, value in delta.meta_data.items():
            assert key not in meta_data, f"Key '{key}' already exists in meta data"
            meta_data[key] = value
        return meta_data
    
    def apply_update_meta_data_delta(self, meta_data, delta):
        for key, value in delta.meta_data.items():
            assert key in meta_data, f"Key '{key}' does not exist in meta data"
            meta_data[key] = value
        return meta_data
    
    def apply_delete_meta_data_delta(self, meta_data, delta):
        for key in delta.meta_data.keys():
            assert key in meta_data, f"Key '{key}' does not exist in meta data"
            del meta_data[key]
        return meta_data
    
    def apply_reorder_meta_data_delta(self, meta_data, delta):
        meta_data.reorder(delta.meta_data_ordering_after)
        return meta_data
