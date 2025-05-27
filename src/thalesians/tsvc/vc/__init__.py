import copy

import thalesians.tsvc.deltas as deltas
from thalesians.tsvc.meta_data import dict_meta_data

class TimeSeriesVersionControl(object):
    def __init__(self, delta_log, time_series_impl, revision_cache, meta_data_cache):
        self._delta_log = delta_log
        self._time_series_impl = time_series_impl
        self._revision_cache = revision_cache
        self._row_count = 0
        self._meta_data_cache = meta_data_cache

    def apply_time_series_delta(self, data, delta, ignore_unknown_deltas=True):
        if isinstance(delta, deltas.InsertRowsDelta):
            data = self._time_series_impl.apply_insert_rows_delta(data, delta)
        elif isinstance(delta, deltas.UpdateRowsDelta):
            data = self._time_series_impl.apply_update_rows_delta(data, delta)
        elif isinstance(delta, deltas.DeleteRowsDelta):
            data = self._time_series_impl.apply_delete_rows_delta(data, delta)
        elif isinstance(delta, deltas.AppendColumnsDelta):
            data = self._time_series_impl.apply_append_columns_delta(data, delta)
        elif isinstance(delta, deltas.DeleteColumnsDelta):
            data = self._time_series_impl.apply_delete_columns_delta(data, delta)
        elif isinstance(delta, deltas.RenameColumnsDelta):
            data = self._time_series_impl.apply_rename_columns_delta(data, delta)
        elif isinstance(delta, deltas.ReorderColumnsDelta):
            data = self._time_series_impl.apply_reorder_columns_delta(data, delta)
        else:
            if not ignore_unknown_deltas: raise ValueError('Unknown delta type')
        return data
    
    def apply_meta_data_delta(self, meta_data, delta, ignore_unknown_deltas=True):
        if isinstance(delta, deltas.InsertMetaDataDelta):
            meta_data = self._time_series_impl.apply_insert_meta_data_delta(meta_data, delta)
        elif isinstance(delta, deltas.UpdateMetaDataDelta):
            meta_data = self._time_series_impl.apply_update_meta_data_delta(meta_data, delta)
        elif isinstance(delta, deltas.DeleteMetaDataDelta):
            meta_data = self._time_series_impl.apply_delete_meta_data_delta(meta_data, delta)
        elif isinstance(delta, deltas.ReorderMetaDataDelta):
            meta_data = self._time_series_impl.apply_reorder_meta_data_delta(meta_data, delta)
        else:
            if not ignore_unknown_deltas: raise ValueError('Unknown delta type')
        return meta_data

    def insert_rows(self, new_data, index=-1):
        to_insert = self._time_series_impl.fetch_data_copy(new_data)
        delta = deltas.InsertRowsDelta(self._row_count, index, to_insert)
        self._row_count += len(to_insert)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1

    def update_rows(self, new_data, index, columns=None):
        revision_before = self.fetch_time_series(len(self._delta_log) - 1)
        old_data = self._time_series_impl.fetch_data_copy(revision_before, index, len(new_data))
        delta = deltas.UpdateRowsDelta(self._row_count, index, old_data, self._time_series_impl.fetch_data_copy(new_data), columns=columns)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1

    def delete_rows(self, index, count):
        revision_before = self.fetch_time_series(len(self._delta_log) - 1)
        old_data = self._time_series_impl.fetch_data_copy(revision_before, index, count)
        delta = deltas.DeleteRowsDelta(self._row_count, index, count, old_data)
        self._delta_log.append(delta)
        self._row_count -= count
        return len(self._delta_log) - 1
        
    def append_columns(self, columns_to_append):
        delta = deltas.AppendColumnsDelta(self._row_count, columns_to_append)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def delete_columns(self, columns_to_delete):
        revision_before = self.fetch_time_series(len(self._delta_log) - 1)
        for column in columns_to_delete:
            if column not in revision_before.columns:
                raise ValueError(f"Column '{column}' does not exist in the time series")
            columns_to_delete_dict = {column: revision_before[column] for column in columns_to_delete}
        delta = deltas.DeleteColumnsDelta(self._row_count, columns_to_delete=columns_to_delete_dict)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def rename_columns(self, columns_mapping):
        delta = deltas.RenameColumnsDelta(self._row_count, columns_mapping)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def reorder_columns(self, column_ordering_after):
        revision_before = self.fetch_time_series(len(self._delta_log) - 1)
        column_ordering_before = list(revision_before.columns)
        delta = deltas.ReorderColumnsDelta(self._row_count, column_ordering_before, column_ordering_after)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def insert_meta_data(self, meta_data):
        delta = deltas.InsertMetaDataDelta(self._row_count, 0, meta_data)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def update_meta_data(self, meta_data):
        meta_data_before = self.fetch_meta_data(len(self._delta_log) - 1)
        delta = deltas.UpdateMetaDataDelta(self._row_count, 0, meta_data_before, meta_data)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def delete_meta_data(self, keys):
        meta_data_before = self.fetch_meta_data(len(self._delta_log) - 1)
        delta = deltas.DeleteMetaDataDelta(self._row_count, 0, {key: meta_data_before[key] for key in keys})
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
    
    def reorder_meta_data(self, keys):
        meta_data_before = self.fetch_meta_data(len(self._delta_log) - 1)
        if len(keys) != len(set(keys)):
            raise ValueError('Keys must be unique')
        if len(keys) != len(meta_data_before):
            raise ValueError('Keys must match the current meta data keys')
        if not all(key in meta_data_before for key in keys):
            raise ValueError('Not all keys are present in the current meta data')
        delta = deltas.ReorderMetaDataDelta(self._row_count, list(meta_data_before.keys()), keys)
        self._delta_log.append(delta)
        return len(self._delta_log) - 1
        
    def get_revisions(self):
        return range(len(self._delta_log))

    def fetch_time_series(self, revision, cache_result=True):
        if revision < 0 or revision >= len(self._delta_log):
            raise IndexError('Revision out of bounds')
        result = None
        if revision in self._revision_cache:
            result = self._revision_cache[revision]
        else:
            min_distance = float('inf')
            revision_at_min_distance = None
            for a_revision in self._revision_cache.keys():
                distance = abs(revision - a_revision)
                if distance < min_distance:
                    min_distance = distance
                    revision_at_min_distance = a_revision
            if revision_at_min_distance is None:
                result = None
                for delta in self._delta_log[:revision + 1]:
                    result = self.apply_time_series_delta(result, delta)
            elif revision_at_min_distance < revision:
                result = self._revision_cache[revision_at_min_distance]
                for delta in self._delta_log[revision_at_min_distance + 1:revision + 1]:
                    result = self.apply_time_series_delta(result, delta)
            else:
                result = self._revision_cache[revision_at_min_distance]
                for delta in self._delta_log[revision_at_min_distance:revision:-1]:
                    result = self.apply_time_series_delta(result, delta.inverse())
            if cache_result:
                self._revision_cache[revision] = self._time_series_impl.fetch_data_copy(result)
        return result

    def fetch_meta_data(self, revision, cache_result=True):
        if revision < 0 or revision >= len(self._delta_log):
            raise IndexError('Revision out of bounds')
        result = dict_meta_data.DictMetaData()
        if revision in self._meta_data_cache:
            result = self._meta_data_cache[revision]
        else:
            min_distance = float('inf')
            revision_at_min_distance = None
            for a_revision in self._meta_data_cache.keys():
                distance = abs(revision - a_revision)
                if distance < min_distance:
                    min_distance = distance
                    revision_at_min_distance = a_revision
            if revision_at_min_distance is None:
                result = dict_meta_data.DictMetaData()
                for delta in self._delta_log[:revision + 1]:
                    result = self.apply_meta_data_delta(result, delta)
            elif revision_at_min_distance < revision:
                result = copy.deepcopy(self._meta_data_cache[revision_at_min_distance])
                for delta in self._delta_log[revision_at_min_distance + 1:revision + 1]:
                    result = self.apply_meta_data_delta(result, delta)
            else:
                result = copy.deepcopy(self._meta_data_cache[revision_at_min_distance])
                for delta in self._delta_log[revision_at_min_distance:revision:-1]:
                    result = self.apply_meta_data_delta(result, delta.inverse())
            if cache_result:
                self._meta_data_cache[revision] = copy.deepcopy(result)
        return copy.deepcopy(result)
