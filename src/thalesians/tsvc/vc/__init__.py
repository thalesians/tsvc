import thalesians.tsvc.deltas as deltas

class TimeSeriesVersionControl(object):
    def __init__(self, delta_log, time_series_impl, revision_cache):
        self._delta_log = delta_log
        self._time_series_impl = time_series_impl
        self._revision_cache = revision_cache
        self._row_count = 0

    def apply_delta(self, data, delta):
        if isinstance(delta, deltas.InsertRowsDelta):
            data = self._time_series_impl.apply_insert_rows_delta(data, delta)
        elif isinstance(delta, deltas.UpdateRowsDelta):
            data = self._time_series_impl.apply_update_rows_delta(data, delta)
        elif isinstance(delta, deltas.DeleteRowsDelta):
            data = self._time_series_impl.apply_delete_rows_delta(data, delta)
        else:
            raise ValueError('Invalid delta type')
        return data

    def insert_rows(self, new_data, index=-1):
        to_insert = self._time_series_impl.fetch_data_copy(new_data)
        delta = deltas.InsertRowsDelta(self._row_count, index, to_insert)
        self._row_count += len(to_insert)
        self._delta_log.append(delta)

    def update_rows(self, new_data, index):
        revision_before = self.fetch_revision(len(self._delta_log) - 1)
        old_data = self._time_series_impl.fetch_data_copy(revision_before, index, len(new_data))
        delta = deltas.UpdateRowsDelta(self._row_count, index, old_data, self._time_series_impl.fetch_data_copy(new_data))
        self._delta_log.append(delta)

    def delete_rows(self, index, count):
        revision_before = self.fetch_revision(len(self._delta_log) - 1)
        old_data = self._time_series_impl.fetch_data_copy(revision_before, index, count)
        delta = deltas.DeleteRowsDelta(self._row_count, index, count, old_data)
        self._delta_log.append(delta)
        self._row_count -= count
        
    def get_revisions(self):
        return range(len(self._delta_log))

    def fetch_revision(self, revision, cache_result=True):
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
                    result = self.apply_delta(result, delta)
            elif revision_at_min_distance < revision:
                result = self._revision_cache[revision_at_min_distance]
                for delta in self._delta_log[revision_at_min_distance + 1:revision + 1]:
                    result = self.apply_delta(result, delta)
            else:
                result = self._revision_cache[revision_at_min_distance]
                for delta in self._delta_log[revision_at_min_distance:revision:-1]:
                    result = self.apply_delta(result, delta.inverse())
            if cache_result:
                self._revision_cache[revision] = self._time_series_impl.fetch_data_copy(result)
        return result
