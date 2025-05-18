import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)  # @ReservedAssignment

import abc

class Delta(abc.ABC):
    def __init__(self, row_count_before, index):
        assert row_count_before >= 0
        self._row_count_before = row_count_before
        self._index = self.effective_index(index)
        assert self._index >= 0
        assert self._index <= self._row_count_before

    def effective_index(self, index=None):
        if index is None: index = self._index
        return index if index >= 0 else self._row_count_before + index + 1

    @abc.abstractmethod
    def inverse(self):
        raise NotImplementedError

class InsertRowsDelta(Delta):
    def __init__(self, row_count_before, index, subdata_after, inverse=None):
        super().__init__(row_count_before, index)
        self._subdata_after = subdata_after
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = DeleteRowsDelta(row_count_before + len(subdata_after), self._index, len(self._subdata_after), self._subdata_after, inverse=self)

    @property
    def subdata_after(self):
        return self._subdata_after

    def inverse(self):
        return self._inverse

class UpdateRowsDelta(Delta):
    def __init__(self, row_count_before, index, subdata_before, subdata_after, inverse=None):
        super().__init__(row_count_before, index)
        assert len(subdata_before) == len(subdata_after)
        assert self._index + len(subdata_after) < len(row_count_before)
        self._subdata_after = subdata_after
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = UpdateRowsDelta(row_count_before, self._index, subdata_after, subdata_before, inverse=self)

    @property
    def subdata_after(self):
        return self._subdata_after

    def inverse(self):
        return self._inverse

class DeleteRowsDelta(Delta):
    def __init__(self, row_count_before, index, count, subdata_before, inverse=None):
        super().__init__(row_count_before, index)
        self._index = index
        self._count = count
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = InsertRowsDelta(row_count_before - count, self._index, subdata_before, inverse=self)

    @property
    def count(self):
        return self._count

    def inverse(self):
        return self._inverse

class TimeSeriesVersionControl(abc.ABC):
    def __init__(self, fetch_data_copy):
        self._fetch_data_copy = fetch_data_copy
        self._delta_log = []
        self._revision_cache = {}
        self._row_count = 0

    def apply_delta(self, data, delta):
        if isinstance(delta, InsertRowsDelta):
            data = self.apply_insert_rows_delta(data, delta)
        elif isinstance(delta, UpdateRowsDelta):
            data = self.apply_update_rows_delta(data, delta)
        elif isinstance(delta, DeleteRowsDelta):
            data = self.apply_delete_rows_delta(data, delta)
        else:
            raise ValueError('Invalid delta type')
        return data

    @abc.abstractmethod
    def apply_insert_rows_delta(self, data, delta):
        raise NotImplementedError

    @abc.abstractmethod
    def apply_update_rows_delta(self, data, delta):
        raise NotImplementedError

    @abc.abstractmethod
    def apply_delete_rows_delta(self, data, delta):
        raise NotImplementedError

    def insert_rows(self, new_data, index=-1):
        to_insert = self._fetch_data_copy(new_data)
        delta = InsertRowsDelta(self._row_count, index, to_insert)
        self._row_count += len(to_insert)
        self._delta_log.append(delta)

    def update_rows(self, new_data, index):
        delta = UpdateRowsDelta(self._row_count, index, self._fetch_data_copy(self._data, index, len(new_data)), self._fetch_data_copy(new_data))
        self._delta_log.append(delta)

    def delete_rows(self, index, count):
        delta = DeleteRowsDelta(self._row_count, index, count, self._fetch_data_copy(self._data, index, count))
        self._row_count -= count
        self._delta_log.append(delta)

    def get_revision(self, revision, cache_result=True):
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
                for delta in self._delta_log[revision_at_min_distance - 1:revision - 1:-1]:
                    result = self.apply_delta(result, delta)
            if cache_result:
                self._revision_cache[revision] = result
        return result

class InMemoryPandasVersionControl(TimeSeriesVersionControl):
    def __init__(self):
        super().__init__(fetch_data_copy=lambda data, index=0, count=None: data.iloc[index:index+(len(data) if count is None else count)])

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

if __name__ == '__main__':
    import pandas as pd
    tsvc = InMemoryPandasVersionControl()
    tsvc.insert_rows(pd.DataFrame({'a': [10., 20.], 'b': [2.3, 2.1]}, index=[10, 20]))
    tsvc.insert_rows(pd.DataFrame({'a': [30., 40., 50.], 'b': [2.1, 2.1, 2.2]}, index=[30, 40, 50]))
    tsvc.insert_rows(pd.DataFrame({'a': [5], 'b': [2.2]}, index=[5]), index=0)
    print('Revision 0:')
    print(tsvc.get_revision(0))
    print('Revision 0:')
    print(tsvc.get_revision(0))
    print('Revision 1:')
    print(tsvc.get_revision(1))
    print('Revision 0:')
    print(tsvc.get_revision(0))
    print('Revision 1:')
    print(tsvc.get_revision(1))
    print('Revision 2:')
    print(tsvc.get_revision(2))
