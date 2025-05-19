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
