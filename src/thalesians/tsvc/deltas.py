import abc
import datetime as dt
import os

class Delta(abc.ABC):
    def __init__(self, row_count_before, index, timestamp=None, user=None):        
        assert row_count_before >= 0
        self._row_count_before = row_count_before
        self._index = self.effective_index(index)
        assert self._index >= 0
        assert self._index <= self._row_count_before
        self._timestamp = timestamp if timestamp is not None else dt.datetime.now(dt.timezone.utc).timestamp()
        self._user = user if user is not None else os.getlogin()        
        
    @property
    def timestamp(self):
        return self._timestamp
    
    @property
    def user(self):
        return self._user

    def effective_index(self, index=None):
        if index is None: index = self._index
        return index if index >= 0 else self._row_count_before + index + 1

    @abc.abstractmethod
    def inverse(self):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    def __repr__(self):
        return f"{self.__class__.__name__}(row_count_before={self._row_count_before}, index={self._index}, timestamp={self._timestamp}, user={self._user})"

    def __str__(self):
        return f"{self.__class__.__name__}(row_count_before={self._row_count_before}, index={self._index}, timestamp={self._timestamp}, user={self._user})"

class InsertRowsDelta(Delta):
    def __init__(self, row_count_before, index, subdata_after, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        self._subdata_after = subdata_after
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = DeleteRowsDelta(row_count_before + len(subdata_after), self._index, len(self._subdata_after), self._subdata_after.copy(), inverse=self)

    @property
    def subdata_after(self):
        return self._subdata_after

    def inverse(self):
        return self._inverse

class UpdateRowsDelta(Delta):
    def __init__(self, row_count_before, index, subdata_before, subdata_after, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        assert len(subdata_before) == len(subdata_after)
        assert self._index + len(subdata_after) < row_count_before
        self._subdata_after = subdata_after
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = UpdateRowsDelta(row_count_before, self._index, subdata_after.copy(), subdata_before.copy(), inverse=self)

    @property
    def subdata_after(self):
        return self._subdata_after

    def inverse(self):
        return self._inverse

class DeleteRowsDelta(Delta):
    def __init__(self, row_count_before, index, count, subdata_before, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        self._index = index
        self._count = count
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = InsertRowsDelta(row_count_before - count, self._index, subdata_before.copy(), inverse=self)

    @property
    def count(self):
        return self._count

    def inverse(self):
        return self._inverse
