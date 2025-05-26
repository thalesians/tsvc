import abc
import copy
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
        self._inverse = None        
        
    @property
    def timestamp(self):
        return self._timestamp
    
    @property
    def user(self):
        return self._user

    def effective_index(self, index=None):
        if index is None: index = self._index
        return index if index >= 0 else self._row_count_before + index + 1

    def inverse(self):
        return self._inverse
    
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
            self._inverse = DeleteRowsDelta(row_count_before + len(subdata_after), self._index, len(self._subdata_after), copy.deepcopy(self._subdata_after), inverse=self)

    @property
    def subdata_after(self):
        return self._subdata_after

class UpdateRowsDelta(Delta):
    def __init__(self, row_count_before, index, subdata_before, subdata_after, columns=None, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        assert len(subdata_before) == len(subdata_after)
        assert self._index + len(subdata_after) <= row_count_before
        self._subdata_after = subdata_after
        self._columns = columns
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = UpdateRowsDelta(row_count_before, self._index, copy.deepcopy(subdata_after), copy.deepcopy(subdata_before), inverse=self)

    @property
    def subdata_after(self):
        return self._subdata_after
    
    @property
    def columns(self):
        return self._columns

class DeleteRowsDelta(Delta):
    def __init__(self, row_count_before, index, count, subdata_before, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        self._index = index
        self._count = count
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = InsertRowsDelta(row_count_before - count, self._index, copy.deepcopy(subdata_before), inverse=self)

    @property
    def count(self):
        return self._count

class AppendColumnsDelta(Delta):
    def __init__(self, row_count_before, columns_to_add, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, 0, timestamp, user)
        self._columns_to_add = columns_to_add
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = DeleteColumnsDelta(row_count_before, copy.deepcopy(columns_to_add), inverse=self)
            
    @property
    def columns_to_add(self):
        return self._columns_to_add
            
class DeleteColumnsDelta(Delta):
    def __init__(self, row_count_before, columns_to_delete, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, 0, timestamp, user)
        self._columns_to_delete = columns_to_delete
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = AppendColumnsDelta(row_count_before, copy.deepcopy(columns_to_delete), inverse=self)
            
    @property
    def columns_to_delete(self):
        return self._columns_to_delete

class RenameColumnsDelta(Delta):
    def __init__(self, row_count_before, columns_mapping, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, 0, timestamp, user)
        self._columns_mapping = columns_mapping
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = RenameColumnsDelta(row_count_before, {v: k for k, v in columns_mapping.items()}, inverse=self)
            
    @property
    def columns_mapping(self):
        return self._columns_mapping
    
class ReorderColumnsDelta(Delta):
    def __init__(self, row_count_before, column_ordering_before, column_ordering_after, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, 0, timestamp, user)
        self._column_ordering_before = column_ordering_before
        self._column_ordering_after = column_ordering_after
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = ReorderColumnsDelta(row_count_before, column_ordering_after, column_ordering_before, inverse=self)
            
    @property
    def column_ordering_before(self):
        return self._column_ordering_before
    
    @property
    def column_ordering_after(self):
        return self._column_ordering_after

class InsertMetaDataDelta(Delta):
    def __init__(self, row_count_before, index, meta_data, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        self._meta_data = meta_data
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = DeleteMetaDataDelta(row_count_before + 1, self._index, copy.deepcopy(meta_data), inverse=self)
            
    @property
    def meta_data(self):
        return self._meta_data
    
class UpdateMetaDataDelta(Delta):
    def __init__(self, row_count_before, index, meta_data_before, meta_data_after, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        self._meta_data_after = meta_data_after
        self._meta_data_before = meta_data_before
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = UpdateMetaDataDelta(row_count_before, self._index, copy.deepcopy(meta_data_after), copy.deepcopy(meta_data_before), inverse=self)
            
    @property
    def meta_data_after(self):
        return self._meta_data_after
    
    @property
    def meta_data_before(self):
        return self._meta_data_before
    
class DeleteMetaDataDelta(Delta):
    def __init__(self, row_count_before, index, meta_data, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, index, timestamp, user)
        self._meta_data = meta_data
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = InsertMetaDataDelta(row_count_before - 1, self._index, copy.deepcopy(meta_data), inverse=self)
            
    @property
    def meta_data(self):
        return self._meta_data

class ReorderMetaDataDelta(Delta):
    def __init__(self, row_count_before, meta_data_ordering_before, meta_data_ordering_after, inverse=None, timestamp=None, user=None):
        super().__init__(row_count_before, 0, timestamp, user)
        self._meta_data_ordering_before = meta_data_ordering_before
        self._meta_data_ordering_after = meta_data_ordering_after
        if inverse is not None:
            self._inverse = inverse
        else:
            self._inverse = ReorderMetaDataDelta(row_count_before, meta_data_ordering_after, meta_data_ordering_before, inverse=self)
            
    @property
    def meta_data_ordering_before(self):
        return self._meta_data_ordering_before
    
    @property
    def meta_data_ordering_after(self):
        return self._meta_data_ordering_after
