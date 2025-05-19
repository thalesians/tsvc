import abc

class TimeSeriesImpl(abc.ABC):
    @abc.abstractmethod
    def fetch_data_copy(self, data, index=0, count=None):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_insert_rows_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_update_rows_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_delete_rows_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")
    
