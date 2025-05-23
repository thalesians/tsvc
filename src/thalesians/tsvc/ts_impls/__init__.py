import abc

class TimeSeriesImpl(abc.ABC):
    def __init__(self):
        pass
    
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
    
    @abc.abstractmethod
    def apply_append_columns_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_delete_columns_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_rename_columns_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_reorder_columns_delta(self, data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_insert_meta_data_delta(self, meta_data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_update_meta_data_delta(self, meta_data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_delete_meta_data_delta(self, meta_data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")

    @abc.abstractmethod
    def apply_reorder_meta_data_delta(self, meta_data, delta):
        raise NotImplementedError("This method should be implemented by subclasses.")
