import abc

class Store(abc.ABC):
    @abc.abstractmethod
    def keys(self):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    def __len__(self):
        return len(self.keys())

    @abc.abstractmethod     
    def __contains__(self, key):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abc.abstractmethod
    def add(self, key):
        raise NotImplementedError("This method should be implemented by subclasses")

    @abc.abstractmethod
    def __getitem__(self, key):
        raise NotImplementedError("This method should be implemented by subclasses")
