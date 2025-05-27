import abc

class MetaData(object):
    def __init__(self):
        pass
    
    @abc.abstractmethod
    def __len__(self):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abc.abstractmethod
    def __contains__(self, key):
        raise NotImplementedError("This method should be implemented by subclasses")

    @abc.abstractmethod
    def __getitem__(self, key):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abc.abstractmethod
    def __setitem__(self, key, value):
        raise NotImplementedError("This method should be implemented by subclasses")
        
    @abc.abstractmethod
    def __delitem__(self, key):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abc.abstractmethod
    def keys(self):
        raise NotImplementedError("This method should be implemented by subclasses")
    
    @abc.abstractmethod
    def reorder(self, new_order):
        raise NotImplementedError("This method should be implemented by subclasses")

    def __eq__(self, other):
        return list(self.keys()) == list(other.keys()) and all(self[key] == other[key] for key in self.keys())
    