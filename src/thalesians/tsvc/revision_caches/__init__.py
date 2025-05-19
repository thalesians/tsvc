import abc

class RevisionCache(abc.ABC):
    @abc.abstractmethod
    def __len__(self):
        pass
        
    @abc.abstractmethod
    def __contains__(self, key):
        pass
    
    @abc.abstractmethod
    def __getitem__(self, key):
        pass
    
    @abc.abstractmethod
    def __setitem__(self, key, value):
        pass
    
    @abc.abstractmethod
    def __delitem__(self, key):
        pass
    
    @abc.abstractmethod
    def keys(self):
        pass
    
    @abc.abstractmethod
    def clear(self):
        pass
