import abc

class DeltaLog(abc.ABC):
    def __init__(self):
        pass
    
    @abc.abstractmethod
    def append(self, delta):
        raise NotImplementedError
    
    @abc.abstractmethod
    def __getitem__(self, index):
        raise NotImplementedError
    
    @abc.abstractmethod
    def __len__(self):
        raise NotImplementedError
