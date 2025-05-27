import os
import pickle

import thalesians.tsvc.caches as caches

class PickleCache(caches.Cache):
    def __init__(self, dir_path):
        self._dir_path = dir_path
    
    def __len__(self):
        return len([fn for fn in os.listdir(self._dir_path) if fn.endswith('.pickle')])
        
    def __contains__(self, key):
        return os.path.exists(os.path.join(self._dir_path, f'{key}.pickle'))
    
    def __getitem__(self, key):
        file_name = os.path.join(self._dir_path, f'{key}.pickle')
        if not os.path.exists(file_name):
            raise KeyError(f"Key {key} not found in cache.")
        with open(file_name, 'rb') as file:
            return pickle.load(file)
    
    def __setitem__(self, key, value):
        file_name = os.path.join(self._dir_path, f'{key}.pickle')
        with open(file_name, 'wb') as file:
            pickle.dump(value, file)
    
    def __delitem__(self, key):
        file_name = os.path.join(self._dir_path, f'{key}.pickle')
        if os.path.exists(file_name):
            os.remove(file_name)
        else:
            raise KeyError(f"Key {key} not found in cache.")
    
    def keys(self):
        return [int(fn[:-7]) for fn in os.listdir(self._dir_path) if fn.endswith('.pickle')]
    
    def clear(self):
        for key in self.keys():
            self.__delitem__(key)
