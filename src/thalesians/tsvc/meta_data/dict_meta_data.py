import thalesians.tsvc.meta_data as meta_data_module

class DictMetaData(meta_data_module.MetaData):
    def __init__(self):
        super().__init__()
        self._meta_data = {}
        
    def __len__(self):
        return len(self._meta_data)
    
    def __contains__(self, key):
        return key in self._meta_data

    def __getitem__(self, key):
        if key in self._meta_data:
            return self._meta_data[key]
        raise KeyError(f"Key '{key}' not found in meta data.")
    
    def __setitem__(self, key, value):
        self._meta_data[key] = value
        
    def __delitem__(self, key):
        if key in self._meta_data:
            del self._meta_data[key]
        else:
            raise KeyError(f"Key '{key}' not found in meta data.")
        
    def keys(self):
        return list(self._meta_data.keys())
    
    def reorder(self, new_order):
        if len(new_order) != len(self._meta_data):
            raise ValueError("New order length must match the length of the meta data.")
        
        new_meta_data = {}
        for i in range(len(new_order)):
            new_meta_data[new_order[i]] = self._meta_data[new_order[i]]
        
        self._meta_data = new_meta_data
