import os

import thalesians.tsvc.vc as vc
import thalesians.tsvc.delta_logs.pickle_delta_log as pickledl
import thalesians.tsvc.meta_data_caches.pickle_meta_data_cache as meta_data_caches
import thalesians.tsvc.revision_caches.pickle_revision_cache as revision_caches
import thalesians.tsvc.store as store
import thalesians.tsvc.ts_impls.pandas_ts_impl as pdtsimpl

class PickleStore(store.Store):
    LIST_FILE_NAME = 'list.csv'
    ENCODING = 'utf-8'
    
    def __init__(self, dir_path):
        super().__init__()
        self._dir_path = dir_path
        
    def keys(self):
        with open(os.path.join(self._dir_path, self.LIST_FILE_NAME), 'rt', encoding=self.ENCODING) as file:
            for line in file:
                stripped_line = line.strip()
                split_line = stripped_line.split(',')
                if len(split_line) >= 2:
                    yield split_line[0]
    
    def __len__(self):
        return len(list(self.keys()))

    def __contains__(self, key):
        return key in self.keys()
    
    def add(self, key):
        index = 0
        while os.path.exists(os.path.join(self._dir_path, f'{index}')):
            index += 1
        os.makedirs(os.path.join(self._dir_path, f'{index}'))
        with open(os.path.join(self._dir_path, self.LIST_FILE_NAME), 'at', encoding=self.ENCODING) as file:
            file.write(f'{key},{index}\n')
            
    def _get_index(self, key):
        with open(os.path.join(self._dir_path, self.LIST_FILE_NAME), 'rt', encoding=self.ENCODING) as file:
            for line in file:
                stripped_line = line.strip()
                split_line = stripped_line.split(',')
                if len(split_line) >= 2 and split_line[0] == key:
                    return int(split_line[1])
        raise KeyError(f"Key {key} not found in store.")
    
    def __getitem__(self, key):
        index = self._get_index(key)    
        delta_log = pickledl.PickleDeltaLog(dir_path=os.path.join(self._dir_path, str(index)))
        time_series_impl = pdtsimpl.PandasTimeSeriesImpl()
        revision_cache_dir_path = os.path.join(self._dir_path, str(index), 'revision_cache')
        os.makedirs(revision_cache_dir_path, exist_ok=True)
        revision_cache = revision_caches.PickleRevisionCache(dir_path=revision_cache_dir_path)
        meta_data_cache_dir_path = os.path.join(self._dir_path, str(index), 'meta_data_cache')
        meta_data_cache = meta_data_caches.PickleMetaDataCache(dir_path=meta_data_cache_dir_path)
        return vc.TimeSeriesVersionControl(delta_log=delta_log, time_series_impl=time_series_impl, revision_cache=revision_cache, meta_data_cache=meta_data_cache)
