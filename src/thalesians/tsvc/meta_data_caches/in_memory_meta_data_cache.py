import thalesians.tsvc.caches.in_memory_cache as in_memory_cache
import thalesians.tsvc.meta_data_caches as meta_data_caches

class InMemoryMetaDataCache(meta_data_caches.MetaDataCache):
    def __init__(self, max_size=None):
        super().__init__()
        self._cache = in_memory_cache.InMemoryCache(max_size=max_size)

    def __len__(self):
        return len(self._cache)

    def __contains__(self, key):
        return key in self._cache

    def __getitem__(self, key):
        return self._cache[key]

    def __setitem__(self, key, value):
        self._cache[key] = value

    def __delitem__(self, key):
        del self._cache[key]

    def keys(self):
        return self._cache.keys()

    def clear(self):
        self._cache.clear()
