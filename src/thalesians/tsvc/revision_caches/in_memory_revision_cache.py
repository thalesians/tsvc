import thalesians.tsvc.revision_caches as revisioncaches

class InMemoryRevisionCache(revisioncaches.RevisionCache):
    def __init__(self, max_size=None):
        super().__init__()
        self._cache = {}
        self._max_size = max_size if max_size is not None else float('inf')

    def __len__(self):
        return len(self._cache)

    def __contains__(self, key):
        return key in self._cache

    def __getitem__(self, key):
        return self._cache[key]

    def __setitem__(self, key, value):
        self._cache[key] = value
        if len(self._cache) > self._max_size:
            # Remove the oldest item if the cache exceeds max size
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]

    def __delitem__(self, key):
        del self._cache[key]

    def keys(self):
        return self._cache.keys()

    def clear(self):
        self._cache.clear()
