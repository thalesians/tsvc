import os
import pickle

import thalesians.tsvc.deltalogs as deltalogs

class PickleDeltaLog(deltalogs.DeltaLog):
    def __init__(self, dir_path):
        super().__init__()
        self._dir_path = dir_path

    def append(self, delta):
        revision = len(self)
        file_name = os.path.join(self._dir_path, f'{revision}.pickle')
        with open(file_name, 'wb') as file:
            pickle.dump(delta, file)
        return revision

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(index.start or 0, index.stop, index.step or 1)]
        else:
            file_name = os.path.join(self._dir_path, f'{index}.pickle')
            with open(file_name, 'rb') as file:
                return pickle.load(file)

    def __len__(self):
        return len([fn for fn in os.listdir(self._dir_path) if fn.endswith('.pickle')])
