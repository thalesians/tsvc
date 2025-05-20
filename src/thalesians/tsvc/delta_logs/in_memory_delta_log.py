import thalesians.tsvc.delta_logs as deltalogs

class InMemoryDeltaLog(deltalogs.DeltaLog):
    def __init__(self):
        super().__init__()
        self._deltas = []

    def append(self, delta):
        self._deltas.append(delta)
        return len(self._deltas) - 1
    
    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(index.start or 0, index.stop, index.step or 1)]
        else:
            return self._deltas[index]

    def __len__(self):
        return len(self._deltas)
