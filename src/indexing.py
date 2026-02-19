class _iLocIndexer:
    def __init__(self, df):
        self._df = df

    def __getitem__(self, item):
        # 1. Integer: Return row as dict
        if isinstance(item, int):
            row = {}
            # Handle negative index
            idx = item
            if idx < 0:
                idx += self._df.shape[0]
            
            if idx < 0 or idx >= self._df.shape[0]:
                 raise IndexError("DataFrame index out of range")

            for col, values in self._df._data.items():
                row[col] = values[idx]
            return row

        # 2. Slice: Return new DataFrame
        elif isinstance(item, slice):
            new_data = {}
            for col, values in self._df._data.items():
                new_data[col] = values[item]
            return self._df.__class__(new_data)

        # 3. List of integers: Return new DataFrame
        elif isinstance(item, list) and all(isinstance(x, int) for x in item):
             new_data = {}
             for col, values in self._df._data.items():
                 new_column = [values[i] for i in item]
                 new_data[col] = new_column
             return self._df.__class__(new_data)
        
        else:
            raise TypeError("Invalid argument for iloc")
