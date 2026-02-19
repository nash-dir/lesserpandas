from .series import Series
from .indexing import _iLocIndexer

class DataFrame:
    def __init__(self, data=None):
        self._data = {}
        self._length = 0

        if data is None:
            return

        if isinstance(data, dict):
            # 1. Dict of lists case
            # Verify lengths
            lengths = [len(v) for v in data.values()]
            if lengths and len(set(lengths)) > 1:
                raise ValueError("All arrays must be of the same length")
            
            # Copy data to avoid side effects and ensure list type
            self._data = {k: list(v) for k, v in data.items()}
            if lengths:
                self._length = lengths[0]

        elif isinstance(data, list):
            # 2. List of dicts case
            if not data:
                return

            # Collect all column names
            all_keys = set()
            for row in data:
                if isinstance(row, dict):
                    all_keys.update(row.keys())
            
            # Initialize empty lists for each column
            self._data = {k: [] for k in all_keys}

            # Fill data
            for row in data:
                for k in all_keys:
                    self._data[k].append(row.get(k, None))
            
            self._length = len(data)

        else:
            raise TypeError("Data must be a dict or list of dicts")

    @property
    def columns(self):
        """Returns a list of column names."""
        return list(self._data.keys())

    @property
    def shape(self):
        """Returns a tuple representing the dimensionality of the DataFrame."""
        if not self._data:
            return (0, 0)
        return (self._length, len(self._data))

    @property
    def iloc(self):
        return _iLocIndexer(self)

    def __getitem__(self, item):
        # 1. String: Return Series
        if isinstance(item, str):
            if item not in self._data:
                 raise KeyError(f"Column '{item}' not found")
            return Series(self._data[item], name=item)

        # 2. List of Strings: Return new DataFrame with selected columns
        elif isinstance(item, list) and item and isinstance(item[0], str):
            new_data = {col: self._data[col] for col in item if col in self._data}
            # Create new DataFrame from dict of lists
            return DataFrame(new_data)

        # 3. List of Booleans: Boolean Indexing
        elif isinstance(item, list) and item and isinstance(item[0], bool):
            if len(item) != self._length:
                 raise ValueError(f"Item length {len(item)} does not match DataFrame length {self._length}")
            
            new_data = {}
            for col, values in self._data.items():
                new_column = [val for val, keep in zip(values, item) if keep]
                new_data[col] = new_column
            return DataFrame(new_data)
        
        # 4. Empty list
        elif isinstance(item, list) and not item:
            return DataFrame({})

        else:
             raise TypeError(f"Invalid argument type for indexing: {type(item)}")

    def groupby(self, by: str):
        from .groupby import GroupBy
        return GroupBy(self, by)

    def merge(self, right, on: str, how: str = 'inner'):
        from .merge import merge
        return merge(self, right, on, how)

    def __repr__(self):
        rows, cols = self.shape
        return f"<LesserDataFrame: {rows} rows x {cols} cols>"
