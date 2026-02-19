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

    def __setitem__(self, key, value):
        from .series import Series
        
        new_col_data = []
        target_len = self._length
        
        if self._length == 0 and not self._data:
            # Case: Empty DataFrame, setting first column
            if isinstance(value, list):
                target_len = len(value)
            elif isinstance(value, Series):
                target_len = len(value)
            else:
                # Setting scalar to empty DF? Ambiguous length.
                # Let's assume length 1 or just fail if not list/series
                # Standard pandas allows setting scalar if index exists.
                # Here we have no index.
                # If scalar, treat as length 1? Or just store as [value]
                target_len = 1 
            self._length = target_len

        # 1. Series
        if isinstance(value, Series):
            if len(value) != target_len:
                raise ValueError(f"Length of values ({len(value)}) does not match length of index ({target_len})")
            new_col_data = list(value._data)
            
        # 2. List
        elif isinstance(value, list):
            if len(value) != target_len:
                raise ValueError(f"Length of values ({len(value)}) does not match length of index ({target_len})")
            new_col_data = list(value)
            
        # 3. Scalar
        else:
            new_col_data = [value] * target_len
            
        self._data[key] = new_col_data

    def sort_values(self, by, ascending=True):
        if by not in self._data:
            raise KeyError(f"Column '{by}' not found")
        
        col_data = self._data[by]
        indices = list(range(self._length))
        
        # Safe sort key for None
        # (is_none, value) tuple:
        # False (0) < True (1), so valid values come before None
        # ascending=True: [Valid..., None]
        # ascending=False: [None..., Valid] -> Wait, this puts None first?
        # If we want None last always:
        # key = lambda i: (col_data[i] is None, col_data[i] if col_data[i] is not None else 0)
        # ascending=True: (0, val) < (1, 0) -> Valid < None. Correct.
        # ascending=False: (1, 0) > (0, val) -> None > Valid. None comes first in desc sort (reverse=True).
        # To strictly mimick pandas (NaN last), we need a custom key that respects direction
        # But 'ascending' param in sort() acts on the result of key comparison.
        # Let's stick to safe comparison: None > Valid.
        # Then Ascending: Valid... None
        # Descending: None... Valid
        # The user requested: "None은 무조건 맨 마지막에 오거나 예외 에러가 나지 않도록"
        # If user wants None last always, we can separate indices
        
        none_indices = [i for i in indices if col_data[i] is None]
        valid_indices = [i for i in indices if col_data[i] is not None]
        
        valid_indices.sort(key=lambda i: col_data[i], reverse=not ascending)
        
        # Merge: valid then none (Assuming None always last for now, or controllable)
        # Standard Pandas `na_position='last'` is default.
        sorted_indices = valid_indices + none_indices
            
        return self.iloc[sorted_indices]

    def to_dict(self, orient="records"):
        if orient != "records":
            raise ValueError("Only orient='records' is currently supported")
        
        records = []
        for i in range(self.shape[0]):
            row = {}
            for col in self.columns:
                row[col] = self._data[col][i]
            records.append(row)
        return records

    def fillna(self, value):
        new_data = {}
        for col, values in self._data.items():
            new_data[col] = [val if val is not None else value for val in values]
        return DataFrame(new_data)

    def dropna(self):
        # Identify indices to keep
        keep_indices = []
        for i in range(self.shape[0]):
            has_none = False
            for col in self.columns:
                if self._data[col][i] is None:
                    has_none = True
                    break
            if not has_none:
                keep_indices.append(i)
        
        # Create new DataFrame with kept rows
        if not keep_indices:
            return DataFrame({col: [] for col in self.columns})
        
        return self.iloc[keep_indices]

    def to_csv(self, filepath):
        from .io import to_csv
        to_csv(self, filepath)

    def to_json(self, filepath):
        from .io import to_json
        to_json(self, filepath)

    def groupby(self, by):
        from .groupby import GroupBy
        return GroupBy(self, by)

    def merge(self, right, on: str, how: str = 'inner'):
        from .merge import merge
        return merge(self, right, on, how)

    def __repr__(self):
        rows, cols = self.shape
        return f"<LesserDataFrame: {rows} rows x {cols} cols>"
