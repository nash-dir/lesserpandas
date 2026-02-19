from .series import Series
from .indexing import _iLocIndexer, _LocIndexer

class DataFrame:
    def __init__(self, data=None, index=None):
        self._data = {}
        self._length = 0
        self.index = []

        if data is None:
            if index is not None:
                self.index = list(index)
                self._length = len(self.index)
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
            else:
                 # Empty dict but maybe index provided?
                 if index is not None:
                     self._length = len(index)

        elif isinstance(data, list):
            # 2. List of dicts case
            if not data:
                if index is not None:
                    self.index = list(index)
                    self._length = len(self.index)
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
            
        # Initialize Index
        if index is None:
            self.index = list(range(self._length))
        else:
            if len(index) != self._length:
                raise ValueError(f"Index length {len(index)} does not match data length {self._length}")
            self.index = list(index)

    @property
    def columns(self):
        """Returns a list of column names."""
        return list(self._data.keys())

    @property
    def shape(self):
        """Returns a tuple representing the dimensionality of the DataFrame."""
        if not self._data and self._length == 0:
            return (0, 0)
        return (self._length, len(self._data))

    @property
    def iloc(self):
        return _iLocIndexer(self)

    @property
    def loc(self):
        return _LocIndexer(self)

    def __getitem__(self, item):
        from .series import Series
        
        # Handle Series as indexer (convert to list)
        # Check by type or name to avoid potential circular import/reloading issues during tests
        if isinstance(item, Series) or type(item).__name__ == 'Series':
            item = list(item)

        # 1. String: Return Series
        if isinstance(item, str):
            if item not in self._data:
                 raise KeyError(f"Column '{item}' not found")
            return Series(self._data[item], index=self.index, name=item)

        # 2. List of Strings: Return new DataFrame with selected columns
        elif isinstance(item, list) and item and isinstance(item[0], str):
            new_data = {col: self._data[col] for col in item if col in self._data}
            # Create new DataFrame from dict of lists, preserving index
            return DataFrame(new_data, index=self.index)

        # 3. List of Booleans: Boolean Indexing
        elif isinstance(item, list) and item and isinstance(item[0], bool):
            if len(item) != self._length:
                 raise ValueError(f"Item length {len(item)} does not match DataFrame length {self._length}")
            
            new_data = {}
            for col, values in self._data.items():
                new_column = [val for val, keep in zip(values, item) if keep]
                new_data[col] = new_column
            
            new_index = [idx for idx, keep in zip(self.index, item) if keep]
            return DataFrame(new_data, index=new_index)
        
        # 4. Empty list
        elif isinstance(item, list) and not item:
            return DataFrame({}, index=[])

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
                target_len = 1 
            self._length = target_len
            # Update index if it was empty
            if not self.index:
                self.index = list(range(target_len))

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

    def sort_values(self, by, ascending=True, na_position='last'):
        if by not in self._data:
            raise KeyError(f"Column '{by}' not found")
        
        if na_position not in ('first', 'last'):
             raise ValueError("na_position must be 'first' or 'last'")

        col_data = self._data[by]
        indices = list(range(self._length))
        
        none_indices = [i for i in indices if col_data[i] is None]
        valid_indices = [i for i in indices if col_data[i] is not None]
        
        valid_indices.sort(key=lambda i: col_data[i], reverse=not ascending)
        
        if na_position == 'last':
            sorted_indices = valid_indices + none_indices
        else: # first
            sorted_indices = none_indices + valid_indices
            
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

    def groupby(self, by, as_index=True):
        from .groupby import GroupBy
        return GroupBy(self, by, as_index=as_index)

    def apply(self, func, axis=0):
        if axis == 0:
            # Column-wise application
            # Return Series/Dict of results
            result = {}
            for col in self.columns:
                # Apply to Series
                result[col] = func(self[col])
            # If result values are scalars, maybe return Series?
            # If result values are Series, return DataFrame?
            # For "small tasks", usually we want reduction or transformation.
            # If transformation (Series -> Series), we can rebuild DF.
            # Let's see:
            first_val = next(iter(result.values()), None)
            from .series import Series
            if isinstance(first_val, Series):
                 # Convert dict of Series back to DataFrame
                 # Assume aligned properties? Not always true.
                 # Simplified: Just return DataFrame if possible.
                 return DataFrame({k: v._data for k, v in result.items()})
            else:
                 return result # Return dict as "Series" with index=cols

        elif axis == 1:
            # Row-wise application
            # Iterate rows, create dict, apply func
            result = []
            for i in range(self._length):
                row = self.iloc[i] # This returns dict
                try:
                    res = func(row)
                    result.append(res)
                except Exception:
                    result.append(None)
            from .series import Series
            return Series(result)
        
        else:
             raise ValueError("Axis must be 0 or 1")

    def merge(self, right, on: str, how: str = 'inner'):
        from .merge import merge
        return merge(self, right, on, how)

    def drop(self, columns):
        """Return a new DataFrame with specified columns removed."""
        if isinstance(columns, str):
            columns = [columns]
            
        new_data = {col: self._data[col] for col in self.columns if col not in columns}
        
        # Check if any columns in 'columns' were not found? Pandas raises KeyError.
        for col in columns:
            if col not in self._data:
                 raise KeyError(f"Column '{col}' not found")
                 
        return DataFrame(new_data, index=self.index)

    def rename(self, columns):
        """Return a new DataFrame with renamed columns."""
        if not isinstance(columns, dict):
            raise TypeError("columns must be a dictionary")
            
        new_data = {}
        for col in self.columns:
            new_name = columns.get(col, col)
            new_data[new_name] = self._data[col]
            
        return DataFrame(new_data, index=self.index)

    def assign(self, **kwargs):
        """Assign new columns to a DataFrame."""
        # Create a copy of existing data
        new_data = self._data.copy()
        
        for key, value in kwargs.items():
            if callable(value):
                # If value is a callable, it is called with self
                # We need to construct a robust self representation or just pass self
                # BUT 'self' might be modified if we pass it directly? No, assign creates NEW DF.
                # However, the callable acts on the *current* state of the DF *before* this assignment?
                # Pandas: "The callable must accept the DataFrame and return the Series/value."
                # Does it see previous assignments in the same assign call?
                # Pandas: "Later items in **kwargs may refer to newly created or modified columns in 'df';
                # items are computed and assigned in order."
                # So we should update `new_data` incrementally and allow callables to see updates?
                # To do that, we'd need to construct a temporary DF for each step if the callable expects a DF.
                # But creating full DF each time is expensive.
                # Let's approximate: pass `self` (original) to callable.
                # WAIT: Pandas docs say "The callable must accept the DataFrame". 
                # "Since Python 3.6, **kwargs order is preserved."
                
                # To support referencing newly assigned columns, we would need to wrap new_data in a DF 
                # or pass a proxy.
                # For simplicity in 'LesserPandas', let's stick to: callable receives `self`.
                # Limitation: Can't refer to columns created *earlier in the same assign call*.
                
                # Update: The user requested "chaining convenience". 
                # Let's stick to simple implementation: pass `self`.
                
                computed_value = value(self)
                
                # Handle Series result from callable
                from .series import Series
                if isinstance(computed_value, Series):
                    new_data[key] = list(computed_value)
                else:
                     new_data[key] = computed_value if isinstance(computed_value, list) else [computed_value] * self._length
            else:
                # Value assignment
                from .series import Series
                if isinstance(value, Series):
                    new_data[key] = list(value)
                elif isinstance(value, list):
                    new_data[key] = value
                else:
                    new_data[key] = [value] * self._length

        return DataFrame(new_data, index=self.index)

    def __repr__(self):
        rows, cols = self.shape
        return f"<LesserDataFrame: {rows} rows x {cols} cols>"
