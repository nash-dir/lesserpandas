class Series:
    def __init__(self, data: list, index=None, name: str = None):
        if not isinstance(data, list):
            raise TypeError(f"Series data must be a list, got {type(data)}")
        self._data = data
        self.name = name
        
        if index is None:
            self.index = list(range(len(data)))
        else:
            if len(index) != len(data):
                raise ValueError(f"Index length {len(index)} must match data length {len(data)}")
            self.index = list(index)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        # 1. Slice: Return new Series with sliced data and index
        if isinstance(item, slice):
            new_data = self._data[item]
            new_index = self.index[item]
            return Series(new_data, index=new_index, name=self.name)
            
        # 2. Boolean Indexing (List of bools): Return filtered Series
        if isinstance(item, list) and item and isinstance(item[0], bool):
            if len(item) != len(self._data):
                raise ValueError(f"Item length {len(item)} does not match Series length {len(self._data)}")
            new_data = [val for val, keep in zip(self._data, item) if keep]
            new_index = [idx for idx, keep in zip(self.index, item) if keep]
            return Series(new_data, index=new_index, name=self.name)

        # 3. Simple integer index
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        name_str = f", name='{self.name}'" if self.name else ""
        return f"Series({self._data}, index={self.index}{name_str})"

    def _compare(self, other, op):
        result = []
        is_series = isinstance(other, Series)
        
        if is_series and len(self) != len(other):
             raise ValueError("Can only compare identically-labeled Series objects")

        for i, x in enumerate(self._data):
            if x is None:
                result.append(False) 
                continue
            
            if is_series:
                 val = other._data[i]
            else:
                 val = other

            try:
                res = op(x, val)
                result.append(res)
            except TypeError:
                result.append(False)
        return Series(result, index=self.index, name=self.name)

    def _arithmetic_op(self, other, op):
        result = []
        is_series = isinstance(other, Series)
        
        if is_series:
            if len(self) != len(other):
                raise ValueError("Can only compare identically-labeled Series objects")
        
        for i in range(len(self._data)):
            val1 = self._data[i]
            
            if is_series:
                val2 = other._data[i]
            else:
                val2 = other
                
            if val1 is None or val2 is None:
                result.append(None)
            else:
                try:
                    res = op(val1, val2)
                    result.append(res)
                except (TypeError, ZeroDivisionError):
                    result.append(None)
                    
        return Series(result, index=self.index, name=self.name)

    def __add__(self, other):
        return self._arithmetic_op(other, lambda x, y: x + y)

    def __sub__(self, other):
        return self._arithmetic_op(other, lambda x, y: x - y)

    def __mul__(self, other):
        return self._arithmetic_op(other, lambda x, y: x * y)

    def __truediv__(self, other):
        return self._arithmetic_op(other, lambda x, y: x / y)

    def __eq__(self, other):
        return self._compare(other, lambda x, y: x == y)

    def __ne__(self, other):
        return self._compare(other, lambda x, y: x != y)

    def __lt__(self, other):
        return self._compare(other, lambda x, y: x < y)

    def __le__(self, other):
        return self._compare(other, lambda x, y: x <= y)

    def __gt__(self, other):
        return self._compare(other, lambda x, y: x > y)

    def __ge__(self, other):
        return self._compare(other, lambda x, y: x >= y)

    def isin(self, values):
        """Check if elements are in values."""
        # optimize with set
        if not isinstance(values, set):
            values = set(values)
        return Series([x in values for x in self._data], name=self.name)

    def apply(self, func):
        """Apply function to each element safely."""
        result = []
        for x in self._data:
            try:
                res = func(x)
                result.append(res)
            except Exception:
                result.append(None)
        return Series(result, name=self.name)

    def astype(self, dtype):
        """Cast Series elements to dtype."""
        result = []
        for x in self._data:
            if x is None:
                result.append(None)
            else:
                try:
                    result.append(dtype(x))
                except (ValueError, TypeError):
                    # Raise or None?
                    # User requested 'appropriate defense logic'
                    # Standard pandas raises.
                    # But user also mentioned "smoothly pass".
                    # Let's try strict first as per standard practice, but maybe allow errors='ignore' later.
                    # Given "appropriate defense logic", catching and raising ValueError with clear message is good.
                    raise ValueError(f"Could not cast value '{x}' to {dtype}")
        return Series(result, index=self.index, name=self.name)

    def value_counts(self):
        """Return a Series containing counts of unique values."""
        counts = {}
        for x in self._data:
            if x in counts:
                counts[x] += 1
            else:
                counts[x] = 1
        
        # Sort descending
        sorted_items = sorted(counts.items(), key=lambda item: item[1], reverse=True)
        
        # Create Series
        # Index = unique values, Data = counts
        indices = [item[0] for item in sorted_items]
        data = [item[1] for item in sorted_items]
        
        return Series(data, index=indices, name=self.name)

    @property
    def str(self):
        return StringMethods(self)

class StringMethods:
    def __init__(self, series):
        self._series = series

    def _str_op(self, op):
        result = []
        for x in self._series._data:
            if x is None:
                result.append(None) # Propagate None
                continue
            if not isinstance(x, str):
                result.append(None) # Non-string becomes None (safe)
                continue
            
            try:
                result.append(op(x))
            except Exception:
                result.append(None)
        return Series(result, name=self._series.name)

    def lower(self):
        return self._str_op(lambda x: x.lower())

    def upper(self):
        return self._str_op(lambda x: x.upper())

    def replace(self, old, new):
        return self._str_op(lambda x: x.replace(old, new))

    def contains(self, pat):
        result = []
        for x in self._series._data:
            if x is None:
                result.append(None)
                continue
            if not isinstance(x, str):
                result.append(False) # Strict type check?
                continue
            result.append(pat in x)
        return Series(result, name=self._series.name)
