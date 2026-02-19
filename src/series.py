class Series:
    def __init__(self, data: list, name: str = None):
        if not isinstance(data, list):
            raise TypeError(f"Series data must be a list, got {type(data)}")
        self._data = data
        self.name = name

    def __len__(self):
        return len(self._data)

    def __getitem__(self, index):
        return self._data[index]

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        name_str = f", name='{self.name}'" if self.name else ""
        return f"Series({self._data}{name_str})"

    def _compare(self, other, op):
        result = []
        for x in self._data:
            if x is None:
                result.append(False) # None comparison usually results in False or specific handling
                continue
            
            val = other
            if isinstance(other, Series):
                 # Element-wise comparison not fully implemented for Series vs Series in this simple version
                 # Assumption: other is scalar for now based on requirements
                 pass

            try:
                res = op(x, val)
                result.append(res)
            except TypeError:
                result.append(False)
        return result

    def _arithmetic_op(self, other, op):
        result = []
        is_series = isinstance(other, Series)
        
        for i in range(len(self._data)):
            val1 = self._data[i]
            
            if is_series:
                if i < len(other._data):
                    val2 = other._data[i]
                else:
                    val2 = None # Should not happen if aligned, but safe
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
                    
        return Series(result, name=self.name)

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
            # Defensive coding: check None first?
            # Or just try-except.
            # User request: "우아하게 처리" (Handle gracefully).
            # If x is None, usually we preserve None unless function handles it.
            # Let's pass x to func, but catch errors.
            try:
                res = func(x)
                result.append(res)
            except Exception:
                # If error, append None? 
                # Or maybe the function expects None?
                # If strict "None in -> None out" is desired, check x is None.
                # But let's act like map: try to map, fail to None.
                result.append(None)
        return Series(result, name=self.name)

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
        # Returns boolean series. None -> None? Or False?
        # Pandas .str.contains returns NaN for NaN.
        # Let's return False for non-string to be "safe" or None?
        # Request says: "에러를 뱉지 않고 None 또는 False".
        # Let's return False for non-string, but None for None.
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
