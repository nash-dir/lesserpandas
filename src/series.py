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
