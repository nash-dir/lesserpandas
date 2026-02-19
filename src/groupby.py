from .core import DataFrame

class GroupBy:
    def __init__(self, df, by, as_index=True):
        self.df = df
        self.by = by
        self.as_index = as_index
        
        # Normalize 'by' to always be a list for consistent internal handling
        if isinstance(by, str):
            self.by_cols = [by]
        elif isinstance(by, list):
            self.by_cols = by
        else:
             raise TypeError("Group key must be a string or list of strings")
            
        for col in self.by_cols:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found")

        # Group indices: {group_key_tuple: [row_idx1, row_idx2, ...]}
        self.groups = {}
        
        # Pre-fetch columns for performance
        by_data = [df._data[col] for col in self.by_cols]
        num_rows = df.shape[0]
        
        for idx in range(num_rows):
            # Create a tuple key
            key = tuple(col_data[idx] for col_data in by_data)
            
            if key not in self.groups:
                self.groups[key] = []
            self.groups[key].append(idx)

    def agg(self, func_dict):
        """Aggregate using a dictionary mapping columns to functions."""
        if not isinstance(func_dict, dict):
            raise TypeError("agg must be called with a dictionary")
            
        # Result structure
        result_data = {col: [] for col in self.by_cols} if not self.as_index or len(self.by_cols) > 1 else {}
        # If as_index=True and single by col, 'by' col becomes index, not data col.
        
        agg_cols = list(func_dict.keys())
        for col in agg_cols:
             result_data[col] = []
             
        # Sorting keys
        def safe_key(k):
            return tuple((x is None, x) for x in k)
        sorted_keys = sorted(self.groups.keys(), key=safe_key)
        
        result_index = []
        
        for key in sorted_keys:
            indices = self.groups[key]
            
            # Handle Grouping Keys
            if not self.as_index or len(self.by_cols) > 1:
                for i, col in enumerate(self.by_cols):
                    result_data[col].append(key[i])
            else:
                # Single key as index
                result_index.append(key[0])
            
            # Handle Aggregations
            for col, func_name in func_dict.items():
                if col not in self.df._data:
                     # Could raise error or fill None. Pandas raises KeyError usually.
                     # Let's be strict.
                     raise KeyError(f"Column '{col}' not found")
                
                values = [self.df._data[col][i] for i in indices if self.df._data[col][i] is not None]
                
                val = None
                if func_name == 'count':
                    # Count includes None? standard pandas count excludes NaN.
                    # My previous implementation was len(values) where values excluded None. Correct.
                    val = len(values)
                elif not values:
                    val = None
                elif func_name == 'sum':
                    val = sum(values)
                elif func_name == 'mean':
                    val = sum(values) / len(values)
                elif func_name == 'min':
                    val = min(values)
                elif func_name == 'max':
                    val = max(values)
                else:
                    raise ValueError(f"Unknown aggregation function '{func_name}'")
                
                result_data[col].append(val)
        
        return DataFrame(result_data, index=result_index if (self.as_index and len(self.by_cols) == 1) else None)

    def _aggregate(self, func_name):
        """Helper to aggregate numeric columns."""
        # Identify numeric columns (excluding 'by' columns)
        numeric_cols = []
        for col in self.df.columns:
            if col in self.by_cols:
                continue
            # Simple check
            first_val = next((x for x in self.df._data[col] if x is not None), None)
            if isinstance(first_val, (int, float)):
                numeric_cols.append(col)
        
        # Build func_dict
        func_dict = {col: func_name for col in numeric_cols}
        return self.agg(func_dict)

    def sum(self):
        return self._aggregate('sum')

    def mean(self):
        return self._aggregate('mean')

    def count(self):
        return self._aggregate('count')
