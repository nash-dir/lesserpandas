from .core import DataFrame

class GroupBy:
    def __init__(self, df, by):
        self.df = df
        self.by = by
        
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

    def _aggregate(self, func_name):
        """Helper to aggregate numeric columns."""
        # Result structure: {col: [val_group1, val_group2, ...]}
        result_data = {col: [] for col in self.by_cols}
        
        # Identify numeric columns (excluding 'by' columns)
        numeric_cols = []
        for col in self.df.columns:
            if col in self.by_cols:
                continue
            # Simple check: check first non-None value if it's a number
            first_val = next((x for x in self.df._data[col] if x is not None), None)
            if isinstance(first_val, (int, float)):
                numeric_cols.append(col)
                result_data[col] = []

        # Iterate over groups (sorted by key for deterministic output)
        # Sorting tuples works naturally in Python (None needs care if mixed, 
        # but here keys are tuples of values. If values contain None, we need safe sort)
        # Let's assume for simplicity safe sort isn't strictly enforced for Group keys yet
        # or use a lambda that handles None in tuples if needed.
        # Python 3: (1, None) < (1, 2) crashes? No, None vs Int crashes.
        # Ensure we have a safe key for sorting.
        def safe_key(k):
            return tuple((x is None, x) for x in k)

        sorted_keys = sorted(self.groups.keys(), key=safe_key)
        
        for key in sorted_keys:
            indices = self.groups[key]
            
            # Add grouping keys to result
            for i, col in enumerate(self.by_cols):
                result_data[col].append(key[i])

            for col in numeric_cols:
                values = [self.df._data[col][i] for i in indices if self.df._data[col][i] is not None]
                
                if func_name == 'count':
                    val = len(values)
                elif not values: # sum/mean on empty or all-None
                    val = None
                elif func_name == 'sum':
                    val = sum(values)
                elif func_name == 'mean':
                    val = sum(values) / len(values)
                else:
                    val = None
                
                result_data[col].append(val)

        return DataFrame(result_data)

    def sum(self):
        return self._aggregate('sum')

    def mean(self):
        return self._aggregate('mean')

    def count(self):
        return self._aggregate('count')
