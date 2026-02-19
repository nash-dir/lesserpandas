from .core import DataFrame

class GroupBy:
    def __init__(self, df, by):
        self.df = df
        self.by = by
        if by not in df.columns:
            raise KeyError(f"Column '{by}' not found")

        # Group indices: {group_key: [row_idx1, row_idx2, ...]}
        self.groups = {}
        by_col = df._data[by]
        
        for idx in range(df.shape[0]):
            key = by_col[idx]
            if key not in self.groups:
                self.groups[key] = []
            self.groups[key].append(idx)

    def _aggregate(self, func_name):
        """Helper to aggregate numeric columns."""
        # Result structure: {col: [val_group1, val_group2, ...]}
        result_data = {self.by: []}
        
        # Identify numeric columns (excluding 'by' column)
        numeric_cols = []
        for col in self.df.columns:
            if col == self.by:
                continue
            # Simple check: check first non-None value if it's a number
            first_val = next((x for x in self.df._data[col] if x is not None), None)
            if isinstance(first_val, (int, float)):
                numeric_cols.append(col)
                result_data[col] = []

        # Iterate over groups (sorted by key for deterministic output)
        sorted_keys = sorted(self.groups.keys(), key=lambda x: (x is None, x))
        
        for key in sorted_keys:
            indices = self.groups[key]
            result_data[self.by].append(key)

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
