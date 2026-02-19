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
            
            # Slice the index as well
            new_index = self._df.index[item]
            return self._df.__class__(new_data, index=new_index)

        # 3. List of integers: Return new DataFrame
        elif isinstance(item, list) and all(isinstance(x, int) for x in item):
             new_data = {}
             for col, values in self._df._data.items():
                 new_column = [values[i] for i in item]
                 new_data[col] = new_column
             
             new_index = [self._df.index[i] for i in item]
             return self._df.__class__(new_data, index=new_index)
        
        else:
            raise TypeError("Invalid argument for iloc")


class _LocIndexer:
    def __init__(self, df):
        self._df = df

    def __getitem__(self, item):
        # 1. Single Label
        if not isinstance(item, (list, slice, bool)):
            # Find index of label
            try:
                # Find all occurrences or just the first? Pandas finds all if duplicate.
                # Requirement: "df.loc[label] (해당 인덱스 라벨을 가진 행을 dict로 반환)"
                # If unique, dict. If multiple, DataFrame?
                # Simple implementation: First occurrence for now, or assume unique?
                # "list(range(self._length))" implies unique default.
                # Let's support searching.
                
                indices = [i for i, x in enumerate(self._df.index) if x == item]
                if not indices:
                    raise KeyError(f"Label '{item}' not found in index")
                
                if len(indices) == 1:
                    # Return dict/Series (using iloc for dict return currently)
                    return self._df.iloc[indices[0]]
                else:
                    # Multiple matches -> DataFrame
                    return self._df.iloc[indices]

            except TypeError:
                # In case item is not comparable?
                raise KeyError(f"Label '{item}' not found in index")

        # 2. List of Labels
        elif isinstance(item, list) and not isinstance(item[0], bool):
            indices = []
            for label in item:
                # Find first occurrence for each label? Or strict mapping?
                # Pandas raises KeyError if any label missing (unless .reindex)
                # Let's find first index for each label.
                found = False
                for i, idx_val in enumerate(self._df.index):
                    if idx_val == label:
                        indices.append(i)
                        found = True
                        break # Take first match
                if not found:
                     raise KeyError(f"Label '{label}' not found in index")
            
            return self._df.iloc[indices]

        # 3. Boolean Mask (List of bools)
        elif isinstance(item, list) and item and isinstance(item[0], bool):
            # Delegate to dataframe boolean indexing logic (which accesses iloc/slices)
            # DF boolean indexing usually implemented effectively same as loc[bool_mask]
            
            # Re-implement or call internal method? 
            # Boolean logic is: Keep rows where True.
            if len(item) != len(self._df):
                 raise ValueError(f"Item length {len(item)} does not match DataFrame length {len(self._df)}")
            
            keep_indices = [i for i, keep in enumerate(item) if keep]
            return self._df.iloc[keep_indices]

        # 4. Slice
        elif isinstance(item, slice):
            # Label slicing is tricky (start:stop includes stop).
            # Need to find start index and stop index in the index list.
            start_label = item.start
            stop_label = item.stop
            
            start_idx = 0
            stop_idx = len(self._df)
            
            if start_label is not None:
                try:
                    start_idx = self._df.index.index(start_label)
                except ValueError:
                     raise KeyError(f"Start label '{start_label}' not found")

            if stop_label is not None:
                try:
                    stop_idx = self._df.index.index(stop_label)
                    # Label slicing includes stop!
                    stop_idx += 1 
                except ValueError:
                     raise KeyError(f"Stop label '{stop_label}' not found")
            
            # If step is present
            step = item.step
            
            return self._df.iloc[start_idx:stop_idx:step]

        else:
            raise TypeError("Invalid argument for loc")
