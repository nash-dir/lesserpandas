from .core import DataFrame

def merge(left, right, on, how='inner'):
    if how not in ('inner', 'left'):
        raise ValueError("Only 'inner' and 'left' merge types are supported")
    
    if on not in left.columns:
        raise KeyError(f"Column '{on}' not found in left DataFrame")
    if on not in right.columns:
        raise KeyError(f"Column '{on}' not found in right DataFrame")

    # 1. Build Hash Map from Right DataFrame
    # right_map: {key: [row_idx1, row_idx2, ...]}
    right_map = {}
    right_col = right._data[on]
    for idx in range(right.shape[0]):
        key = right_col[idx]
        if key not in right_map:
            right_map[key] = []
        right_map[key].append(idx)

    # 2. Determine Result Columns and handle suffixes
    # Identify overlapping columns
    overlap_cols = set(left.columns) & set(right.columns) - {on}
    
    new_columns = {} # {new_col_name: source_col_name (or None for suffixes)}
    
    # Add left columns
    for col in left.columns:
        if col == on:
            new_columns[col] = ('left', col)
        elif col in overlap_cols:
            new_columns[f"{col}_x"] = ('left', col)
        else:
            new_columns[col] = ('left', col)
            
    # Add right columns
    for col in right.columns:
        if col == on:
            continue # Already handled
        elif col in overlap_cols:
            new_columns[f"{col}_y"] = ('right', col)
        else:
            new_columns[col] = ('right', col)

    # Initialize result data structure
    result_data = {col: [] for col in new_columns}

    # 3. Probe Left DataFrame and Build Result
    left_col = left._data[on]
    
    for left_idx in range(left.shape[0]):
        key = left_col[left_idx]
        right_indices = right_map.get(key, [])

        if not right_indices:
            if how == 'inner':
                continue
            elif how == 'left':
                # Add row with None for right columns
                for new_col, (source, orig_col) in new_columns.items():
                    if source == 'left':
                        result_data[new_col].append(left._data[orig_col][left_idx])
                    else: # right
                        result_data[new_col].append(None)
        else:
            # Match found (Inner or Left)
            for right_idx in right_indices:
                for new_col, (source, orig_col) in new_columns.items():
                    if source == 'left':
                         result_data[new_col].append(left._data[orig_col][left_idx])
                    else: # right
                         result_data[new_col].append(right._data[orig_col][right_idx])

    return DataFrame(result_data)
