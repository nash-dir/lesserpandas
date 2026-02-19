from .core import DataFrame

def merge(left, right, on, how='inner'):
    if how not in ('inner', 'left'):
        raise ValueError("Only 'inner' and 'left' merge types are supported")
    
    # Normalize 'on' to always be a list
    if isinstance(on, str):
        on_cols = [on]
    elif isinstance(on, list):
        on_cols = on
    else:
        raise TypeError("Merge key must be a string or list of strings")

    for col in on_cols:
        if col not in left.columns:
            raise KeyError(f"Column '{col}' not found in left DataFrame")
        if col not in right.columns:
            raise KeyError(f"Column '{col}' not found in right DataFrame")

    # 1. Build Hash Map from Right DataFrame
    # right_map: {key_tuple: [row_idx1, row_idx2, ...]}
    right_map = {}
    
    # Pre-fetch right columns
    right_on_data = [right._data[col] for col in on_cols]
    
    for idx in range(right.shape[0]):
        key = tuple(col_data[idx] for col_data in right_on_data)
        if key not in right_map:
            right_map[key] = []
        right_map[key].append(idx)

    # 2. Determine Result Columns and handle suffixes
    # Identify overlapping columns (excluding join keys)
    overlap_cols = set(left.columns) & set(right.columns) - set(on_cols)
    
    new_columns = {} # {new_col_name: source_col_name (or None for suffixes)}
    
    # Add left columns
    for col in left.columns:
        if col in on_cols:
            new_columns[col] = ('left', col)
        elif col in overlap_cols:
            new_columns[f"{col}_x"] = ('left', col)
        else:
            new_columns[col] = ('left', col)
            
    # Add right columns
    for col in right.columns:
        if col in on_cols:
            continue # Already handled (from left)
        elif col in overlap_cols:
            new_columns[f"{col}_y"] = ('right', col)
        else:
            new_columns[col] = ('right', col)

    # Initialize result data structure
    result_data = {col: [] for col in new_columns}

    # 3. Probe Left DataFrame and Build Result
    left_on_data = [left._data[col] for col in on_cols]
    
    for left_idx in range(left.shape[0]):
        key = tuple(col_data[left_idx] for col_data in left_on_data)
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
