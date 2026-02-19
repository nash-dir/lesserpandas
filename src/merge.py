from .core import DataFrame

def merge(left, right, on, how='inner'):
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

    if how not in ('inner', 'left', 'right', 'outer'):
        raise ValueError("Only 'inner', 'left', 'right', 'outer' merge types are supported")

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
    
    # Track visited right indices for right/outer join
    visited_right_indices = set()

    # 3. Probe Left DataFrame and Build Result
    left_on_data = [left._data[col] for col in on_cols]
    
    # For 'right' join optimization:
    # If strictly right join, we could just iterate right and probe left map.
    # But current structure (left probe right map) is good for left/inner/outer.
    # 'Right' join can be viewed as: match generic (inner part) + unmatched right.
    # But standard left-probe loop doesn't easily capture unmatched right unless we do full outer approach.
    # Implementing generic "Outer" logic handles match + left_unmatched + right_unmatched.
    # Inner: match only.
    # Left: match + left_unmatched.
    # Right: match + right_unmatched.
    # Outer: all.
    
    # NOTE: If 'right' join, we strictly only care about right rows.
    # If we iterate left rows, we might process rows that shouldn't exist in right join (left unmatched).
    # So if 'right', we should SKIP left unmatched.
    
    for left_idx in range(left.shape[0]):
        key = tuple(col_data[left_idx] for col_data in left_on_data)
        right_indices = right_map.get(key, [])

        if not right_indices:
            if how == 'inner' or how == 'right':
                continue
            elif how == 'left' or how == 'outer':
                # Add row with None for right columns
                for new_col, (source, orig_col) in new_columns.items():
                    if source == 'left':
                        result_data[new_col].append(left._data[orig_col][left_idx])
                    else: # right
                        result_data[new_col].append(None)
        else:
            # Match found
            for right_idx in right_indices:
                visited_right_indices.add(right_idx)
                for new_col, (source, orig_col) in new_columns.items():
                    if source == 'left':
                         result_data[new_col].append(left._data[orig_col][left_idx])
                    else: # right
                         result_data[new_col].append(right._data[orig_col][right_idx])
    
    # 4. Handle Unmatched Right Rows (for Right and Outer joins)
    if how == 'right' or how == 'outer':
        # Find indices in right that were not visited
        all_right_indices = set(range(right.shape[0]))
        unmatched_right_indices = sorted(list(all_right_indices - visited_right_indices))
        
        for right_idx in unmatched_right_indices:
            for new_col, (source, orig_col) in new_columns.items():
                if source == 'right':
                    result_data[new_col].append(right._data[orig_col][right_idx])
                elif source == 'left':
                    # If this is a join key column, we might be able to fill it from right data!
                    # "On" columns are mapped from 'left' in new_columns logic above: new_columns[col] = ('left', col)
                    # So 'left' source logic handles the key column in result.
                    # But for unmatched RIGHT row, we don't have a left row.
                    # HOWEVER, we DO have the value in the right row for the key column.
                    # Is orig_col (from left) name same as right? Yes, 'on' cols must exist in both.
                    
                    # Check if this result column corresponds to an 'on' column
                    # The mapping above used key 'col' -> ('left', col).
                    # Check if 'new_col' (the key in result) is in 'on_cols'.
                    if new_col in on_cols:
                         # Fill from right data
                         result_data[new_col].append(right._data[new_col][right_idx])
                    else:
                         # Truly left-only column -> None
                         result_data[new_col].append(None)

    return DataFrame(result_data)
