from .core import DataFrame

def concat(objs):
    if not objs:
        raise ValueError("No objects to concatenate")
    
    # 1. Collect all unique columns
    all_columns = []
    seen_columns = set()
    for df in objs:
        for col in df.columns:
            if col not in seen_columns:
                all_columns.append(col)
                seen_columns.add(col)
                
    # 2. Build Result Data
    result_data = {col: [] for col in all_columns}
    
    for df in objs:
        current_len = df.shape[0]
        
        for col in all_columns:
            if col in df.columns:
                # Append existing data
                col_data = df._data[col]
                # Ensure we handle slicer logic if df is a view (iloc result)
                # But currently df._data is the full column list. 
                # If df is result of iloc slicing, it returns a new DF with sliced lists.
                # So we can just extend.
                result_data[col].extend(col_data)
            else:
                # Append None for missing columns
                result_data[col].extend([None] * current_len)
                
    return DataFrame(result_data)
