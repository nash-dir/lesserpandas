import csv
import os
from .core import DataFrame

def _infer_type(value):
    """
    Attempt to convert string value to int, float, or None.
    Returns original string if no conversion is possible.
    """
    if value == "":
        return None
    
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        pass
    
    return value

def read_csv(filepath):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    with open(filepath, mode='r', newline='', encoding='utf-8') as f:
        # Use DictReader to handle headers automatically
        reader = csv.DictReader(f)
        
        # Read all rows and infer types
        data = []
        for row in reader:
            processed_row = {k: _infer_type(v) for k, v in row.items()}
            data.append(processed_row)
            
    return DataFrame(data)

def to_csv(df, filepath):
    # Get column names
    fieldnames = df.columns
    
    # Prepare data for writing: None -> ""
    rows_to_write = []
    
    # Iterate row by row (using iloc logic essentially, but optimized for list of dicts)
    # Since we store columns, we need to transpose
    num_rows = df.shape[0]
    for i in range(num_rows):
        row = {}
        for col in fieldnames:
            val = df._data[col][i]
            if val is None:
                row[col] = ""
            else:
                row[col] = val
        rows_to_write.append(row)

    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_to_write)
