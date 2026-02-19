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

def read_csv(filepath_or_buffer, chunksize=None):
    """
    Read CSV from filepath or file-like object.
    If chunksize is set, returns a generator yielding DataFrames.
    """
    if isinstance(filepath_or_buffer, str):
        if not os.path.exists(filepath_or_buffer):
            raise FileNotFoundError(f"File not found: {filepath_or_buffer}")
        
        if chunksize is None:
            with open(filepath_or_buffer, mode='r', newline='', encoding='utf-8') as f:
                return _read_csv_from_file_obj(f)
        else:
            return _read_csv_chunks_from_path(filepath_or_buffer, chunksize)
    else:
        # Assume it's a file-like object
        if chunksize is None:
            return _read_csv_from_file_obj(filepath_or_buffer)
        else:
            return _read_csv_chunks(filepath_or_buffer, chunksize)

def _read_csv_chunks_from_path(filepath, chunksize):
    with open(filepath, mode='r', newline='', encoding='utf-8') as f:
        yield from _read_csv_chunks(f, chunksize)

def _read_csv_chunks(f, chunksize):
    reader = csv.DictReader(f)
    chunk = []
    for row in reader:
        processed_row = {k: _infer_type(v) for k, v in row.items()}
        chunk.append(processed_row)
        if len(chunk) >= chunksize:
            yield DataFrame(chunk)
            chunk = []
    
    if chunk:
        yield DataFrame(chunk)

def _read_csv_from_file_obj(f):
    # Use DictReader to handle headers automatically
    reader = csv.DictReader(f)
    
    # Read all rows and infer types
    data = []
    for row in reader:
        processed_row = {k: _infer_type(v) for k, v in row.items()}
        data.append(processed_row)
        
    return DataFrame(data)

def to_csv(df, filepath_or_buffer):
    """
    Write DataFrame to CSV filepath or file-like object.
    """
    if isinstance(filepath_or_buffer, str):
        with open(filepath_or_buffer, mode='w', newline='', encoding='utf-8') as f:
            _to_csv_to_file_obj(df, f)
    else:
        _to_csv_to_file_obj(df, filepath_or_buffer)

def _to_csv_to_file_obj(df, f):
    # Get column names
    fieldnames = df.columns
    
    # Prepare data for writing: None -> ""
    rows_to_write = []
    
    # Iterate row by row
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

    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_to_write)

import json

def read_json(filepath_or_buffer):
    """
    Read JSON from filepath or file-like object.
    Expects a list of records.
    """
    if isinstance(filepath_or_buffer, str):
        if not os.path.exists(filepath_or_buffer):
            raise FileNotFoundError(f"File not found: {filepath_or_buffer}")

        with open(filepath_or_buffer, 'r', encoding='utf-8') as f:
            return _read_json_from_file_obj(f)
    else:
        return _read_json_from_file_obj(filepath_or_buffer)

def _read_json_from_file_obj(f):
    data = json.load(f)
    
    if not isinstance(data, list):
         raise TypeError("JSON content must be a list of records")

    return DataFrame(data)

def to_json(df, filepath_or_buffer):
    """
    Write DataFrame to JSON filepath or file-like object.
    Writes as a list of records.
    """
    if isinstance(filepath_or_buffer, str):
        with open(filepath_or_buffer, 'w', encoding='utf-8') as f:
            _to_json_to_file_obj(df, f)
    else:
        _to_json_to_file_obj(df, filepath_or_buffer)

def _to_json_to_file_obj(df, f):
    records = df.to_dict(orient='records')
    json.dump(records, f, indent=4)

def read_ndjson(filepath_or_buffer, chunksize=None):
    """
    Read NDJSON (Newline Delimited JSON) from filepath or file-like object.
    Each line is a separate JSON object.
    If chunksize is set, returns a generator yielding DataFrames.
    """
    if isinstance(filepath_or_buffer, str):
        if not os.path.exists(filepath_or_buffer):
            raise FileNotFoundError(f"File not found: {filepath_or_buffer}")

        if chunksize is None:
            with open(filepath_or_buffer, 'r', encoding='utf-8') as f:
                return _read_ndjson_from_file_obj(f)
        else:
            return _read_ndjson_chunks_from_path(filepath_or_buffer, chunksize)
    else:
        if chunksize is None:
             return _read_ndjson_from_file_obj(filepath_or_buffer)
        else:
             return _read_ndjson_chunks(filepath_or_buffer, chunksize)

def _read_ndjson_chunks_from_path(filepath, chunksize):
    with open(filepath, 'r', encoding='utf-8') as f:
         yield from _read_ndjson_chunks(f, chunksize)

def _read_ndjson_chunks(f, chunksize):
    chunk = []
    for line in f:
        line = line.strip()
        if line:
            chunk.append(json.loads(line))
        
        if len(chunk) >= chunksize:
            yield DataFrame(chunk)
            chunk = []
            
    if chunk:
        yield DataFrame(chunk)

def _read_ndjson_from_file_obj(f):
    data = []
    for line in f:
        line = line.strip()
        if line:
            data.append(json.loads(line))
    return DataFrame(data)

def to_ndjson(df, filepath_or_buffer):
    """
    Write DataFrame to NDJSON filepath or file-like object.
    """
    if isinstance(filepath_or_buffer, str):
        with open(filepath_or_buffer, 'w', encoding='utf-8') as f:
            _to_ndjson_to_file_obj(df, f)
    else:
         _to_ndjson_to_file_obj(df, filepath_or_buffer)

def _to_ndjson_to_file_obj(df, f):
    records = df.to_dict(orient='records')
    for record in records:
        f.write(json.dumps(record) + '\n')
