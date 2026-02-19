import pytest
import os
from src import DataFrame, read_csv, read_json, read_ndjson

def test_csv_io(tmp_path):
    # Setup
    df = DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    f = tmp_path / "test.csv"
    path = str(f)
    
    # Write
    df.to_csv(path)
    assert os.path.exists(path)
    
    # Read
    df_read = read_csv(path)
    assert df_read.shape == (2, 2)
    assert df_read.iloc[0]['a'] == 1 # Inference
    assert df_read.iloc[1]['b'] == 'y'

def test_json_io(tmp_path):
    # Setup
    df = DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    f = tmp_path / "test.json"
    path = str(f)
    
    # Write
    df.to_json(path)
    assert os.path.exists(path)
    
    # Read
    df_read = read_json(path)
    assert df_read.shape == (2, 2)
    assert df_read.iloc[0]['a'] == 1
    assert df_read.iloc[1]['b'] == 'y'

def test_csv_stringio():
    import io
    # Setup
    df = DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    buffer = io.StringIO()
    
    # Write to buffer
    df.to_csv(buffer)
    
    # Reset buffer position
    buffer.seek(0)
    content = buffer.getvalue()
    assert "a,b" in content
    assert "1,x" in content
    
    # Read from buffer
    buffer.seek(0)
    df_read = read_csv(buffer)
    
    assert df_read.shape == (2, 2)
    assert df_read.iloc[0]['a'] == 1
    assert df_read.iloc[1]['b'] == 'y'

def test_ndjson_io(tmp_path):
    import io
    # Setup
    df = DataFrame({'a': [1, 2], 'b': ['x', 'y']})
    
    # 1. Test File I/O
    f = tmp_path / "test.ndjson"
    path = str(f)
    df.to_ndjson(path)
    
    df_read = read_ndjson(path)
    assert df_read.shape == (2, 2)
    assert df_read.iloc[0]['a'] == 1
    
    # 2. Test Stream I/O
    buffer = io.StringIO()
    df.to_ndjson(buffer)
    
    buffer.seek(0)
    content = buffer.getvalue()
    # NDJSON should have separate objects per line
    assert '{"a": 1, "b": "x"}' in content
    assert '{"a": 2, "b": "y"}' in content
    
    buffer.seek(0)
    df_stream = read_ndjson(buffer)
    assert df_stream.shape == (2, 2)
    assert df_stream.iloc[1]['b'] == 'y'
