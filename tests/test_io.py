import pytest
import os
from src import DataFrame, read_csv, read_json

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
