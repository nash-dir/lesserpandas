import pytest
from src import DataFrame

def test_merge_inner():
    left = DataFrame({'id': [1, 2], 'l': ['a', 'b']})
    right = DataFrame({'id': [1, 3], 'r': ['x', 'y']})
    merged = left.merge(right, on='id', how='inner')
    assert merged.shape == (1, 3)
    assert merged.iloc[0]['id'] == 1
    assert merged.iloc[0]['l'] == 'a'
    assert merged.iloc[0]['r'] == 'x'

def test_merge_left():
    left = DataFrame({'id': [1, 2]})
    right = DataFrame({'id': [1]})
    merged = left.merge(right, on='id', how='left')
    assert merged.shape == (2, 1)
    # 2nd row should have None for right columns - check column name (if overlapping, suffixes might apply, but 'id' is joined)
    # 'id' is in left and right, but it's the key.
    # If there were other columns?
    # Right has no other columns here? wait, merge adds all columns.
    # 'id' is key. 'id' comes from left.
    # Right columns? 'id' is the only one.
    # Let's add a value column to right to test None.
    
    right2 = DataFrame({'id': [1], 'r': [10]})
    merged2 = left.merge(right2, on='id', how='left')
    assert merged2.shape == (2, 2)
    assert merged2.iloc[1]['id'] == 2
    assert merged2.iloc[1]['r'] is None

def test_merge_multi_key():
    left = DataFrame({
        'k1': ['A', 'A', 'B'],
        'k2': [1, 2, 1],
        'v': [10, 20, 30]
    })
    right = DataFrame({
        'k1': ['A', 'B'],
        'k2': [1, 1],
        'v': [100, 300]
    })
    # v_x, v_y expected
    merged = left.merge(right, on=['k1', 'k2'])
    
    assert merged.shape == (2, 4) # k1, k2, v_x, v_y
    columns = set(merged.columns)
    assert 'v_x' in columns
    assert 'v_y' in columns
    
    # Check Row 1 (A, 1)
    row0 = merged.iloc[0]
    assert row0['k1'] == 'A'
    assert row0['k2'] == 1
    assert row0['v_x'] == 10
    assert row0['v_y'] == 100

def test_groupby_single():
    df = DataFrame({'key': ['A', 'B', 'A'], 'val': [1, 2, 3]})
    g = df.groupby('key').sum()
    assert g.shape == (2, 2)
    
    # Sort order is deterministic (dictionary keys sorted)
    # A comes before B
    assert g.iloc[0]['key'] == 'A'
    assert g.iloc[0]['val'] == 4 # 1+3

def test_df_apply_rows():
    df = DataFrame({'a': [1, 2], 'b': [10, 20]})
    # Sum rows
    s = df.apply(lambda row: row['a'] + row['b'], axis=1)
    assert len(s) == 2
    assert s[0] == 11
    assert s[1] == 22


def test_groupby_multi():
    df = DataFrame({
        'k1': ['A', 'A', 'B'],
        'k2': [1, 2, 1],
        'val': [10, 20, 30]
    })
    g = df.groupby(['k1', 'k2']).sum()
    assert g.shape == (3, 3) 
    # Groups: (A,1)->10, (A,2)->20, (B,1)->30
    assert g.iloc[0]['val'] == 10
    assert g.iloc[2]['val'] == 30
