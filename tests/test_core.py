import pytest
from src import DataFrame, Series

def test_init_basic():
    data = {
        'col1': [1, 2, 3],
        'col2': [10, 20, 30]
    }
    df = DataFrame(data)
    assert df.shape == (3, 2)
    assert df.columns == ['col1', 'col2']

def test_selection_series():
    data = {'col1': [1, 2]}
    df = DataFrame(data)
    s = df['col1']
    assert isinstance(s, Series)
    assert s[0] == 1
    assert len(s) == 2

def test_selection_multi():
    data = {'c1': [1], 'c2': [2], 'c3': [3]}
    df = DataFrame(data)
    subset = df[['c1', 'c3']]
    assert subset.shape == (1, 2)
    assert subset.columns == ['c1', 'c3']

def test_boolean_indexing():
    data = {'v': [1, 5, 2]}
    df = DataFrame(data)
    filtered = df[df['v'] > 2]
    assert filtered.shape == (1, 1)
    assert filtered.iloc[0]['v'] == 5

def test_iloc():
    data = {'v': [10, 20]}
    df = DataFrame(data)
    assert df.iloc[0]['v'] == 10
    assert df.iloc[1]['v'] == 20

def test_series_arithmetic():
    s1 = Series([10, 20, None])
    s2 = Series([1, 2, 3])
    
    # Add
    s_add = s1 + s2
    assert s_add[0] == 11
    assert s_add[1] == 22
    assert s_add[2] is None
    
    # Scalar Add
    s_scalar = s1 + 5
    assert s_scalar[0] == 15
    assert s_scalar[2] is None

def test_assignment():
    df = DataFrame({'A': [1, 2]})
    df['B'] = df['A'] * 2
    assert df['B'][0] == 2
    assert df['B'][1] == 4
    
    # Assign scalar
    df['C'] = 10
    assert df['C'][0] == 10

def test_sort_values():
    df = DataFrame({'A': [3, 1, None, 2]})
    
    # Ascending
    sorted_asc = df.sort_values(by='A', ascending=True)
    res_asc = sorted_asc['A']
    assert res_asc[0] == 1
    assert res_asc[1] == 2
    assert res_asc[2] == 3
    assert res_asc[3] is None

    # Descending
    sorted_desc = df.sort_values(by='A', ascending=False)
    res_desc = sorted_desc['A']
    # Based on our implementation: None comes after valid values in regular sort.
    # In Desc (reverse=True): None (True) > Valid (False) ?
    # Key: (x is None, x).
    # Ascending test confirmed None is last.
    # Descending test:
    # If consistent with 'reverse', indices are reversed.
    # Standard Python sort reverse simply reverses the order.
    # So if Asc was [1, 2, 3, None], Desc should be [None, 3, 2, 1].
    # Let's verify our earlier assumption in test_run.py.
    # In test_run.py we asserted: 3, 2, 1, None.
    # Let's check logic:
    # valid_indices.sort(reverse=True). none_indices stays same.
    # Result = valid_sorted + none_indices.
    # So None is ALWAYS last regardless of sort order because we explicitly concatenate at end.
    assert res_desc[0] == 3
    assert res_desc[1] == 2
    assert res_desc[2] == 1
    assert res_desc[3] is None
