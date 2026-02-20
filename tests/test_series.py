import pytest
from src import Series, DataFrame

def test_isin():
    s = Series([1, 2, 3, None])
    mask = s.isin([1, 3])
    assert mask[0] is True
    assert mask[1] is False
    assert mask[2] is True
    assert mask[3] is False

def test_apply_simple():
    s = Series([1, 2, 3])
    res = s.apply(lambda x: x * 2)
    assert res[0] == 2
    assert res[2] == 6

def test_apply_error_handling():
    s = Series([1, 'a', 2])
    # x * 2 works for string 'aa', but let's do x + 1 which fails for str
    res = s.apply(lambda x: x + 1)
    assert res[0] == 2
    assert res[1] is None # Error caught
    assert res[2] == 3

def test_str_accessor_lower_upper():
    s = Series(['A', 'b', None, 1])
    low = s.str.lower()
    assert low[0] == 'a'
    assert low[1] == 'b'
    assert low[2] is None
    assert low[3] is None # Non string
    
    up = s.str.upper()
    assert up[0] == 'A'
    assert up[1] == 'B'

def test_str_contains():
    s = Series(['apple', 'banana', 'cherry', None])
    has_a = s.str.contains('a')
    assert has_a[0] is True  # apple
    assert has_a[1] is True  # banana
    assert has_a[2] is False # cherry
    assert has_a[3] is None  # None -> None

def test_apply_preserves_index():
    s = Series([1, 2, 3], index=['x', 'y', 'z'])
    res = s.apply(lambda x: x * 2)
    assert res.index == ['x', 'y', 'z']
    assert res[0] == 2
    assert res[2] == 6

def test_str_accessor_preserves_index():
    s = Series(['A', 'B'], index=['idx1', 'idx2'])
    res = s.str.lower()
    assert res.index == ['idx1', 'idx2']
    assert res[0] == 'a'

def test_str_contains_preserves_index():
    s = Series(['apple', 'banana'], index=['fruit1', 'fruit2'])
    res = s.str.contains('a')
    assert res.index == ['fruit1', 'fruit2']
    assert res[0] is True

def test_isin_preserves_index():
    s = Series([1, 2, 3], index=['x', 'y', 'z'])
    res = s.isin([1, 3])
    assert res.index == ['x', 'y', 'z']
    assert res[0] is True
    assert res[1] is False
    assert res[2] is True

def test_series_list_mutation():
    raw_data = [1, 2, 3]
    s = Series(raw_data)
    # Mutate the original list
    raw_data[0] = 999
    # The Series should NOT be affected
    assert s[0] == 1, "Series state corrupted by external list mutation"

def test_series_copy_false_mutation():
    raw_data = [1, 2, 3]
    s = Series(raw_data, copy=False)
    # Mutate the original list
    raw_data[0] = 999
    # The Series SHOULD be affected because we asked for no copy
    assert s[0] == 999, "Series did not share reference when copy=False"

def test_dataframe_getitem_returns_copy():
    raw_data = {"A": [1, 2, 3]}
    df = DataFrame(raw_data)
    s = df["A"]
    
    # Maliciously mutate the returned Series
    s._data.append(999)
    
    # The DataFrame should NOT be affected, preserving rectangular structure
    assert len(df._data["A"]) == 3, "DataFrame column access returned a view, enabling length mutations"
