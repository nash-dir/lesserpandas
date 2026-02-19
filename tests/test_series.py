import pytest
from src import Series

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
