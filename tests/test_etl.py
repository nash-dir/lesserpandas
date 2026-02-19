import pytest
from src import DataFrame, Series

def test_drop_columns():
    df = DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]})
    res = df.drop('B')
    assert 'B' not in res.columns
    assert 'A' in res.columns
    assert 'C' in res.columns
    assert res.shape == (2, 2)
    
    res2 = df.drop(['A', 'C'])
    assert res2.columns == ['B']
    
    with pytest.raises(KeyError):
        df.drop('Z')

def test_rename_columns():
    df = DataFrame({'A': [1, 2], 'B': [3, 4]})
    res = df.rename({'A': 'X', 'B': 'Y'})
    assert res.columns == ['X', 'Y']
    assert res['X'][0] == 1
    
    # Partial rename
    res2 = df.rename({'A': 'Z'})
    assert res2.columns == ['Z', 'B']

def test_assign():
    df = DataFrame({'A': [1, 2]})
    
    # Scalar assignment
    res = df.assign(B=10)
    assert res['B'][0] == 10
    assert res['B'][1] == 10
    
    # List assignment
    res2 = df.assign(C=[3, 4])
    assert res2['C'][0] == 3
    
    # Callable assignment
    res3 = df.assign(D=lambda x: x['A'] * 2)
    assert res3['D'][0] == 2
    assert res3['D'][1] == 4
    
    # Callable returning Series (if supported via simple arithmetic)
    # The lambda x: x['A'] * 2 returns a Series if Series arithmetic works.
    # Series arithmetic IS implemented. So res3['D'] is constructed from Series list.
    pass

def test_astype():
    s = Series(['1', '2', '3'])
    res = s.astype(int)
    assert res[0] == 1
    assert isinstance(res[0], int)
    
    s2 = Series(['1.1', '2.2'])
    res2 = s2.astype(float)
    assert res2[0] == 1.1
    
    s_none = Series(['1', None])
    res_none = s_none.astype(int)
    assert res_none[0] == 1
    assert res_none[1] is None
    
    # Error case
    s_err = Series(['a'])
    with pytest.raises(ValueError):
        s_err.astype(int)

def test_value_counts():
    s = Series(['a', 'b', 'a', 'c', 'a', 'b'])
    vc = s.value_counts()
    
    # Should be sorted descending
    assert vc.index[0] == 'a'
    assert vc[0] == 3
    
    assert vc.index[1] == 'b'
    assert vc[1] == 2
    
    assert vc.index[2] == 'c'
    assert vc[2] == 1
