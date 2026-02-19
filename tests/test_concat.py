import pytest
from src import DataFrame, concat
from src.series import Series

def test_concat_simple():
    df1 = DataFrame({'A': [1], 'B': [2]})
    df2 = DataFrame({'A': [3], 'B': [4]})
    res = concat([df1, df2])
    assert res.shape == (2, 2)
    assert res.iloc[0]['A'] == 1
    assert res.iloc[1]['A'] == 3

def test_concat_outer():
    df1 = DataFrame({'A': [1], 'B': [2]})
    df2 = DataFrame({'A': [3], 'C': [5]}) # Missing B, New C
    res = concat([df1, df2])
    
    assert res.shape == (2, 3) # A, B, C
    cols = set(res.columns)
    assert {'A', 'B', 'C'} == cols
    
    # Check Row 1 (from df1)
    assert res.iloc[0]['A'] == 1
    assert res.iloc[0]['B'] == 2
    assert res.iloc[0]['C'] is None
    
    # Check Row 2 (from df2)
    assert res.iloc[1]['A'] == 3
    assert res.iloc[1]['B'] is None
    assert res.iloc[1]['C'] == 5

def test_concat_empty_list():
    with pytest.raises(ValueError):
        concat([])
