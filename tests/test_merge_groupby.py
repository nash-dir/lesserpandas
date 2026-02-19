import pytest
from src import DataFrame, Series

def test_merge_right():
    df1 = DataFrame({'key': ['a', 'b'], 'val1': [1, 2]})
    df2 = DataFrame({'key': ['b', 'c'], 'val2': [3, 4]})
    
    # Right join: keys [b, c]
    res = df1.merge(df2, on='key', how='right')
    assert res.shape == (2, 3)
    
    # Check 'b' row
    # Sort for deterministic check if order not guaranteed? 
    # Hash map logic order depends on right dataframe order? Yes, implemented that way.
    # Right DF order: b then c.
    # Result should correspond to right indices order (unless map bucket issue, but map iterates indices).
    # wait, map construction iterates right.
    # probing left doesn't apply for right-only items in my outer logic.
    # my implementation:
    # 1. probes left -> matches 'b'.
    # 2. adds unmatched right -> 'c'.
    # so order: match(b), unmatched(c).
    
    row_b = res.loc[0] if res['key'][0] == 'b' else res.loc[1]
    assert row_b['key'] == 'b'
    assert row_b['val1'] == 2
    assert row_b['val2'] == 3
    
    row_c = res.loc[1] if res['key'][1] == 'c' else res.loc[0]
    assert row_c['key'] == 'c'
    assert row_c['val1'] is None
    assert row_c['val2'] == 4

def test_merge_outer():
    df1 = DataFrame({'key': ['a', 'b'], 'val1': [1, 2]})
    df2 = DataFrame({'key': ['b', 'c'], 'val2': [3, 4]})
    
    # Outer join: keys [a, b, c]
    res = df1.merge(df2, on='key', how='outer')
    assert res.shape == (3, 3)
    
    # Logic: Left probe (a, b) -> adds a(left-only), b(match). Then adds c(right-unmatched).
    # Expected keys: a, b, c (order might vary slightly depending on map implementation, but roughly left then right-unmatched)
    
    keys = set(res['key'])
    assert keys == {'a', 'b', 'c'}
    
    # Check values
    # We can use boolean indexing if implemented robustly, or just loop
    for i in range(3):
        row = res.loc[i]
        k = row['key']
        if k == 'a':
            assert row['val1'] == 1
            assert row['val2'] is None
        elif k == 'b':
            assert row['val1'] == 2
            assert row['val2'] == 3
        elif k == 'c':
            assert row['val1'] is None
            assert row['val2'] == 4

def test_groupby_agg_dict():
    df = DataFrame({
        'A': ['foo', 'bar', 'foo', 'bar'],
        'B': [1, 2, 3, 4],
        'C': [10, 20, 30, 40]
    })
    
    g = df.groupby('A')
    res = g.agg({'B': 'sum', 'C': 'max'})
    
    # Default as_index=True. 'A' should be index.
    assert res.index == ['bar', 'foo']  # Sorted keys
    assert 'A' not in res.columns # It's in index
    
    # Check bar
    # B: 2+4=6, C: max(20, 40)=40
    # res.loc['bar'] should work
    assert res.loc['bar']['B'] == 6
    assert res.loc['bar']['C'] == 40
    
    # Check foo
    # B: 1+3=4, C: max(10, 30)=30
    assert res.loc['foo']['B'] == 4
    assert res.loc['foo']['C'] == 30

def test_groupby_as_index_false():
    df = DataFrame({
        'A': ['foo', 'bar'],
        'B': [1, 2]
    })
    g = df.groupby('A', as_index=False)
    res = g.agg({'B': 'sum'})
    
    assert 'A' in res.columns
    assert res.index == [0, 1] # RangeIndex
    assert res['A'][0] == 'bar'
    assert res['B'][0] == 2
