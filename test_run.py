import sys
import os
import csv
import json
import importlib.util

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src import DataFrame, Series, merge, read_csv, read_json

def run_tests():
    print("=== Running Standard Tests (Source Modules) ===")
    _run_test_logic(DataFrame, Series, merge, read_csv, read_json)
    print("=== Standard Tests Passed ===\n")

    print("=== Running Monolith Tests (Single File) ===")
    dist_path = os.path.join(os.path.dirname(__file__), 'dist', 'lesserpandas.py')
    if not os.path.exists(dist_path):
        print(f"Monolith file not found at {dist_path}. Skipping monolith tests.")
        return

    # Dynamic import of the monolith file
    spec = importlib.util.spec_from_file_location("lesserpandas", dist_path)
    lp = importlib.util.module_from_spec(spec)
    sys.modules["lesserpandas"] = lp
    spec.loader.exec_module(lp)
    
    _run_test_logic(lp.DataFrame, lp.Series, lp.merge, lp.read_csv, lp.read_json)
    print("=== Monolith Tests Passed ===")

def _run_test_logic(DataFrameClass, SeriesClass, merge_func, read_csv_func, read_json_func):
    # Setup data
    data = {
        'col1': [1, 2, 3, 4, 5],
        'col2': [10, 20, 30, 40, 50],
        'col3': ['a', 'b', 'c', 'd', 'e']
    }
    df = DataFrameClass(data)
    
    # 1. Basic Selection
    assert isinstance(df['col1'], SeriesClass)
    assert df[['col1', 'col3']].shape == (5, 2)
    assert df[df['col1'] > 2].shape == (3, 3)

    # 2. Merge (Single Key)
    left = DataFrameClass({'id': [1], 'v': [1]})
    right = DataFrameClass({'id': [1], 'v': [2]})
    merged = left.merge(right, on='id')
    assert merged.shape == (1, 3)

    # 3. GroupBy (Single Key)
    gb_df = DataFrameClass({'c': ['A', 'A'], 'v': [1, 2]})
    assert gb_df.groupby('c').sum().shape == (1, 2)

    # 4. I/O (CSV)
    csv_name = "test_monolith.csv"
    df.to_csv(csv_name)
    df_read = read_csv_func(csv_name)
    assert df_read.shape == (5, 3)
    assert df_read.iloc[0]['col1'] == 1 # Check type inference (int)
    if os.path.exists(csv_name):
        os.remove(csv_name)

    # 5. Arithmetic
    print("Test: Arithmetic")
    s1 = SeriesClass([10, 20, None])
    s2 = SeriesClass([1, 2, 3])
    s_add = s1 + s2
    assert s_add[0] == 11
    
    # 6. Sorting
    print("Test: Sorting")
    sort_data = {'A': [3, 1, None, 2]}
    df_sort = DataFrameClass(sort_data)
    sorted_asc = df_sort.sort_values(by='A', ascending=True)
    assert sorted_asc['A'][0] == 1

    # 7. Multi-key GroupBy
    print("Test: Multi-key GroupBy")
    m_gb_data = {
        'cat': ['A', 'A', 'B', 'B'],
        'sub': ['X', 'X', 'Y', 'Z'],
        'val': [1, 2, 3, 4]
    }
    m_gb_df = DataFrameClass(m_gb_data)
    m_gb_sum = m_gb_df.groupby(['cat', 'sub']).sum()
    assert m_gb_sum.shape == (3, 3) # cat, sub, val
    
    assert m_gb_sum['cat'][0] == 'A'
    assert m_gb_sum['sub'][0] == 'X'
    assert m_gb_sum['val'][0] == 3

    # 8. Multi-key Merge
    print("Test: Multi-key Merge")
    left_m = DataFrameClass({
        'k1': ['A', 'A', 'B'],
        'k2': [1, 2, 1],
        'val_l': [10, 20, 30]
    })
    right_m = DataFrameClass({
        'k1': ['A', 'B'],
        'k2': [1, 1],
        'val_r': [100, 300]
    })
    # Join on k1, k2. Matches: (A, 1), (B, 1). (A, 2) unmatched.
    merged_m = left_m.merge(right_m, on=['k1', 'k2'], how='inner')
    
    # Debug print if failure expected
    print(f"Merged Shape: {merged_m.shape}")
    print(f"Merged Columns: {merged_m.columns}")
    print(f"Merged Data: {merged_m.to_dict()}")

    # Expected: (A,1) matches -> 1 row. (A,2) no match. (B,1) matches -> 1 row. Total 2 rows.
    # Columns: k1, k2, val_l, val_r. (4 cols)
    # Wait, k1 and k2 are join keys. In single key merge, we keep the key.
    # In multi key merge, we keep keys.
    # Left columns: k1, k2, val_l. Right columns: k1, k2, val_r.
    # Overlap keys: k1, k2. They are in 'on'. So they are not duplicated with suffixes.
    # Result cols: k1, k2, val_l, val_r.
    
    assert merged_m.shape == (2, 4) 
    
    # 9. JSON I/O
    print("Test: JSON I/O")
    json_name = "test_io.json"
    df.to_json(json_name)
    df_json_read = read_json_func(json_name)
    assert df_json_read.shape == (5, 3)
    assert df_json_read.iloc[0]['col1'] == 1
    
    if os.path.exists(json_name):
        os.remove(json_name)

if __name__ == "__main__":
    run_tests()
