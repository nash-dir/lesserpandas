import sys
import os
import csv
import importlib.util

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src import DataFrame, Series, merge, read_csv

def run_tests():
    print("=== Running Standard Tests (Source Modules) ===")
    _run_test_logic(DataFrame, Series, merge, read_csv)
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
    
    _run_test_logic(lp.DataFrame, lp.Series, lp.merge, lp.read_csv)
    print("=== Monolith Tests Passed ===")

def _run_test_logic(DataFrameClass, SeriesClass, merge_func, read_csv_func):
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

    # 2. Merge
    left = DataFrameClass({'id': [1], 'v': [1]})
    right = DataFrameClass({'id': [1], 'v': [2]})
    merged = left.merge(right, on='id')
    assert merged.shape == (1, 3)

    # 3. GroupBy
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
    
    # Add
    s_add = s1 + s2
    assert s_add[0] == 11
    assert s_add[1] == 22
    assert s_add[2] is None
    
    # Scalar Add
    s_scalar = s1 + 5
    assert s_scalar[0] == 15
    assert s_scalar[2] is None

    # DataFrame Assignment
    print("Test: DataFrame Assignment")
    df_assign = DataFrameClass({'A': [1, 2]})
    df_assign['B'] = df_assign['A'] * 2
    assert df_assign['B'][0] == 2
    assert df_assign['B'][1] == 4
    
    df_assign['C'] = 10
    assert df_assign['C'][0] == 10
    assert df_assign['C'][1] == 10

    # 6. Sorting
    print("Test: Sorting")
    sort_data = {'A': [3, 1, None, 2], 'B': ['c', 'a', None, 'b']}
    df_sort = DataFrameClass(sort_data)
    
    # Ascending
    sorted_asc = df_sort.sort_values(by='A', ascending=True)
    # Expected: 1, 2, 3, None
    res_asc = sorted_asc['A']
    assert res_asc[0] == 1
    assert res_asc[1] == 2
    assert res_asc[2] == 3
    assert res_asc[3] is None
    
    # Descending
    sorted_desc = df_sort.sort_values(by='A', ascending=False)
    # Expected: 3, 2, 1, None (We decided None is always last for now)
    res_desc = sorted_desc['A']
    assert res_desc[0] == 3
    assert res_desc[1] == 2
    assert res_desc[2] == 1
    assert res_desc[3] is None

if __name__ == "__main__":
    run_tests()
