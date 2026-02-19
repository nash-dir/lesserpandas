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
    # Note: merge is a method on DataFrame too, but we test the standalone function if provided or method
    # Here we can test method call primarily as it delegates
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

if __name__ == "__main__":
    run_tests()
