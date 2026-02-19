import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src import DataFrame, Series, merge

def run_tests():
    # Setup data
    data = {
        'col1': [1, 2, 3, 4, 5],
        'col2': [10, 20, 30, 40, 50],
        'col3': ['a', 'b', 'c', 'd', 'e']
    }
    df = DataFrame(data)
    print(f"DataFrame created: {df}")

    # --- Previous Tests (Brief Check) ---
    print("\n[Basic Selection Tests]")
    assert isinstance(df['col1'], Series)
    assert df[['col1', 'col3']].shape == (5, 2)
    assert df[df['col1'] > 2].shape == (3, 3)
    assert df.iloc[0]['col1'] == 1
    print("Passed basic selection tests.")

    # --- Merge Tests ---
    print("\n[Merge Tests]")
    # Left DataFrame
    left_data = {
        'id': [1, 2, 3, 4],
        'val_l': ['L1', 'L2', 'L3', 'L4'],
        'common': [100, 200, 300, 400]
    }
    left_df = DataFrame(left_data)
    
    # Right DataFrame (One-to-many relation for id=2, missing id=3, new id=5)
    right_data = {
        'id': [1, 2, 2, 5],
        'val_r': ['R1', 'R2a', 'R2b', 'R5'],
        'common': [101, 201, 202, 501]
    }
    right_df = DataFrame(right_data)

    # 1. Inner Join
    print("1. Inner Join on 'id'")
    merged_inner = left_df.merge(right_df, on='id', how='inner')
    print(merged_inner)
    print(f"Columns: {merged_inner.columns}")
    
    # Expected: id=[1, 2, 2], val_l, val_r, common_x, common_y
    assert merged_inner.shape == (3, 5) # 3 rows, 5 cols
    assert 'common_x' in merged_inner.columns
    assert 'common_y' in merged_inner.columns
    # Check data correctness (id=1)
    row1 = merged_inner[merged_inner['id'] == 1].iloc[0]
    assert row1['val_l'] == 'L1' and row1['val_r'] == 'R1'
    # Check one-to-many (id=2)
    rows2 = merged_inner[merged_inner['id'] == 2]
    assert rows2.shape == (2, 5)

    # 2. Left Join
    print("\n2. Left Join on 'id'")
    merged_left = left_df.merge(right_df, on='id', how='left')
    print(merged_left)
    
    # Expected: id=[1, 2, 2, 3, 4] (id=3,4 have no match)
    # Total rows: 1 (id=1) + 2 (id=2) + 1 (id=3) + 1 (id=4) = 5
    assert merged_left.shape == (5, 5)
    
    # Check missing values for id=3
    row3 = merged_left[merged_left['id'] == 3].iloc[0]
    assert row3['val_l'] == 'L3'
    assert row3['val_r'] is None
    assert row3['common_y'] is None


    # --- GroupBy Tests ---
    print("\n[GroupBy Tests]")
    gb_data = {
        'category': ['A', 'B', 'A', 'B', 'A', 'C'],
        'value1': [1, 2, 3, 4, 5, 6],
        'value2': [10, 20, 30, 40, 50, 60]
    }
    gb_df = DataFrame(gb_data)
    
    # 1. Sum
    print("1. GroupBy Sum")
    gb_sum = gb_df.groupby('category').sum()
    print(gb_sum)
    # Expected: A: 9 (1+3+5), B: 6 (2+4), C: 6
    row_a = gb_sum[gb_sum['category'] == 'A'].iloc[0]
    assert row_a['value1'] == 9
    assert row_a['value2'] == 90 # 10+30+50
    
    # 2. Mean
    print("\n2. GroupBy Mean")
    gb_mean = gb_df.groupby('category').mean()
    print(gb_mean)
    # Expected: A: 3.0 (9/3)
    row_a_mean = gb_mean[gb_mean['category'] == 'A'].iloc[0]
    assert row_a_mean['value1'] == 3.0

    # 3. Count
    print("\n3. GroupBy Count")
    gb_count = gb_df.groupby('category').count()
    print(gb_count)
    # Expected: A: 3, B: 2, C: 1
    row_a_count = gb_count[gb_count['category'] == 'A'].iloc[0]
    assert row_a_count['value1'] == 3
    
    print("\nAll merge and groupby tests passed!")

if __name__ == "__main__":
    run_tests()
