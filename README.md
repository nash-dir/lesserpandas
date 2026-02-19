# Lesserpandas

**The 100KB Pandas for AWS Lambda & Edge AI.**
*Recommended only when you don't need Numpy.*

## Features

- **Zero dependencies**: No `numpy`, `pandas`, `dateutil`, or `pytz`. Just pure Python.
- **Pure Python**: Runs anywhere Python runs, including AWS Lambda, Pyodide, and MicroPython.
- **Pandas API Compatible**: Uses `DataFrame`, `Series`, `read_csv`, `to_csv`, `merge`, `groupby` just like you're used to.
- **One-file Deployment**: Copy `dist/lesserpandas.py` to your project and you're done.

## Installation

Simply copy `lesserpandas.py` to your source directory.

## Usage

```python
import lesserpandas as pd

# Create DataFrame
data = [
    {'id': 1, 'category': 'A', 'value': 10},
    {'id': 2, 'category': 'B', 'value': 20},
    {'id': 3, 'category': 'A', 'value': 30},
    {'id': 4, 'category': 'B', 'value': 40}
]
df = pd.DataFrame(data)

# Filter
filtered = df[df['value'] > 15]

# GroupBy
grouped = filtered.groupby('category').sum()
print(grouped)
# Output:
#    category  id  value
# 0        A   3     30
# 1        B   6     60

# Merge
other_df = pd.DataFrame({'category': ['A', 'B'], 'name': ['Alpha', 'Beta']})
merged = df.merge(other_df, on='category', how='left')
```
