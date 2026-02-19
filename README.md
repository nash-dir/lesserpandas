# Lesserpandas 🐼🪶

**A lightweight DataFrame for small data and constrained environments.**
*Recommended only when you don't need NumPy-backed performance.*

Inspired by the idea that the "lesser panda" is not a smaller giant panda, but a different species adapted to different constraints, `lesserpandas` is a pure-Python, zero-dependency dataframe tool. It is designed to survive in environments where installing compiled extensions is difficult or undesirable.

---

## ⚡ When pandas feels too heavy

Using the full `pandas` library in constrained deployment environments comes with friction:
* You need to manage bulky AWS Layers or custom Docker images.
* It increases cold start latency due to its import size.
* It increases baseline memory usage, which can be noticeable in the 128MB Lambda tier.

Sometimes, you just need to download a 5MB CSV, filter some rows, and aggregate them by a category. `lesserpandas` does exactly this with minimal infrastructure friction.

---

## 📊 Honest Benchmarks

We measured import time and basic operations in a fresh subprocess to approximate a Python cold start environment.

| Metric | `lesserpandas` | `pandas` | Key Takeaway |
| :--- | :--- | :--- | :--- |
| **Import Time** (Cold Start) | 5.97 ms | 423.57 ms | Significantly faster import time in cold-start scenarios. |
| **Baseline Memory** (Peak)* | 0.28 MB | 33.84 MB | Adds minimal Python-level overhead. |
| **Small Data ETL** (5k rows) | 3.23 ms | 3.46 ms | Fast enough for small payloads. |

*\* **Note on memory:** Memory was measured using Python’s `tracemalloc` (object allocation only). OS-level RSS in real Lambda environments may differ. The key takeaway is that `lesserpandas` adds minimal Python-level overhead compared to initializing C-extensions.*

---

## ⚖️ Design Trade-offs (Please read before use)

`lesserpandas` is heavily opinionated. It optimizes for deployment size and startup speed over sheer compute performance.
The goal is not feature parity with `pandas`, but predictable behavior in constrained environments.

**🟢 Sweet Spots (When to use):**
* AWS Lambda handlers processing small JSON/CSV payloads.
* Data engineering micro-steps (e.g., lightweight ETL tasks in Airflow).
* Pyodide or Cloudflare Workers where C-extensions are blocked.
* Small data processing (Under 10,000 rows or < 10MB). **Performance degrades significantly beyond this range, as dataset size grows due to pure Python data structures.**
* These numbers are empirical guidelines, not hard limits.

**🔴 Anti-Patterns (When NOT to use):**
* Machine Learning training or heavy numerical computation.
* Large datasets (100k+ rows). Pure Python lists/dicts will scale poorly.
* Complex statistical analytics, window functions, or multi-indexing.
* **If you need serious analytics, use `pandas`.**

---

## 🚀 Installation & Features

Pure Python. No compiled extensions.

```bash
pip install lesserpandas

```

*(Or simply copy `dist/lesserpandas.py` into your deployment package).*

**Features:**
* The API surface is intentionally small and only covers common reshaping operations. Not all `pandas` behaviors are replicated.
* **Zero dependencies**: No `numpy`, `pandas`, `dateutil`, or `pytz`.
* **Familiar API**: `DataFrame`, `Series`, `read_csv`, `to_csv`, `merge`, `groupby`...
* **Multi-key support**: `df.merge(other, on=['A', 'B'])` or `df.groupby(['A', 'B'])`.
* **String accessors**: `df['col'].str.lower()`, `.str.contains()`.

## 💻 Quickstart
Designed for small scripts and micro-ETL tasks.

```python
import lesserpandas as pd

# Create DataFrame
data = [
    {'id': 1, 'category': 'A', 'value': 10},
    {'id': 2, 'category': 'B', 'value': 20},
    {'id': 3, 'category': 'A', 'value': 30}
]
df = pd.DataFrame(data)

# Filter & GroupBy
filtered = df[df['value'] > 15]
grouped = filtered.groupby('category').sum()

# Merge
other_df = pd.DataFrame({'category': ['A', 'B'], 'name': ['Alpha', 'Beta']})
merged = df.merge(other_df, on='category', how='left')

```
