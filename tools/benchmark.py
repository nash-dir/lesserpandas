import time
import sys
import subprocess
import json
import statistics
import os
import platform

# Detect if we need to alias src to lesserpandas
try:
    import lesserpandas
    LP_MODULE = "lesserpandas"
except ImportError:
    try:
        import src as lesserpandas
        LP_MODULE = "src"
    except ImportError:
        print("Error: Could not import 'lesserpandas' or 'src'. Make sure you are in the project root.")
        sys.exit(1)

def run_subprocess_code(code, cwd=None):
    """Runs python code in a fresh subprocess and returns stdout."""
    # Ensure current directory is in python path for the subprocess
    env = os.environ.copy()
    if cwd:
        env["PYTHONPATH"] = cwd + os.pathsep + env.get("PYTHONPATH", "")
    else:
        env["PYTHONPATH"] = os.path.abspath(".") + os.pathsep + env.get("PYTHONPATH", "")
        
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=cwd
    )
    if result.returncode != 0:
        raise RuntimeError(f"Subprocess failed:\n{result.stderr}")
    return result.stdout.strip()

def measure_import_time(module_name):
    """Measures import time in a fresh process."""
    code = f"""
import time
start = time.perf_counter()
import {module_name}
end = time.perf_counter()
print((end - start) * 1000)
"""
    try:
        output = run_subprocess_code(code)
        return float(output)
    except Exception as e:
        print(f"Failed to measure import time for {module_name}: {e}")
        return None

def measure_memory_usage(module_name):
    """Measures peak memory usage during import in a fresh process."""
    code = f"""
import tracemalloc
tracemalloc.start()
import {module_name}
current, peak = tracemalloc.get_traced_memory()
print(peak / 1024 / 1024)
tracemalloc.stop()
"""
    try:
        output = run_subprocess_code(code)
        return float(output)
    except Exception as e:
        print(f"Failed to measure memory for {module_name}: {e}")
        return None

def measure_etl_execution(module_name):
    """Measures ETL execution time in a fresh process."""
    # We define the data generation and ETL logic inside the string to run in subprocess
    # This avoids serializing large data to the subprocess
    code = f"""
import time
import {module_name} as pd

# Data Generation
data = [
    {{"id": i, "category": str(i % 10), "value": float(i)}} 
    for i in range(5000)
]

# ETL
start = time.perf_counter()

# 1. DataFrame Creation
df = pd.DataFrame(data)

# 2. Filtering
filtered = df[df['category'] == '5']

# 3. GroupBy + Sum
try:
    # Pre-select columns to ensure compatibility. 
    # Pandas supports df.groupby('cat')['val'], but lesserpandas might not yet.
    # Selecting columns before groupby is efficient and portable.
    subset = filtered[['category', 'value']]
    if hasattr(subset, 'groupby'):
        grouped = subset.groupby('category').sum()
    else:
        raise AttributeError("DataFrame object has no attribute 'groupby'")
except Exception as e:
    # Print full traceback to stderr for debugging
    import traceback
    traceback.print_exc()
    raise e

end = time.perf_counter()
print((end - start) * 1000)
"""
    try:
        output = run_subprocess_code(code)
        return float(output)
    except Exception as e:
        # If standard pandas syntax fails for lesserpandas, we might need a specific adapter.
        # But per requirements, we assume compatibility for this simple pipeline.
        print(f"Failed to measure ETL for {module_name}: {e}")
        return None

def main():
    print("Running Benchmark: lesserpandas vs pandas")
    print("=" * 40)

    # Verify pandas is available
    try:
        import pandas
        has_pandas = True
    except ImportError:
        print("Warning: 'pandas' not found. Skipping pandas benchmarks.")
        has_pandas = False

    results = []

    # 1. Import Time
    print("Measuring Import Time...")
    lp_import = measure_import_time(LP_MODULE)
    pd_import = measure_import_time("pandas") if has_pandas else None
    
    # 2. Memory Usage
    print("Measuring Baseline Memory...")
    lp_memory = measure_memory_usage(LP_MODULE)
    pd_memory = measure_memory_usage("pandas") if has_pandas else None

    # 3. ETL Execution
    print("Measuring ETL Execution...")
    lp_etl = measure_etl_execution(LP_MODULE)
    pd_etl = measure_etl_execution("pandas") if has_pandas else None

    # Format Results
    output = []
    output.append("\nBenchmark Results")
    output.append("| Metric | LesserPandas | Pandas | Improvement (LP vs PD) |")
    output.append("| :--- | :--- | :--- | :--- |")

    def fmt(val, unit):
        return f"{val:.2f} {unit}" if val is not None else "N/A"

    def calc_diff(lp, pd, lower_is_better=True):
        if lp is None or pd is None:
            return "N/A"
        if lower_is_better:
            ratio = pd / lp if lp != 0 else float('inf')
            return f"{ratio:.1f}x Faster/Smaller"
        else:
            ratio = lp / pd if pd != 0 else float('inf')
            return f"{ratio:.1f}x Better"

    # Row 1: Import Time
    output.append(f"| **Import Time** (Cold Start) | {fmt(lp_import, 'ms')} | {fmt(pd_import, 'ms')} | {calc_diff(lp_import, pd_import)} |")
    
    # Row 2: Memory
    output.append(f"| **Baseline Memory** (RSS Peak) | {fmt(lp_memory, 'MB')} | {fmt(pd_memory, 'MB')} | {calc_diff(lp_memory, pd_memory)} |")

    # Row 3: ETL
    output.append(f"| **Small Data ETL** (5k rows) | {fmt(lp_etl, 'ms')} | {fmt(pd_etl, 'ms')} | {calc_diff(lp_etl, pd_etl)} |")

    output.append("\n*Note: Lower is better for all metrics.*")

    output_str = "\n".join(output)
    print(output_str)
    
    with open("benchmark_results.md", "w", encoding="utf-8") as f:
        f.write(output_str)

if __name__ == "__main__":
    main()
