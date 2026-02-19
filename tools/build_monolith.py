import os
import re

# Order matters for dependencies
FILE_ORDER = [
    'series.py',
    'indexing.py',
    'core.py',
    'concat.py',
    'groupby.py',
    'merge.py',
    'io.py'
]

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src')
DIST_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'dist')
OUTPUT_FILE = os.path.join(DIST_DIR, 'lesserpandas.py')

def build_monolith():
    if not os.path.exists(DIST_DIR):
        os.makedirs(DIST_DIR)

    combined_code = []
    
    # Header
    combined_code.append('"""LesserPandas: The 100KB Pandas for AWS Lambda & Edge AI."""')
    combined_code.append('import csv')
    combined_code.append('import json')
    combined_code.append('import os')
    combined_code.append('import sys')
    combined_code.append('')

    # Regex to remove relative imports like "from .core import DataFrame"
    relative_import_pattern = re.compile(r'^\s*from\s+\.\w+\s+import\s+.*')

    for filename in FILE_ORDER:
        filepath = os.path.join(SRC_DIR, filename)
        if not os.path.exists(filepath):
            print(f"Warning: {filename} not found in {SRC_DIR}")
            continue
            
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        combined_code.append(f"# --- Start of {filename} ---")
        
        for line in lines:
            # Skip stdlib imports we already added globally (simplistic check)
            if line.strip().startswith('import csv') or line.strip().startswith('import os') or line.strip().startswith('import sys'):
                continue
            
            # Remove relative imports
            if relative_import_pattern.match(line):
                continue
            
            combined_code.append(line.rstrip())
            
        combined_code.append(f"# --- End of {filename} ---\n")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(combined_code))
    
    print(f"Successfully built monolith at {OUTPUT_FILE}")

if __name__ == "__main__":
    build_monolith()
