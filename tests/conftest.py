import sys
import os

# Ensure 'src' is importable even if not installed
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print(f"DEBUG: sys.path[0] = {sys.path[0]}")
