"""
Backward-compatibility shim.
Allows existing code using `import data_compare` to continue working.
The canonical import is now `import edcp`.
"""
import sys, edcp
sys.modules['data_compare'] = edcp
