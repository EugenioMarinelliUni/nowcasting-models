# scripts/bootstrap.py

import os
import sys

def add_src_to_path():
    """
    Ensure the 'src' directory is included in Python's module search path.

    This function should be called at the start of any script outside 'src/' (e.g. in 'scripts/')
    so that packages inside 'src/dfm_pipeline/' can be imported using absolute imports like:
        from dfm_pipeline.utils.data_loader import load_data

    Why it's needed:
    - Python by default doesn't treat 'src' as part of sys.path
    - This helps scripts in 'scripts/' or other folders run without ModuleNotFoundError
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_path = os.path.abspath(os.path.join(current_dir, "..", "src"))

    if src_path not in sys.path:
        sys.path.append(src_path)
