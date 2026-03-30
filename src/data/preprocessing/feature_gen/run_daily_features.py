"""
Intended to be scheduled (cron/airflow) at 12:00 daily or run manually.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

# Add parent directory to path for imports
# ROOT removed — use package imports
sys.path.insert(0, str(ROOT))

from ops.run_feature_generation import run

if __name__ == '__main__':
    args = SimpleNamespace(start=None, end=None, database_url=None)
    run(args)