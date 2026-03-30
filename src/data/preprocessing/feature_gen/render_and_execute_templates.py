# Re-export from new modules for backward compatibility
from data.preprocessing.feature_gen.template_engine import get_template, render_template, clear_cache
from data.preprocessing.feature_gen.db_utils import execute_sql, build_bccp_src, create_bccp_indexes
from data.preprocessing.feature_gen.static_aggregation import run_static_aggregate
from data.preprocessing.feature_gen.window_aggregation import render_and_run_all

__all__ = [
    'get_template',
    'render_template', 
    'clear_cache',
    'execute_sql',
    'build_bccp_src',
    'create_bccp_indexes',
    'run_static_aggregate',
    'render_and_run_all',
]