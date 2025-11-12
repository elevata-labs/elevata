"""
SQL dialect adapters.

Each dialect implements SqlDialect and knows how to render Expr and
LogicalSelect instances into concrete SQL strings.
"""

from .base import SqlDialect
from .duckdb import DuckDBDialect  # convenient re-export
