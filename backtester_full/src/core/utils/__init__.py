"""
Core Utils Package.

This package provides utility functions for the core module:
- get_unique_dicts: Get unique dictionaries from a list
- partition_ticker: Partition a ticker into components
- GLOBALPARAMS: Global parameters helper
"""

from backtester_full.src.core.utils.utils import get_unique_dicts, partition_ticker
from backtester_full.src.core.utils.global_params_helper import GLOBALPARAMS

__all__ = [
    'get_unique_dicts',
    'partition_ticker',
    'GLOBALPARAMS',
]