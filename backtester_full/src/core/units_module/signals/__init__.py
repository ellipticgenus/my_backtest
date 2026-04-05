"""
Signal Modules for the Backtester.

This package contains various signal generation modules for trading strategies.
"""

from backtester_full.src.core.units_module.signals.base import TS_Signal
from backtester_full.src.core.units_module.signals.trend import TS_Trend
from backtester_full.src.core.units_module.signals.cot import TS_COT

__all__ = [
    'TS_Signal',
    'TS_Trend',
    'TS_COT',
]
