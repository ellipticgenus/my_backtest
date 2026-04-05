"""
Backtester_full Package.

This package provides a full-featured backtester with:
- Data loaders for price, series, and COT data
- Units modules for signal generation and position management
- Price modules for futures and strategy pricing
- Cost modules for transaction costs
"""

__version__ = '0.1.0'

__all__ = [
    'Backtester',
    'Portfolio',
    'Trade',
    'PortfolioState',
]