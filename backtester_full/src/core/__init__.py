"""
Core Package.

This package provides the core backtesting functionality including:
- Backtester: Main backtesting engine
- Portfolio: Portfolio management
- Data loaders: Price, series, and COT data loading
- Units modules: Signal generation and position management
- Price modules: Futures and strategy pricing
- Cost modules: Transaction costs
"""

from backtester_full.src.core.backtest import Backtester
from backtester_full.src.core.portfolio import Portfolio, PortfolioState, Trade

__all__ = [
    'Backtester',
    'Portfolio',
    'PortfolioState',
    'Trade',
]