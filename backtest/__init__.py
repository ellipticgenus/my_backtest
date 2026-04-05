"""
Backtest Package.

This package provides backtesting utilities including:
- Data loaders for price, series, and COT data
- Estimators for trend analysis
- Utility functions for calendar, analysis, and plotting

Modules:
    - data_loader: Data loading utilities
    - estimators: Signal estimators (Kalman filter, etc.)
    - utils: Utility functions and constants
    - backtester: Main backtesting classes
"""

from backtest.utils import (
    # Constants
    MONTH_CODES,
    MONTH_TO_CODE,
    MONTH_TO_NUM,
    NUM_TO_MONTH,
    CONTRACT_TYPE,
    CONTRACT_FACTOR,
    PREROLL,
    # Calendar functions
    load_business_days,
    load_business_days_cmd,
    contract_to_nearby,
    calculate_leftdays,
    get_last_trading_day,
    get_last_trading_days,
    load_nearby_series,
    # Analysis functions
    load_future_data,
    load_and_process,
    monthly_pnl_attribution,
    calculate_drawdown,
    calculate_drawdown_stats,
    plot_var,
    calculate_var,
)

from backtest.data_loader import (
    BaseLoader,
    PriceLoader,
    SeriesLoader,
    COTLoader,
)

from backtest.estimators import (
    KalmanTrendEstimator,
)

__all__ = [
    # Constants
    'MONTH_CODES',
    'MONTH_TO_CODE',
    'MONTH_TO_NUM',
    'NUM_TO_MONTH',
    'CONTRACT_TYPE',
    'CONTRACT_FACTOR',
    'PREROLL',
    # Calendar functions
    'load_business_days',
    'load_business_days_cmd',
    'contract_to_nearby',
    'calculate_leftdays',
    'get_last_trading_day',
    'get_last_trading_days',
    'load_nearby_series',
    # Analysis functions
    'load_future_data',
    'load_and_process',
    'monthly_pnl_attribution',
    'calculate_drawdown',
    'calculate_drawdown_stats',
    'plot_var',
    'calculate_var',
    # Data loaders
    'BaseLoader',
    'PriceLoader',
    'SeriesLoader',
    'COTLoader',
    # Estimators
    'KalmanTrendEstimator',
]