"""
Utils package for the backtest module.

This package contains utility functions and constants for:
- Constants and mappings (constants.py)
- Calendar and date utilities (calendar.py)
- Analysis and plotting functions (analysis.py)
"""

# Import from constants
from backtest.utils.constants import (
    MONTH_CODES,
    MONTH_TO_CODE,
    MONTH_TO_NUM,
    NUM_TO_MONTH,
    CONTRACT_TYPE,
    CONTRACT_FACTOR,
    PREROLL,
)

# Import from calendar
from backtest.utils.calendar import (
    load_business_days,
    load_business_days_cmd,
    contract_to_nearby,
    calculate_leftdays,
    get_last_trading_day,
    get_last_trading_days,
    load_nearby_series,
)

# Import from analysis
from backtest.utils.analysis import (
    load_future_data,
    load_and_process,
    load_and_run_bt,
    monthly_pnl_attribution,
    calculate_drawdown,
    plot_drawdowns,
    calculate_drawdown_stats,
    plot_var,
    calculate_var,
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
    'load_and_run_bt',
    'monthly_pnl_attribution',
    'calculate_drawdown',
    'plot_drawdowns',
    'calculate_drawdown_stats',
    'plot_var',
    'calculate_var',
]