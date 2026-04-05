"""
Units Module Package.

This package provides units modules for:
- Rolling: Simple rolling and preroll modules
- DynamicRolling: Dynamic rolling module
- VarAdjustment: Variance adjustment module
- VolTarget: Volatility targeting module
- VolTargetStrategy: Volatility targeting strategy module
- Skew: Skew module
- Trend: Trend module
- TSSignal: Time series signal modules
"""

from backtester_full.src.core.units_module.rolling import SimpleRolling, PreRoll
from backtester_full.src.core.units_module.dynamic_rolling import DynamicRolling
from backtester_full.src.core.units_module.var_adjustment import VarAdjustment
from backtester_full.src.core.units_module.vol_target import VolTarget
from backtester_full.src.core.units_module.vol_target_strategy import (
    VolTargetStrategy,
    NotionalMatch,
)
from backtester_full.src.core.units_module.skew import Skew
from backtester_full.src.core.units_module.trend import Trend
from backtester_full.src.core.units_module.ts_signal import (
    TS_Trend_COT,
    TS_Trend_COT1,
    TS_Trend_KalmanFilter,
    TS_Trend_KalmanFilter1,
    TS_Trend_KalmanFilter_Shift,
    TS_Trend_KalmanFilter_CS,
    TS_Reversion,
    TS_Reversion_Bollinger_RSI,
    TS_ML_Signal,
    TS_Trend_KalmanFilter_Zscore,
)

__all__ = [
    'SimpleRolling',
    'PreRoll',
    'DynamicRolling',
    'VarAdjustment',
    'VolTarget',
    'VolTargetStrategy',
    'NotionalMatch',
    'Skew',
    'Trend',
    'TS_Trend_COT',
    'TS_Trend_COT1',
    'TS_Trend_KalmanFilter',
    'TS_Trend_KalmanFilter1',
    'TS_Trend_KalmanFilter_Shift',
    'TS_Trend_KalmanFilter_CS',
    'TS_Reversion',
    'TS_Reversion_Bollinger_RSI',
    'TS_ML_Signal',
    'TS_Trend_KalmanFilter_Zscore',
]