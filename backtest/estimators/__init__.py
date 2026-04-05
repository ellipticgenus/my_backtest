"""
Estimators module for the backtest package.

Contains statistical and mathematical estimators used in trading strategies.
"""

from backtest.estimators.kalman import KalmanTrendEstimator

__all__ = ['KalmanTrendEstimator']