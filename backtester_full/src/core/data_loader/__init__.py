"""
Data Loader Module for the Backtester.

Provides unified data loading for prices, timeseries, signals, and COT data.
Supports both CSV and Parquet formats.
"""

from backtester_full.src.core.data_loader.base_loader import BaseLoader
from backtester_full.src.core.data_loader.price_loader import PriceLoader
from backtester_full.src.core.data_loader.series_loader import SeriesLoader
from backtester_full.src.core.data_loader.cot_loader import COTLoader

__all__ = [
    'BaseLoader',
    'PriceLoader', 
    'SeriesLoader',
    'COTLoader'
]