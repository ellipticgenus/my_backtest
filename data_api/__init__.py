"""
Data API Module.

Provides unified interface for downloading data from various data sources.
Currently supports:
- Wind (万得) API
"""

from data_api.base import BaseDataPipeline, DataDownloader
from data_api.wind import WindDownloader, WindPipeline

__all__ = [
    'BaseDataPipeline',
    'DataDownloader',
    'WindDownloader',
    'WindPipeline',
]