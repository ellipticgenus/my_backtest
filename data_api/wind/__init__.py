"""
Wind (万得) Data API Module.

Provides interface for downloading data from Wind financial terminal.
https://www.wind.com.cn/

Requirements:
- Wind Terminal must be running
- WindPy package must be installed (comes with Wind Terminal)
"""

from data_api.wind.downloader import WindDownloader
from data_api.wind.pipeline import WindPipeline
from data_api.wind.config import WindConfig, WIND_SYMBOLS

__all__ = [
    'WindDownloader',
    'WindPipeline',
    'WindConfig',
    'WIND_SYMBOLS',
]