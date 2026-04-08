"""
Configuration for Wind Data API.

Contains configuration classes and common symbol definitions.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path


# Default field lists (class-level constants for easy access)
DEFAULT_FIELDS = ['open', 'high', 'low', 'close', 'volume', 'amt', 'turn']
FUTURE_FIELDS = ['open', 'high', 'low', 'close', 'volume', 'amt', 'oi', 'settle']
INDEX_FIELDS = ['open', 'high', 'low', 'close', 'volume', 'amt']
FUND_FIELDS = ['nav', 'nav_acc']


@dataclass
class WindConfig:
    """
    Configuration class for Wind data downloading.
    
    Attributes:
        data_path: Base path for saving downloaded data
        cache_enabled: Whether to enable data caching
        retry_count: Number of retries on API failure
        retry_delay: Delay between retries in seconds
        default_fields: Default fields to fetch for market data
        timeout: API timeout in seconds
    """
    data_path: str = 'data/wind'
    cache_enabled: bool = True
    retry_count: int = 3
    retry_delay: float = 1.0
    default_fields: List[str] = field(default_factory=lambda: [
        'open', 'high', 'low', 'close', 'volume', 'amt', 'turn'
    ])
    timeout: int = 60
    
    # Future contract specific fields
    future_fields: List[str] = field(default_factory=lambda: [
        'open', 'high', 'low', 'close', 'volume', 'amt', 'oi', 'settle'
    ])
    
    # Index specific fields
    index_fields: List[str] = field(default_factory=lambda: [
        'open', 'high', 'low', 'close', 'volume', 'amt'
    ])
    
    # Fund specific fields
    fund_fields: List[str] = field(default_factory=lambda: [
        'nav', 'nav_acc'
    ])
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            'data_path': self.data_path,
            'cache_enabled': self.cache_enabled,
            'retry_count': self.retry_count,
            'retry_delay': self.retry_delay,
            'default_fields': self.default_fields,
            'timeout': self.timeout,
            'future_fields': self.future_fields,
            'index_fields': self.index_fields,
            'fund_fields': self.fund_fields,
        }


def get_exchange_from_symbol(symbol: str) -> str:
    """
    Get exchange name from Wind symbol.
    
    Args:
        symbol: Wind symbol (e.g., 'AU.SHF', 'IF.CFE')
        
    Returns:
        Exchange name
    """
    exchange_map = {
        'SHF': '上海期货交易所',
        'DCE': '大连商品交易所',
        'ZCE': '郑州商品交易所',
        'GFE': '广州期货交易所',
        'CFE': '中国金融期货交易所',
        'SH': '上海证券交易所',
        'SZ': '深圳证券交易所',
        'OF': '公募基金',
    }
    
    parts = symbol.split('.')
    if len(parts) == 2:
        exchange_code = parts[1]
        return exchange_map.get(exchange_code, exchange_code)
    
    return 'Unknown'


# Wind symbol definitions
WIND_SYMBOLS = {
    'commodity_futures': {
        'AU.SHF': 'Gold',
        'AG.SHF': 'Silver',
        'CU.SHF': 'Copper',
        'AL.SHF': 'Aluminum',
        'ZN.SHF': 'Zinc',
        'RB.SHF': 'Rebar',
        'RU.SHF': 'Rubber',
        'C.DCE': 'Corn',
        'A.DCE': 'Soybean No.1',
        'M.DCE': 'Soybean Meal',
        'Y.DCE': 'Soybean Oil',
        'P.DCE': 'Palm Oil',
        'CF.ZCE': 'Cotton',
        'SR.ZCE': 'White Sugar',
        'TA.ZCE': 'PTA',
    },
    'index_futures': {
        'IF.CFE': 'CSI 300',
        'IC.CFE': 'CSI 500',
        'IH.CFE': 'SSE 50',
        'IM.CFE': 'CSI 1000',
    },
    'bond_futures': {
        'T.CFE': '10Y Treasury',
        'TF.CFE': '5Y Treasury',
        'TS.CFE': '2Y Treasury',
        'TL.CFE': '30Y Treasury',
    },
    'indices': {
        'SH000001': 'SSE Composite',
        'SH000300': 'CSI 300',
        'SH000905': 'CSI 500',
        'SH000852': 'CSI 1000',
    },
}


def get_symbol_type(symbol: str) -> str:
    """
    Determine the type of symbol.
    
    Args:
        symbol: Wind symbol
        
    Returns:
        Symbol type ('index', 'future', 'stock', 'fund', 'fx', 'unknown')
    """
    if '.' not in symbol:
        # Could be FX or simple index
        if any(c in symbol for c in ['CNY', 'CNH', 'USD', 'EUR', 'JPY', 'GBP']):
            return 'fx'
        return 'unknown'
    
    exchange = symbol.split('.')[1]
    
    if exchange == 'CFE':
        if any(x in symbol for x in ['IF', 'IC', 'IH', 'IM']):
            return 'index_future'
        elif any(x in symbol for x in ['T', 'TF', 'TS', 'TL']):
            return 'bond_future'
        return 'future'
    elif exchange in ['SHF', 'DCE', 'ZCE', 'GFE']:
        return 'commodity_future'
    elif exchange in ['SH', 'SZ']:
        return 'stock'
    elif exchange == 'OF':
        return 'fund'
    
    return 'unknown'