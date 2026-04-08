"""
Configuration for Wind Data API.

Contains configuration classes and common symbol definitions.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path


# Default field lists (class-level constants for easy access)
DEFAULT_FIELDS = ['open', 'high', 'low', 'close', 'volume']
FUTURE_FIELDS = ['open', 'high', 'low', 'close', 'volume', 'oi', 'settle']
INDEX_FIELDS = ['open', 'high', 'low', 'close', 'volume']


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




def get_symbol_type(symbol: str) -> str:
    """
    Determine the type of symbol.
    
    Args:
        symbol: Wind symbol
        
    Returns:
        Symbol type ('commodity_future' or 'unknown')
    """
    
    exchange = symbol.split('.')[1]
    if exchange in [ 'DCE', 'ZCE', 'CBT','ICE']:
        return 'commodity_future'
    return 'unknown'