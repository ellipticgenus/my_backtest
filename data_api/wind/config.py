"""
Configuration for Wind Data API.

Contains configuration classes and common symbol definitions.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path


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


# Common Wind symbol definitions
WIND_SYMBOLS = {
    # ============ Stock Indices (股指) ============
    'indices': {
        'SH000001': '上证指数',
        'SH000300': '沪深300',
        'SH000016': '上证50',
        'SH000905': '中证500',
        'SH000852': '中证1000',
        'SZ399001': '深证成指',
        'SZ399005': '中小板指',
        'SZ399006': '创业板指',
    },
    
    # ============ Stock Index Futures (股指期货) ============
    'index_futures': {
        'IF.CFE': '沪深300股指期货',
        'IC.CFE': '中证500股指期货',
        'IH.CFE': '上证50股指期货',
        'IM.CFE': '中证1000股指期货',
    },
    
    # ============ Commodity Futures (商品期货) ============
    'commodity_futures': {
        # Shanghai Futures Exchange (SHFE - 上期所)
        'AU.SHF': '黄金',
        'AG.SHF': '白银',
        'CU.SHF': '铜',
        'AL.SHF': '铝',
        'ZN.SHF': '锌',
        'PB.SHF': '铅',
        'NI.SHF': '镍',
        'SN.SHF': '锡',
        'RB.SHF': '螺纹钢',
        'WR.SHF': '线材',
        'HC.SHF': '热轧卷板',
        'SS.SHF': '不锈钢',
        'FU.SHF': '燃料油',
        'BU.SHF': '沥青',
        'RU.SHF': '橡胶',
        'SP.SHF': '纸浆',
        'AO.SHF': '氧化铝',
        'BR.SHF': '丁二烯橡胶',
        'EC.SHF': '集运指数(欧线)',
        
        # Dalian Commodity Exchange (DCE - 大商所)
        'C.DCE': '玉米',
        'CS.DCE': '玉米淀粉',
        'A.DCE': '豆一',
        'B.DCE': '豆二',
        'M.DCE': '豆粕',
        'Y.DCE': '豆油',
        'P.DCE': '棕榈油',
        'JD.DCE': '鸡蛋',
        'L.DCE': '塑料(LLDPE)',
        'V.DCE': 'PVC',
        'PP.DCE': '聚丙烯',
        'J.DCE': '焦炭',
        'JM.DCE': '焦煤',
        'I.DCE': '铁矿石',
        'FB.DCE': '纤维板',
        'BB.DCE': '胶合板',
        'PG.DCE': '液化石油气',
        'EG.DCE': '乙二醇',
        'RR.DCE': '粳米',
        'EB.DCE': '苯乙烯',
        'LH.DCE': '生猪',
        'PK.DCE': '花生',
        
        # Zhengzhou Commodity Exchange (ZCE - 郑商所)
        'CF.ZCE': '棉花',
        'SR.ZCE': '白糖',
        'TA.ZCE': 'PTA',
        'MA.ZCE': '甲醇',
        'FG.ZCE': '玻璃',
        'RS.ZCE': '菜籽',
        'RM.ZCE': '菜粕',
        'ZC.ZCE': '动力煤',
        'JR.ZCE': '粳稻',
        'LR.ZCE': '晚籼稻',
        'SF.ZCE': '硅铁',
        'SM.ZCE': '锰硅',
        'OI.ZCE': '菜油',
        'CY.ZCE': '棉纱',
        'AP.ZCE': '苹果',
        'CJ.ZCE': '红枣',
        'UR.ZCE': '尿素',
        'SA.ZCE': '纯碱',
        'PF.ZCE': '短纤',
        'PK.ZCE': '花生',
        'SH.ZCE': '烧碱',
        'PX.ZCE': '对二甲苯',
        'PR.ZCE': '瓶片',
        
        # Guangzhou Futures Exchange (GFE - 广期所)
        'SI.GFE': '工业硅',
        'LC.GFE': '碳酸锂',
        
        # China Financial Futures Exchange (CFE - 中金所)
        'T.CFE': '10年期国债期货',
        'TF.CFE': '5年期国债期货',
        'TS.CFE': '2年期国债期货',
        'TL.CFE': '30年期国债期货',
    },
    
    # ============ FX (外汇) ============
    'fx': {
        'USDCNY': '美元人民币',
        'USDCNH': '美元离岸人民币',
        'EURCNY': '欧元人民币',
        'JPYCNY': '日元人民币',
        'GBPCNY': '英镑人民币',
    },
    
    # ============ Funds (基金) ============
    'funds': {
        # Example fund codes - users should add their own
        # '000001.OF': '华夏成长',
    },
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