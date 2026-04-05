"""
Calendar utilities for the backtest package.

Contains business day calculations, holiday handling, and contract conversion utilities.
"""

import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Optional

from commodity.commodity import Commodity
from my_holiday.cbt import CBT
from my_holiday.dce import DCE

from backtest.utils.constants import MONTH_TO_NUM, NUM_TO_MONTH, CONTRACT_FACTOR, CONTRACT_TYPE


def load_business_days(exchange: str = 'CBT') -> List[pd.Timestamp]:
    """
    Load business days for a given exchange.
    
    Args:
        exchange: Exchange code ('CBT', 'DCE')
        
    Returns:
        List of business days as pandas Timestamps
    """
    if exchange == 'CBT':
        holiday_helper = CBT()
    elif exchange == 'DCE':
        holiday_helper = DCE()
    else:
        holiday_helper = CBT()
    
    res = []
    for date in pd.date_range(start='2000-01-01', end='2030-12-31'):
        if date.weekday() > 4:  # Skip weekends
            continue
        if date.strftime('%Y-%m-%d') in holiday_helper:
            continue
        res.append(date)
    return res


def load_business_days_cmd(cmd: str = 'S') -> List[pd.Timestamp]:
    """
    Load business days for a commodity command.
    
    Args:
        cmd: Commodity symbol (e.g., 'S' for Soybeans)
        
    Returns:
        List of business days as pandas Timestamps
    """
    _helper = Commodity(cmd)
    _holiday = _helper.holiday
    return load_business_days(_holiday)


def contract_to_nearby(
    _date: pd.Timestamp,
    contract: str,
    preroll: int = 0,
    contract_type: str = 'monthly',
    trading_days: Dict = None,
    buz_days: List[pd.Timestamp] = None
) -> int:
    """
    Convert a contract code to the nearest future contract code.
    
    Args:
        _date: Current date
        contract: Contract code
        preroll: Number of days to look ahead
        contract_type: 'monthly' or 'quarterly'
        trading_days: Dictionary of last trading days
        buz_days: List of business days
        
    Returns:
        Nearby contract number
    """
    future_dates = [i for i in buz_days if i > _date]
    new_date = future_dates[preroll]
    
    if contract_type == 'quarterly':
        contract = 'Q' + contract
    
    ltd = pd.to_datetime(trading_days.get(contract))
    month = MONTH_TO_NUM[contract[-3]]
    year = 2000 + int(contract[-2:])

    this_month = new_date.month
    this_year = new_date.year
    k = month - this_month + 12 * (year - this_year)
    
    if new_date >= ltd:
        k -= CONTRACT_FACTOR[contract_type]
    
    if contract_type == 'quarterly':
        k = k // 3
    
    return k


def calculate_leftdays(_date: pd.Timestamp) -> float:
    """
    Calculate the fraction of remaining days in the month.
    
    Args:
        _date: Date to calculate for
        
    Returns:
        Fraction of remaining business days in the month
    """
    if _date:
        _date = pd.to_datetime(_date)
    else:
        _date = pd.Timestamp.now()
    
    first_day = _date.replace(day=1)
    last_day = (first_day + relativedelta(months=1)) - timedelta(days=1)
    all_days = pd.date_range(start=first_day, end=last_day, freq='D')
    remaining_days = pd.date_range(start=_date, end=last_day, freq='D')

    return len(remaining_days[remaining_days.dayofweek < 5]) / len(all_days[all_days.dayofweek < 5])


def get_last_trading_day(month_code: str, cmd: str = 'S') -> pd.Timestamp:
    """
    Get the last trading day for a contract.
    
    Args:
        month_code: Contract month code (e.g., 'H25')
        cmd: Commodity symbol
        
    Returns:
        Last trading day as pandas Timestamp
    """
    _helper = Commodity(cmd)
    return _helper.last_trading_day[month_code]


def get_last_trading_days(cmd: str = 'S') -> Dict[str, pd.Timestamp]:
    """
    Get all last trading days for a commodity.
    
    Args:
        cmd: Commodity symbol
        
    Returns:
        Dictionary mapping contract codes to last trading days
    """
    _helper = Commodity(cmd)
    return _helper.get_last_trading_days()


def load_nearby_series(tickers: List[str], max_roll_date: int = 10) -> pd.DataFrame:
    """
    Load nearby series data for multiple tickers.
    
    Args:
        tickers: List of ticker symbols
        max_roll_date: Maximum roll date for nearby calculation
        
    Returns:
        DataFrame with merged nearby series data
    """
    ts_list = []
    for ticker in tickers:
        if ticker[-1] in ['Q']:
            temp = ticker[:-1]
        else:
            temp = ticker
        roll_nearbys = PREROLL.get(ticker[-1], [0, 1, 2])
        for roll_nearby in roll_nearbys:
            symbol = f'{ticker}_{roll_nearby}_{max_roll_date}'
            df = pd.read_csv(f'C:/Users/yuhang.hou/projects/pipeline/data/series/{temp}/{symbol}.csv')
            df['date'] = pd.to_datetime(df['date'])
            df = df.reset_index(drop=True).set_index('date')
            df.rename(columns={'return': symbol}, inplace=True)
            ts_list.append(df[symbol])
    merged_df = pd.concat(ts_list, axis=1)
    return merged_df
