import os
import sys
parent_dir = "C:/Users/yuhang.hou/projects/holidays/poc_backtester/data_pipeline/universal"

import pandas as pd
import os
sys.path.append(parent_dir)
holidays_path = "C:/Users/yuhang.hou/projects/holidays"
sys.path.append(holidays_path)
from backtester import *
from utils import *
import numpy as np
import pandas as pd   
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from pandas.tseries.offsets import BQuarterEnd, DateOffset
from commodity.commodity import Commodity
from my_holidays.cbt import CBT
from my_holidays.dce import DCE

from commodity.commodity import Commodity


CONTRACT_FACTOR = {
    'monthly': 1,

}

NUM_TO_MONTH = {
        1: 'F',  # January
        2: 'G',  # February
        3: 'H',  # March
        4: 'J',  # April
        5: 'K',  # May
        6: 'M',  # June
        7: 'N',  # July
        8: 'Q',  # August
        9: 'U',  # September
        10: 'V',  # October
        11: 'X',  # November
        12: 'Z'   # December
    }

def month_mapping(month):
    """
    transfer month of format Nov-25 to X25
    params month: str with format Nov-25
    return: str, month code for future contract
    """
    month_dict = {
        "Jan": "F", "Feb": "G", "Mar": "H", "Apr": "J",
        "May": "K", "Jun": "M", "Jul": "N", "Aug": "Q",
        "Sep": "U", "Oct": "V", "Nov": "X", "Dec": "Z"
    }
    return month_dict[month[:3]] + month[-2:]


def save_contracts_to_csv(df, file_path):
    """
    Save contract dataframes to CSV files.
    """
    def process_and_save_contracts(contract_df, mapping_func):
        """
        Process and save contract data to CSV files.

        Parameters:
        contract_df (pd.DataFrame): DataFrame containing contract data.
        mapping_func (function): Mapping function to apply to 'month_code' column.

        Returns:
        None
        """
        contract_df = contract_df.reset_index()
        contract_df = contract_df.drop_duplicates(subset=['date', 'month_code'])
        contract_df['date'] = pd.to_datetime(contract_df['date'])
        pivot_df = contract_df.pivot(index='date', columns='month_code', values='close')
        
        for col in pivot_df.columns:
            current_data = pivot_df[[col]].rename(columns={col: 'close'})
            current_data['return'] = current_data['close'] / current_data['close'].shift(1) - 1
            current_data['log_return'] = np.log(current_data['close'] / current_data['close'].shift(1))
            file_name = f"{file_path}/{col}.csv"
            if os.path.exists(file_name):
            # Load existing data
                existing_data = pd.read_csv(file_name, index_col='date', parse_dates=True)
                currents_data_to_use = current_data.dropna(subset=['close'])
            # Find the last common date to check consistency
                common_dates = currents_data_to_use.index.intersection(existing_data.index)
            
                if not common_dates.empty:
                    # Compare overlapping data
                    last_common_date = common_dates[-1]
                    existing_value = existing_data.loc[last_common_date, 'close']
                    current_value = current_data.loc[last_common_date, 'close']
                    
                    if not np.isclose(existing_value, current_value, rtol=1e-10):
                        print(f"Data mismatch for {col} on {last_common_date}. Please Check.")
                        # current_data.to_csv(file_name)
                        continue
                
                # Append only new data
                new_data = current_data[~current_data.index.isin(existing_data.index)]
                if not new_data.empty:
                    combined_data = pd.concat([existing_data, new_data])
                    combined_data.to_csv(file_name)
                    print(f"Appended {len(new_data)} new rows to {col}.csv")
                else:
                    print(f"No new data to append for {col}")
            else:
                # Save new file if it doesn't exist
                if not os.path.exists(os.path.dirname(file_name)):
                    os.makedirs(os.path.dirname(file_name))
                    print(os.path.dirname(file_name))
                current_data.to_csv(file_name)
                print(file_name)
                print(f"Created new file for {col}")

    # Filter dataframes by period type and process
    monthly_contracts = df

    
    process_and_save_contracts(monthly_contracts, month_mapping)

def save_spot_to_csv(df, file_path):
    """
    save dataframe to csv
    """
    new_data= df[(df['TradeRate_Source'] == 'Spot' )]
    new_data = new_data[['date','close']].set_index('date')
    new_data['return'] = new_data['close'].pct_change()
    new_data['log_return'] = np.log(new_data['close'] / new_data['close'].shift(1))

    file_full_path = os.path.join(file_path, 'spot.csv')


    if os.path.exists(file_full_path):
        # Load existing data
        existing_data = pd.read_csv(file_full_path, index_col='date', parse_dates=True)
        
        # Find overlapping dates
        common_dates = new_data.index.intersection(existing_data.index)
        
        if not common_dates.empty:
            # Validate overlapping close prices
            close_mismatch = ~np.isclose(
                existing_data.loc[common_dates, 'close'],
                new_data.loc[common_dates, 'close'],
                rtol=1e-10  # 0.001% tolerance
            )
            
            if close_mismatch.any():
                mismatch_dates = common_dates[close_mismatch]
                raise ValueError(
                    f"Close price mismatch on dates: {mismatch_dates.values}\n"
                    f"Existing: {existing_data.loc[mismatch_dates, 'close'].values}\n"
                    f"New: {new_data.loc[mismatch_dates, 'close'].values}"
                )
        
        # Merge data (keep existing where dates overlap)
        combined_data = pd.concat([
            existing_data,
            new_data[~new_data.index.isin(existing_data.index)]  # Only new dates
        ]).sort_index()
        
        # Recalculate metrics for entire series (ensures consistency)
        combined_data['return'] = combined_data['close'].pct_change()
        combined_data['log_return'] = np.log(combined_data['close'] / combined_data['close'].shift(1))
        combined_data['var'] = combined_data['return'].rolling(252).quantile(0.05)
        combined_data = combined_data[['close', 'return', 'log_return', 'var']]
        # Save merged data
        combined_data.to_csv(file_full_path)
        print(f"Successfully appended {len(new_data) - len(common_dates)} new records")
    
    else:
        # Calculate VaR for initial data
        new_data['var'] = new_data['return'].rolling(252).quantile(0.05)
        if not os.path.exists(os.path.dirname(file_full_path)):
            os.makedirs(os.path.dirname(file_full_path))
            print(os.path.dirname(file_full_path))
        new_data.to_csv(file_full_path)
        print("Created new spot data file")

def _check_mismatch(df1, df2, dates, _column):
            # Validate against existing data
    mismatch = ~np.isclose(
        df1.loc[dates, _column],
        df2[_column],
        rtol=1e-8, atol=1e-8
    )
    
    if mismatch.any():
        error_dates = dates[mismatch]
        raise ValueError(
            f"Data mismatch on dates: {error_dates}\n"
            f"Existing: {df1.loc[error_dates, _column].values}\n"
            f"New calc: {df2.loc[error_dates, _column].values}"
        )

def load_business_days(exchange='CBT'):
    if exchange == 'CBT':
        holiday_helper = CBT()
    elif exchange == 'DCE':
        holiday_helper = DCE()
    res = []
    for date in pd.date_range(start='2000-01-01', end='2030-12-31'):
        if date.weekday() > 4:
            continue
        if date.strftime('%Y-%m-%d') in holiday_helper:
            continue
        res.append(date)
    return res

def load_business_days_cmd(cmd = 'S'):
    _helper = Commodity(cmd)
    _holiday = _helper.holiday
    return load_business_days(_holiday)

def last_day_of_quarter(_date):
    """
    Get the last day of the quarter for a given date.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :return: str, last day of the quarter in 'YYYY-MM-DD' format
    """
    new_date = pd.to_datetime(_date)
    next_q_end = new_date + BQuarterEnd(0)
    return next_q_end


def last_day_of_year(_date):
    _date = pd.to_datetime(_date)
    return pd.to_datetime(f'{_date.year}-12-31' )

def date_to_month(_date):
    """
    Convert a date to a future contract month code.

    :param date: str or datetime, date to be converted.
    :return: str, month code in the format "MYY", where M is the month code 
             (e.g. 'F' for January) and YY are the last two digits of the year.
    """

    _date = pd.to_datetime(_date)
    month_code = NUM_TO_MONTH[_date.month]
    year_code = str(_date.year)[-2:]  # Last 2 digits of year
    code =  f"{month_code}{year_code}"
    return code

def get_next_contract(given_date, month_codes ):
    if not month_codes:
        return None, None
    
    sorted_codes = sorted(month_codes.items(), key=lambda x: x[1])
    # Find the next contract after the given/current date
    for code, last_trading_day in sorted_codes:
        if pd.to_datetime(last_trading_day) > given_date:
            return code, last_trading_day
    return None, None

def get_next_contract_fast(given_date, month_codes):
    """Optimized with pre-sorting and vectorized operations."""
    if not month_codes:
        return None, None
    
    given_date = pd.to_datetime(given_date)
    sorted_items = sorted(month_codes.items(), key=lambda x: pd.to_datetime(x[1]))
    import bisect
    dates = [pd.to_datetime(date) for _, date in sorted_items]
    idx = bisect.bisect_right(dates, given_date)
    
    if idx < len(sorted_items):
        return sorted_items[idx]
    return None, None

def get_next_contract_df(given_date, month_codes):
    """Optimized with pre-sorting and vectorized operations."""
    if len(month_codes) == 0:
        return None, None
    
    given_date = pd.to_datetime(given_date)
    _month_codes = month_codes.copy()
    _month_codes.sort_values(['last_trading_day'], inplace = True)
    res = _month_codes[_month_codes['last_trading_day'] > given_date]
    if len(res):
        res = res.iloc[0]
        print(datetime.now(),given_date,res['contract_code'], res['last_trading_day'] )
        return res['contract_code'], res['last_trading_day']
    else:
        return None, None

def nth_nearby(_date, nth, preroll = 7, business_days = None, last_trading_days = None):
    """
    Calculate the nth nearby future contract code.

    :param date: datetime 
    :param nth: int
    :param preroll: int (default 7)
    :param cmd: str (default 'C5TC')
    :return: str, the nth nearby future contract code
    """
    if business_days is None:
        buz_days = load_business_days()
    else:
        buz_days = business_days
    
    future_dates  =[ i  for i in buz_days if i > _date]  
    new_date = future_dates[preroll]
    # print(new_date,cmd)

    contract_date = new_date
    # print(type(contract_date))
    if last_trading_days is None:
        ltds = load_last_trading_days()
    else:
        ltds = last_trading_days

    ltds_df = pd.DataFrame(list(ltds.items()), columns=['contract_code', 'last_trading_day'])    
    ltds_df['last_trading_day'] = pd.to_datetime(ltds_df['last_trading_day'])
    contract,_ = get_next_contract_df(contract_date,ltds_df)
    # contract,_ = get_next_contract_fast(contract_date,ltds)
    return contract

def _calculate_nearby_data(_df,cmd,  k_nearby, roll_schedule):
    """Helper function to calculate nearby contract data"""
    business_days = load_business_days_cmd(cmd)
    ltds = load_last_trading_days(cmd)
    print(f'start calculating contracts for spot {datetime.now()}')
    days_to_use = [d for d in _df.index if d in business_days]
    df = _df.loc[days_to_use].copy()
    print(days_to_use)
    k_nearby_spot = [nth_nearby(date, k_nearby, roll_schedule, business_days=business_days, last_trading_days=ltds) 
                    for date in days_to_use]
    print(f'start calculating contracts for return {datetime.now()}')
    k_nearby_return = [nth_nearby(date, k_nearby, roll_schedule-1, business_days=business_days, last_trading_days=ltds) 
                      for date in days_to_use]
    print(f'finished contract calculating {datetime.now()}')
    nearby_spot = []
    nearby_return = []
    nearby_log_return = []
    
    for i, date in enumerate(df.index):
        spot_contract = k_nearby_spot[i]
        return_contract = k_nearby_return[i]
        
        spot_val = df.loc[date, ('close', spot_contract)] if spot_contract in df['close'].columns else np.nan
        ret_val = df.loc[date, ('return', return_contract)] if return_contract in df['return'].columns else np.nan
        log_ret_val = df.loc[date, ('log_return', return_contract)] if return_contract in df['log_return'].columns else np.nan
        print(f'{date} {spot_contract} {return_contract} {spot_val} {ret_val} {log_ret_val}')

        nearby_spot.append(spot_val)
        nearby_return.append(ret_val)
        nearby_log_return.append(log_ret_val)
    return pd.DataFrame({
        'close': nearby_spot,
        'return': nearby_return,
        'log_return': nearby_log_return
    }, index=df.index)

def save_nth_nearby_new(df, symbol, k_nearby, roll_schedule, file_path):
    """
    Save nth nearby contract data with intelligent merging:
    1. Checks if file exists
    2. If exists, loads and validates existing data
    3. Appends new data if validation passes
    4. Creates new file if doesn't exist
    
    Args:
        df: DataFrame with multi-index columns ('close', 'return', 'log_return')
        symbol: Instrument symbol
        k_nearby: Nearby contract number
        roll_schedule: Roll schedule parameter
        file_path: Directory to save files
    """
    # Determine file name
    file_name = f'{file_path}/{symbol}_{k_nearby}_{roll_schedule}.csv'
    
    # Initialize empty curve DataFrame
    curve = pd.DataFrame(columns=['close', 'return', 'log_return', 'var'])
    curve.index.name = 'date'
    
    # Check if file exists
    if os.path.exists(file_name):
        # Load existing data
        existing_curve = pd.read_csv(file_name, index_col='date', parse_dates=True)
        
        # Get last date and previous month's data
        last_date = existing_curve.index[-1]
        start_date = last_date - timedelta(days=60)  # 1 month buffer for validation
        
        # Get new dates to process (after last existing date)
        new_dates = df.index[df.index > last_date]
        
        if len(new_dates) == 0:
            print("No new dates to process")
            return
        
        # Get overlapping period for validation (last month of existing data)
        validation_dates = df.index[(df.index >= start_date) & (df.index <= last_date)]
        # Recalculate for validation period
        recalculated = _calculate_nearby_data(df.loc[validation_dates],cmd, k_nearby, roll_schedule)
        
        # Validate against existing data
        _check_mismatch(existing_curve, recalculated, validation_dates, 'close')
        _check_mismatch(existing_curve, recalculated, validation_dates, 'return')
        _check_mismatch(existing_curve, recalculated, validation_dates, 'log_return')
        # If validation passed, keep existing data
        curve = existing_curve
        
        # Process only new dates
        process_dates = new_dates
    else:
        # Start from hard-coded date if file doesn't exist
        os.makedirs(file_path, exist_ok=True)
        process_dates = df.index
    
    # Calculate new data
    new_data = _calculate_nearby_data(df.loc[process_dates],cmd,  k_nearby, roll_schedule)
    
    # Combine old and new data
    curve = pd.concat([curve, new_data])
    
    # Calculate VaR for the entire series
    curve['var'] = curve['return'].rolling(252, min_periods=20).quantile(0.05)
    
    # Save to file
    curve.to_csv(file_name)
    print(f"Saved data to {file_name}")

def load_future_data(data_path = './data/C5TC',values = 'close'):
    """
    Load futures data from CSV files in a given directory and return a pivot table
    of the data. Each CSV file should contain a date column and a close column.
    The function will concatenate the data from all the CSV files and pivot the
    data into a single DataFrame with the date as the index and the contract as
    the column.

    Parameters
    ----------
    data_path : str, optional
        The path to the directory containing the CSV files. The default is
        'data/C5TC'.
    """
    
    files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    dfs = (pd.read_csv(os.path.join(data_path, file)).assign(
        contract=file.replace('.csv', ''),
        date=lambda df: pd.to_datetime(df['date'])
    ) for file in files)

    big_df = pd.concat(dfs, ignore_index=True)
    pivot_df = big_df.pivot(index='date', columns='contract', values= values)

    return pivot_df

def get_last_trading_day(month_code,cmd = 'S'):
    _helper = Commodity(cmd)
    return _helper.last_trading_day[month_code]

def load_last_trading_days(cmd = 'S'):
    _helper = Commodity(cmd)
    return _helper.get_last_trading_days()
        

def update_data(cmd = 'BO'):
    end_date = '2026-02-10' # incase of a rerun
    start_date = '2000-01-01'
    # df = load_db_data(index_map[which_size],start_date)
    df = pd.read_pickle(f'C:/Users/yuhang.hou/projects/holidays/poc_backtester/data_pipeline/universal/pickle_files/{cmd}_price.pickle')
    df = df[df['type'] == 'close']
    month_map = {1:'F',2:'G',3:'H',4:'J',5:'K',6:'M',7:'N',8:'Q',9:'U',10:'V',11:'X',12:'Z'}
    df['month_code'] = df['expiry_month'].map(month_map) + df['expiry_year'].astype(str).str[-2:]
    df['date'] = pd.to_datetime(df['date'])
    business_days = load_business_days_cmd(cmd)
    df = df[df['date'].isin(business_days)]
    df.set_index('date', inplace=True)
    df.rename(columns={'value':'close'},inplace=True)

    save_contracts_to_csv(df, f'C:/Users/yuhang.hou/projects/holidays/poc_backtester/data/{cmd}')
    contract_df = load_future_data(f'C:/Users/yuhang.hou/projects/holidays/poc_backtester/data/{cmd}',['close', 'return', 'log_return'])
    contract_df = contract_df[(contract_df.index<=pd.to_datetime(end_date)) & (contract_df.index>=pd.to_datetime(start_date))]
    monthly = [(i,j) for i in [0,1] for j in list(range(3,10,16))]
    for k_nearby, roll_schedule in monthly:
        save_nth_nearby_new(contract_df,cmd, k_nearby, roll_schedule, f'C:/Users/yuhang.hou/projects/holidays/poc_backtester/data/series/{cmd}')

if __name__ == '__main__':
    cmd = 'S'
    update_data(cmd)