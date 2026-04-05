import holidays
import pandas as pd
import re
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BQuarterEnd, DateOffset

# Lazy imports to avoid circular dependency
# CBT, DCE, CZCE, ICE are imported inside functions that need them

MONTH_TO_NUM  = {  'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12 }
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

CONTRACT_FACTOR = {
    'monthly': 1,
    'quarterly': 3,
    'yearly': 12
}

def parse_rule(rule):
    """
    Parse a rule string with format like '-3B', '2M', etc.
    Returns (minus_sign, integer, last_symbol)
    """
    match = re.match(r'^([+-])?(\d+)(\w)$', rule)
    if match:
        minus = match.group(1)  # '-' or None
        number = int(match.group(2))
        symbol = match.group(3)
        return minus, number, symbol
    else:
        raise ValueError(f"Invalid rule format: {rule}")
        
    
def is_holiday(_date, country='US',exchange = None):
    """
    Check if a given date is a holiday in the specified country.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :param country: str, country code (default is 'US')
    :return: bool, True if the date is a holiday, False otherwise
    """
    _date = pd.to_datetime(_date)
    if exchange is None:
        holiday_list = my_holiday_list(country)
        return _date in holiday_list

def my_holiday_list(country):
    if country in ['CBT', 'DCE','ICE','NYB','CZCE']:
        # Lazy imports to avoid circular dependency
        from my_holiday.cbt import CBT
        from my_holiday.dce import DCE, CZCE
        from my_holiday.ice import ICE
        
        if country == 'CBT':
            return CBT()
        elif country == 'CZCE':
            return CZCE()
        elif country == 'DCE':
            return DCE()
        elif country in ['ICE','NYB']:
            return ICE()
    country = country.split('_')
    if(len(country)==2):
        holiday_list = holidays.CountryHoliday(country[0], subdiv=country[1])
    else:
        holiday_list = holidays.CountryHoliday(country[0])
    return holiday_list

def last_day_of_month(_date):
    """
    Get the last day of the month for a given date.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :return: str, last day of the month in 'YYYY-MM-DD' format
    """
    _date = pd.to_datetime(_date)
    next_month = _date + pd.DateOffset(months=1)
    last_day = next_month - pd.DateOffset(days=next_month.day)
    return last_day.strftime('%Y-%m-%d')

def last_business_day(_date, country='UK_ENG'):
    """
    Get the last business day before a given date.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :param country: str, country code (default is 'UK_ENG')
    :return: str, last business day in 'YYYY-MM-DD' format
    """
    _date = last_day_of_month(_date)
    _date = pd.to_datetime(_date)

    # Move back to the last business day
    while is_holiday(_date, country) or _date.weekday() >= 5:  # 5 and 6 are Saturday and Sunday
        _date -= pd.Timedelta(days=1)
    return _date.strftime('%Y-%m-%d')

def last_day_of_quarter(_date):
    """
    Get the last day of the quarter for a given date.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :return: str, last day of the quarter in 'YYYY-MM-DD' format
    """
    new_date = pd.to_datetime(_date)
    next_q_end = new_date + BQuarterEnd(0)
    return next_q_end

def last_buz_of_quarter(_date, country='UK_ENG'):
    """
    Get the last business day of the quarter for a given date.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :param country: str, country code (default is 'UK_ENG')
    :return: str, last business day of the quarter in 'YYYY-MM-DD' format
    """
    last_day = last_day_of_quarter(_date)
    if is_holiday(last_day, country) or last_day.weekday() >= 5:
        return previous_business_day(last_day, country)
    return last_day

def last_day_of_year(_date):
    _date = pd.to_datetime(_date)
    return pd.to_datetime(f'{_date.year}-12-31' )

def last_buz_of_year(_date, country='UK_ENG'):
    last_day = last_day_of_year(_date)
    if is_holiday(last_day, country) or last_day.weekday() >= 5:
        return previous_business_day(last_day, country)
    return last_day

def next_business_day(_date, country='UK_ENG'):
    """
    Get the next business day after a given date.
    
    :param date: str, date in 'YYYY-MM-DD' format
    :param country: str, country code (default is 'UK_ENG')
    :return: str, next business day in 'YYYY-MM-DD' format
    """
    _date = pd.to_datetime(_date)
    _date += pd.Timedelta(days=1)
    # print(date)
    while is_holiday(_date, country) or _date.weekday() >= 5:  # 5 and 6 are Saturday and Sunday
        _date += pd.Timedelta(days=1)
    return _date.strftime('%Y-%m-%d')
    
def previous_business_day(_date, country='UK_ENG'):
    """
    Get the previous business day before a given date.
    :param date: str, date in 'YYYY-MM-DD' format
    :param country: str, country code (default is 'UK_ENG')
    :return: str, previous business day in 'YYYY-MM-DD' format
    """
    _date = pd.to_datetime(_date)
    _date -= pd.Timedelta(days=1)
    while is_holiday(_date, country) or _date.weekday() >= 5:  # 5 and 6 are Saturday and Sunday
        _date -= pd.Timedelta(days=1)
    return _date.strftime('%Y-%m-%d')

def weekdays_between(start_date, end_date):
    """
    Get weekdays between two dates.
    
    :param start_date: str, start date in 'YYYY-MM-DD' format
    :param end_date: str, end date in 'YYYY-MM-DD' format
    :return: list of weekdays between the two dates
    """
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    weekdays = pd.date_range(start=start_date, end=end_date, freq='B')

    return [date.strftime('%Y-%m-%d') for date in weekdays]

def business_days_between(start_date, end_date, country='US'):
    """
    Get the business days between two dates.
    
    :param start_date: str, start date in 'YYYY-MM-DD' format
    :param end_date: str, end date in 'YYYY-MM-DD' format
    :param country: str, country code (default is 'US')
    :return: list, business days between the two dates
    """
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    business_days = pd.date_range(start=start_date, end=end_date, freq='B')
    holiday_list = my_holiday_list(country)
    # Filter out holidays
    business_days = [day for day in business_days if day not in holiday_list]
    
    return business_days

def apply_date_rule(_date, rules, calendar='US'):
    """
    Apply a set of date rules to a given date.
    B one business day based on calendar
    M one month based(same day if possible)
    m last business day of month
    W one week based(same day if possible)
    w one week and business day
    Q one quarter based(same day if possible)
    q last business day of quarter
    X Christmas Day
    :param date: str, date in 'YYYY-MM-DD' format
    :param rules: string 
    :return: str, modified date in 'YYYY-MM-DD' format
    """
    if rules == 'SPOT':
        return _date
    plus_minus, number, symbol = parse_rule(rules)
    if not isinstance(_date, str):
        _date = _date.strftime('%Y-%m-%d')
    if symbol == 'B':
        if plus_minus == '-':
            while number>0:
                _date = previous_business_day(_date, calendar)
                number -= 1
        else:
            while number>0:
                _date = next_business_day(_date, calendar)
                number -= 1
    elif symbol == 'd':
        _date = pd.to_datetime(_date)
        if plus_minus == '-':
                _date -=  pd.Timedelta(days = +number)
        else:
                _date +=  pd.Timedelta(days = +number)
        _date = _date.strftime('%Y-%m-%d')
    elif symbol in ["M",'m']:
        _date = pd.to_datetime(_date)
        if plus_minus == '-':
                _date -=  relativedelta(months = +number)
        else:
                _date +=  relativedelta(months = +number)
        if symbol == 'm':
            _date = last_business_day(_date, calendar)
        else:
            _date = _date.strftime('%Y-%m-%d')
    elif symbol in ["W",'w']:
        _date = pd.to_datetime(_date)
        if plus_minus == '-':
            _date -=  relativedelta(weeks = +number)
            if symbol == 'w':
                _date = previous_business_day(_date, calendar)
            else:
                _date = _date.strftime('%Y-%m-%d')
        else:
            _date +=  relativedelta(weeks = +number)
            if symbol == 'w':
                _date = next_business_day(_date, calendar)
            else:
                _date = _date.strftime('%Y-%m-%d')
    elif symbol in ["Y",'y']:
        _date = pd.to_datetime(_date)
        if plus_minus == '-':
            _date -=  relativedelta(years = +number)
            if symbol == 'y':
                _date = previous_business_day(_date, calendar)
            else:
                _date = _date.strftime('%Y-%m-%d')
        else:
            _date +=  relativedelta(years = +number)
            if symbol == 'y':
                _date = next_business_day(_date, calendar)
            else:
                _date = _date.strftime('%Y-%m-%d')

    elif symbol in ['Q','q']:
        _date = pd.to_datetime(_date)
        _date += pd.offsets.QuarterEnd(0)
        if plus_minus == '-':
                _date -=  pd.offsets.QuarterEnd(number)
        else:
                _date +=  pd.offsets.QuarterEnd(number)
        if symbol == 'q':
            _date = last_business_day(_date, calendar)
        else:
            _date = _date.strftime('%Y-%m-%d')
    elif symbol == 'X':
        _date = pd.to_datetime(_date)
        if plus_minus == '-':
            _date -= relativedelta(years=number)
        else:
            _date += relativedelta(years=number)
        _date = f'{_date.year}-12-24'
    else:
        raise ValueError(f"Unsupported symbol: {symbol}")

    return _date

def apply_date_rules(_date, rules, calendar='US'):
    rules_list = re.findall(r'([+-]?\d+[a-zA-Z]\b)', rules)
    for rule in rules_list:
        _date = apply_date_rule(_date, rule, calendar)
    return _date


def business_days_until(end_date, period, calendar='UK_ENG'):
    """
    Calculate the number of business days until a given date.
    :param end_date: datetime 
    :param period: int
    :return: []
    """
    dates = []
    current_date = end_date
    if isinstance(current_date, str):
        current_date = pd.to_datetime(current_date)
    while len(dates) < period:
        if not (is_holiday(current_date, calendar) or current_date.weekday() >= 5):
            dates.append(current_date)
        current_date -= pd.Timedelta(days=1)
    return dates[::-1]

def business_days_from(start_date, period, calendar='UK_ENG'):
    '''
    Calculate the number of business days from a given date.
    :param start_date: datetime 
    :param period: int
    :return: []
    '''
    end_date = start_date
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    dates = []
    while len(dates) < period:
        if not (is_holiday(end_date, calendar) or end_date.weekday() >= 5):
            dates.append(end_date)
        end_date += pd.Timedelta(days=1)
    return dates


def contract_to_nearby(_date, contract, preroll = 0, calendar='UK_ENG',symbol = 'C5TC', contract_type = 'monthly'):
    """
    Convert a contract code to the nearest future contract code.
    """
    new_date = pd.to_datetime( apply_date_rule(_date, f'{preroll}B',calendar))
    if contract_type == 'monthly':
        ltd = pd.to_datetime(last_trading_day(date_to_month(new_date), symbol))
    elif contract_type == 'quarterly':
        ltd = pd.to_datetime(last_trading_day(date_to_month(last_day_of_quarter(new_date),contract_type), symbol))
    elif contract_type == 'yearly':
        ltd = pd.to_datetime(last_trading_day(date_to_month(last_day_of_year(new_date + DateOffset(years=1)),contract_type), symbol))
    else:
        raise ValueError(f"Unsupported contract type: {contract_type}")
    month = MONTH_TO_NUM[contract[-3]]
    year = 2000+ int(contract[-2:])

    this_month = new_date.month
    this_year = new_date.year
    k = month - this_month + 12*(year-this_year)
    if new_date>=ltd:
        k-=CONTRACT_FACTOR[contract_type]
    if contract_type == 'quarterly':
        k = k//3
    elif contract_type == 'yearly':
        k = k//12 - 1
    return k

def nth_nearby(_date, nth, preroll = 7, cmd = 'C5TC', contract_type = 'monthly'):
    """
    Calculate the nth nearby future contract code.

    :param date: datetime 
    :param nth: int
    :param preroll: int (default 7)
    :param cmd: str (default 'C5TC')
    :return: str, the nth nearby future contract code
    """

    new_date = pd.to_datetime( apply_date_rule(_date, f'{preroll}B','UK_ENG'))
    # print(new_date,cmd)
    if contract_type == 'monthly':
        contract_date = new_date
    elif contract_type == 'quarterly':
        contract_date = last_day_of_quarter(new_date)
    else:
        contract_date = last_day_of_year(new_date + DateOffset(years=1)) 

    ltd = pd.to_datetime(last_trading_day(date_to_month(contract_date,contract_type), cmd))
    month = contract_date.month + nth*CONTRACT_FACTOR[contract_type] - 1
    year = contract_date.year
    if new_date>=ltd: 
        month+= CONTRACT_FACTOR[contract_type]
    year = year + month//12
    month = month % 12
    code = f'{NUM_TO_MONTH[month+1]}{str(year)[-2:]}'
    # print(date, ltd, new_date, contract_date, code)
    if contract_type == 'monthly':
        return code
    elif contract_type == 'quarterly':
        return 'Q' + code
    else:
        return 'Y' + code
    
def last_trading_day(contract, symbol):
    # print(contract, symbol)
    if symbol in ['C5TC','P4TC','S10TC']:
        if contract[0] == "Z":
            return previous_business_day(next_business_day(f"20{contract[-2:]}-12-24", "UK_ENG"),'UK_ENG')
        elif contract[:2] == 'YZ':
            year = 2000+ int(contract[-2:])-1
            return previous_business_day(next_business_day(f"{year}-12-24", "UK_ENG"),'UK_ENG')
        else:
            year = 2000+ int(contract[-2:])
            month = MONTH_TO_NUM[contract[-3]]
            date = pd.Timestamp(year = year, month = month, day = 1)
            if len(contract) == 4:
                date -= pd.DateOffset(months = 2)
            return last_business_day(date.strftime('%Y-%m-%d'),'UK_ENG')
    else:
        raise ValueError(f"Unsupported symbol: {symbol}")
 
def date_to_month(_date, contract_type = 'monthly'):
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
    if contract_type == 'monthly':
        return code
    elif contract_type == 'quarterly':
        return 'Q'+code
    elif contract_type == 'yearly':
        return 'Y'+code
    else:
        raise ValueError(f"Unsupported contract type: {contract_type}")