import pandas as pd
from commodity.commodconfig import *
from datetime import date
from pandas.tseries.offsets import DateOffset
from my_holiday.holiday_utils import apply_date_rule,apply_date_rules

class Commodity:
    def __init__(self,ticker):
        self.ticker = ticker
        self.commod_info = COMMODINFO[ticker]
        self.data = {}
        
    @property
    def crush_decomposition(self):
        return self.commod_info.get('crush_decomposition',{})
    
    @property
    def holiday(self):
        return self.commod_info['holiday']

    @property
    def currency(self):
        return self.commod_info['currency']

    @property
    def valid_expiration(self):
        return self.commod_info['valid_expiration']
    
    @property
    def liquid_contracts(self):
        return self.commod_info['liquid_expiration']
 
    def _first_notice_rule(self):
        return self.commod_info['first_notice_rule']
    

    def _expiration_day_rule(self):
        return self.commod_info['expiration_rule']
        
    def get_commodity_series(self, start_date, end_date, cache_codes = False):
        _date = pd.to_datetime(apply_date_rule(start_date, '-1m', self.holiday) )
        start_year = _date.year
        new_end_date = pd.to_datetime(end_date) + DateOffset(years=1)
        end_year = new_end_date.year
        future_codes = []
        for year in range(start_year, end_year+1):
            for month_code in self.valid_expiration:
                future_codes.append(f'{month_code}{str(year)[-2:]}')
        if cache_codes:     
            self.data['future_codes'] = future_codes
        return future_codes 
    
    def expiration_day(self, contract):
        if self._expiration_day_rule() is not None:
            expiration_rules = self._expiration_day_rule()
            if contract[0] in self.valid_expiration:
                rdate = expiration_rules[contract[:-2]]
                if int(contract[-2:]) == 99: # if it is the last contract, we use the last day of the year
                    return apply_date_rules(f'{1900+ int(contract[-2:])}-{MONTH_TO_NUM[contract[-3]]}-01', rdate, self.holiday)
                else:
                    return apply_date_rules(f'{2000+ int(contract[-2:])}-{MONTH_TO_NUM[contract[-3]]}-01', rdate, self.holiday)
        else:
            return self.last_trading_day(contract)
    
    def expiration_days(self, start_date, end_date, cache_dates = False):
        if 'expiration_days' in self.data:
            return self.data['expiration_days']
        if cache_dates:
            start_date ='2000-01-01'
        futures_codes = self.get_commodity_series( start_date, end_date, cache_dates)
        expiration_days = [self.expiration_day( code) for code in futures_codes]
        if cache_dates:
            self.data['expiration_days'] = expiration_days
            self.data['ed_mapping'] =  self.data.get('ed_mapping',{})|dict(zip(futures_codes, expiration_days))
        return expiration_days
    
    def get_expiration_days(self):
        if 'ed_mapping' in self.data:
            mapping = {k:v for k,v in self.data['ed_mapping'].items()}
            if mapping:
                return mapping
        start_date ='2000-01-01'
        end_date = date.today().strftime('%Y-%m-%d')
        self.expiration_days(start_date, end_date, True)
        return {k:v for k,v in self.data['ed_mapping'].items()} 

    def last_trading_day(self, contract):
        expiration_rules = self._first_notice_rule() # we default use first notice day
        if contract[0] in self.valid_expiration:
            rdate = expiration_rules[contract[:-2]]
            if int(contract[-2:]) == 99: # if it is the last contract, we use the last day of the year
                return apply_date_rules(f'{1900+ int(contract[-2:])}-{MONTH_TO_NUM[contract[-3]]}-01', rdate, self.holiday)
            else:
                return apply_date_rules(f'{2000+ int(contract[-2:])}-{MONTH_TO_NUM[contract[-3]]}-01', rdate, self.holiday)
        else:
            raise ValueError(f"Unsupported contract: {contract}")

    def last_trading_days(self, start_date, end_date, cache_dates = False):
        if 'last_trading_days' in self.data:
            return self.data['last_trading_days']
        if cache_dates:
            start_date ='2000-01-01'
        futures_codes = self.get_commodity_series( start_date, end_date, cache_dates)
        last_trading_days = [self.last_trading_day( code) for code in futures_codes]
        if cache_dates:
            self.data['last_trading_days'] = last_trading_days
            self.data['ltd_mapping'] =  self.data.get('ltd_mapping',{})|dict(zip(futures_codes, last_trading_days))
        return last_trading_days
    
    def get_last_trading_days(self):
        if 'ltd_mapping' in self.data:
            mapping = {k:v for k,v in self.data['ltd_mapping'].items()}
            if mapping:
                return mapping
        start_date ='2000-01-01'
        end_date = date.today().strftime('%Y-%m-%d')
        self.last_trading_days(start_date, end_date, True)
        return {k:v for k,v in self.data['ltd_mapping'].items()} 
            

    def days_to_expiration(self, contract, date):
        month_code = contract[-3:]
        if 'ed_mapping' not in self.data or month_code not in self.data['ed_mapping']:
            end_date = f'20{contract[-2:]}-{MONTH_TO_NUM[contract[0]]}-01'
            self.expiration_days('2000-01-01', end_date, True)
        expiration_day = pd.to_datetime(self.data['ed_mapping'][month_code])
        return (expiration_day - pd.to_datetime(date)).days

    def days_to_last_trading_day(self, contract, date):
        month_code = contract[-3:]
        if 'ltd_mapping' not in self.data or month_code not in self.data['ltd_mapping']:
            end_date = f'20{contract[-2:]}-{MONTH_TO_NUM[contract[0]]}-01'
            self.last_trading_days('2000-01-01', end_date, True)
        last_trading_day = pd.to_datetime(self.data['ltd_mapping'][month_code])
        return (last_trading_day - pd.to_datetime(date)).days

class CommodityHelper:
    def __init__(self):
        self.commodities = {}

    def get_commodity(self, ticker):
        if ticker not in self.commodities:
            self.commodities[ticker] = Commodity(ticker)
        return self.commodities[ticker]
    
commodity_helper = CommodityHelper()