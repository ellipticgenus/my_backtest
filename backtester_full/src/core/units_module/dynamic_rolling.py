import pandas as pd
import numpy as np
from datetime import date
from my_holiday.holiday_utils import *
from commodity.commodconfig import COMMODINFO
from commodity.commodity import commodity_helper
from backtester_full.src.core.portfolio import Trade
from backtester_full.src.core.units_module.base_module import BaseUnitModule
import re


class DynamicRolling(BaseUnitModule):
    """
    A simple rolling window class that maintains a fixed-size list of values.
    It allows adding new values and retrieving the current window of values.
    """

    def __init__(self, params: dict):
        super().__init__()  
        self.params = params
        self.symbol = params['symbol']
        self.schedule_str = params['roll_schedule']
        self.longshort = params.get('longshort', 1) # 1 or -1
        self.roll_type = params.get('roll_type', 'monthly')
        # roll in parameters
        self.in_anchor_type = params.get('in_anchor_type', 'lbd')
        self.in_period = params.get('in_period', 3)
        self.in_date = params.get('in_date', '-10b')
        # roll out params
        self.out_anchor_type = params.get('out_anchor_type', 'ltd')
        self.out_period = params.get('out_period', 1)
        self.out_date_1 = params.get('out_date_1', '1m')
        self.out_date_2 = params.get('out_date_2', '-1B')

       
        self.initial_date = params .get('initial_date', '2017-01-04')
        self.end_date = params['end_date']
        self.generate_roll_schedule()
        self.generate_instruments_on_dates()

    @property
    def holiday_calendar(self):
        """
        Get the holiday calendar for the commodity.
        :return: str, holiday calendar code
        """
        cal = COMMODINFO[self.symbol]['holiday']
        return cal
    
    
    def generate_roll_schedule(self):
        """
        return roll schedule as a list
        """
        if self.roll_type == 'monthly':
            self.roll_schedule = re.findall(r'[A-Z]\*?', self.schedule_str)
        elif self.roll_type == 'quarterly':
            self.roll_schedule = re.findall(r'Q[A-Z]\*?', self.schedule_str)
        elif self.roll_type == 'yearly':
            self.roll_schedule = re.findall(r'Y[Z]\*?', self.schedule_str)
        
    
    def risk_dates(self, start_date, end_date):
        return business_days_between(start_date, end_date, self.holiday_calendar)
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Check if a trade can be executed on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: [] list of trades to be executed on the date
        """
        cal = self.holiday_calendar
        trades=[]
        if date.strftime('%Y-%m-%d') == self.rollin_date(date):
            rollin = self.rollin_contract(date)
            rollout = self.rollout_contract(date)
            if rollin == rollout:
                return trades
            # If the date is the start of the rolling window, return the trades to be executed
            roll_start = self.rollin_date(date)
            roll_end = self.rollout_date(date)
            in_dates = business_days_from(roll_start, self.in_period, cal)
            last_in_dates = business_days_until(last_business_day(roll_start, cal), self.in_period, cal)
            # print(date, roll_start, in_dates)
            out_dates = business_days_until(roll_end, self.out_period, cal)
            if self.params.get('roll_constant_size',False):
                quantity = self.params['roll_size']*self.longshort
            else:
                quantity = np.sign(portfolio.portfolio_state.positions.get(rollout, self.params['roll_size']*self.longshort))*self.params['roll_size']
            if self.out_period:
                daily_qty_out = quantity/ self.out_period
            daily_qty_in = quantity/ self.in_period
            for roll_date in range(len(in_dates)):     
                # Only add business days that are not holidays
                in_trade = Trade(rollin, 
                              in_dates[roll_date], 
                              daily_qty_in, 
                              COMMODINFO[self.params['symbol']]['currency'],
                              'dynamic_rolling',
                              symbol = self.params['symbol'],
                              extra_info={'last_rollin_date':last_in_dates[roll_date]})
                trades.append(in_trade)
            for roll_date in out_dates:
                out_trade = Trade(rollin, 
                               roll_date, 
                               -daily_qty_out, 
                               COMMODINFO[self.params['symbol']]['currency'],
                               'dynamic_rolling',
                               symbol = self.params['symbol'])
                trades.append(out_trade)
            # Return the trades to be executed on the date  
            return trades
        else:
            return []


    def rollin_contract(self, date):
        """
        Get the contract to be rolled in on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :return: str, contract to be rolled in
        """
        date = pd.to_datetime(date)
        month = date.month
        year =  date.year 
        month_code = self.roll_schedule[month-1]
        # print(self.roll_schedule)
        if month_code[-1] == '*':
            month_code = month_code[:-1]
            year += 1 
        if self.roll_type == 'yearly':
            year += 1
        return f"{self.params['symbol']}{month_code}{str(year)[-2:]}"
        
    def rollout_contract(self, date):
        """
        Get the contract to be rolled out on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :return: str, contract to be rolled out
        """

        day_in_previous_month = apply_date_rules(date, '-1M')
        return self.rollin_contract(day_in_previous_month)

    def roll_mapping(self):
        dates = self.risk_dates(self.initial_date, self.end_date)
        res = {}
        for date in dates:
            temp = {}
            temp['in_contract'] = self.rollin_contract(date)
            temp['rollout_date'] = self.rollout_date(date)
            temp['rollin_size'] = self.rollin_sizes()[date]
            in_date = self.rollin_date(date)
            if not in_date in res:
                res[in_date] = temp
        return res

    def anchor_date(self, date, anchor_type = 'lbd'):
        """
        Get the anchor date for the given date based on the anchor_date parameter.
        If anchor_date is 'lbd', the anchor date is the last business day of the month.
        If anchor_date is 'ltd', the anchor date is the last trading day of the month if the roll schedule is 'GHJKMNQUVXZF*', otherwise it is the last business day.
        :param date: str, date in 'YYYY-MM-DD' format
        :return: str, anchor date for the given date
        """
        if anchor_type == 'lbd':
            return last_business_day(date)
        elif anchor_type == 'ltd':
            if self.params['roll_type'] == 'monthly':
                return last_trading_day(date_to_month(date), self.params['symbol'])
            #something seems not working
            elif self.params['roll_type'] == 'quarterly':
                return last_trading_day(self.rollin_contract(date), self.params['symbol'])
        
            elif self.params['roll_type'] == 'yearly':
                date = pd.to_datetime(date)
                return previous_business_day(next_business_day(f'{date.year}-12-24', 'UK_ENG'), 'UK_ENG')
            else:
                return last_business_day(date)
        else:
            raise ValueError(f"Invalid anchor_date type: {anchor_type}")
        
        
    def rollin_date(self, date):
        """
        Set the start date for the rolling window.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        cal = self.holiday_calendar
        anchor_date = self.anchor_date(date,self.in_anchor_type)
        start_date = apply_date_rules(anchor_date, self.in_date, cal)
        return start_date
    
    def rollout_date(self, date):
        """
        Set the end date for the rolling window.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        cal = self.holiday_calendar
        date = apply_date_rules(date, self.out_date_1, cal)
        anchor_date = self.anchor_date(date,self.out_anchor_type)
        
        end_date = apply_date_rules(anchor_date,self.out_date_2, cal)
        return end_date
    

    
    def generate_instruments_on_dates(self):
        iods = {}
        end_date = pd.to_datetime(last_business_day(self.end_date, self.holiday_calendar))
        date = pd.to_datetime(self.initial_date)
        while date <= end_date:
            in_date = self.rollin_date(date)
            out_date = self.rollout_date(date)
            in_contract = self.rollin_contract(date)
            
            for hold_date in business_days_between(in_date, out_date,self.holiday_calendar):
                if hold_date not in iods:
                    iods[hold_date] = []
                
                if in_contract not in iods.get(hold_date, []):
                    iods[hold_date].append(in_contract)
            date = pd.to_datetime(apply_date_rule(date,'1m', self.holiday_calendar)) 
      
        self.instruments_on_dates = iods
    
    def instrument_on_date(self, date_str):
        """
        Calculate the risk on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: float, risk value
        """
        # Placeholder for risk calculation logic
        tickers = self.instruments_on_dates.get(date_str,[])
        res = []
        for ticker in tickers:
            res.append({'ticker':ticker,'type':'Future'})
        return res
        

class AutoRolling(BaseUnitModule):
    """
    The main function for this class is:
    1. generate instruments_on_dates which contains
        a. front 3 monthly
        b. front 2 quarterley
        c. front 1 yearly
    2. loop through trades and positions, to check if trades/positions should be rolled
    """
    def __init__(self,params):
        super().__init__()  
        self.params = params
        self.symbol = params['symbol']
        #should contains 
        # symbol|contract type|anchor date|schedule string|relative date|nearbys
        self.schedule_table = params['schedule_table']

        # roll out params
        self.out_anchor_type = params.get('out_anchor_type', 'ltd')
        self.out_period = params.get('out_period', 1)
        self.out_date_1 = params.get('out_date_1', '1m')
        self.out_date_2 = params.get('out_date_2', '-1B')

       
        self.initial_date = params .get('initial_date', '2017-01-04')
        self.end_date = params['end_date']

    @property
    def holiday_calendar(self):
        """
        Get the holiday calendar for the commodity.
        :return: str, holiday calendar code
        """
        cal = COMMODINFO[self.symbol]['holiday']
        return cal


    def trades_on_date(self, date, portfolio, risks_on_dates):
        positions = portfolio.portfolio_state.positions
        trades = portfolio.portfolio_state.trades_for_date
        for ticker, size in positions.items():
            if ticker != 'USD':
                pass
            #1. roll out the current contract
            #2. roll in the next contract
            #3. change trade in trade also to next contract.
        pass

    def generate_trade(self, positions, date):
        pass

    def instrument_on_date(self, date_str):
        """
        Calculate the risk on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: float, risk value
        """
        instruments = [] 
        for row in self.schedule_table:
            cmd = commodity_helper.get_commodity(row['symbol'])
            month_codes = cmd.get_last_trading_days()
            valid_months = [k for k,v in month_codes.items() if pd.to_datetime(v)>=pd.to_datetime(date_str)][:row['nearbys']+2]
            for valid_month in valid_months:
                instruments.append({'ticker':row['symbol']+valid_month,'type':'Future'})
        return instruments
            
        

    def risk_dates(self, start_date, end_date):
        return business_days_between(start_date, end_date, self.holiday_calendar)
    
    def generate_roll_schedule(self, _roll_type, _schedule_str):
        """
        return roll schedule as a list
        """
        if _roll_type == 'monthly':
            roll_schedule = re.findall(r'[A-Z]\*?', _schedule_str)
        elif _roll_type == 'quarterly':
            roll_schedule = re.findall(r'Q[A-Z]\*?', _schedule_str)
        elif _roll_type == 'yearly':
            roll_schedule = re.findall(r'Y[Z]\*?', _schedule_str)
        else:
            raise ValueError(f"Invalid roll_type: {_roll_type}")
        return roll_schedule
    
