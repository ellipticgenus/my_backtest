import pandas as pd
from my_holiday.holiday_utils import *
from commodity.commodconfig import COMMODINFO
from backtester_full.src.core.portfolio import Trade
from backtester_full.src.core.units_module.base_module import BaseUnitModule
import re


class SimpleRolling(BaseUnitModule):
    """
    A simple rolling window class that maintains a fixed-size list of values.
    It allows adding new values and retrieving the current window of values.
    """

    def __init__(self, params: dict):
        super().__init__()
        self.params = params
        self.longshort = params.get('longshort', 1)
        self.anchor_date_type = params.get('anchor_date_type', 'lbd') #either last business day or last trading day
        self.roll_type = params.get('roll_type', 'monthly')
        self.init_date = params.get('initial_date', '2017-01-04')


    @property
    def holiday_calendar(self):
        """
        Get the holiday calendar for the commodity.
        :return: str, holiday calendar code
        """
        symbol = self.params['symbol']
        cal = COMMODINFO[symbol]['holiday']
        return cal
    
    @property
    def roll_schedule(self):
        """
        return roll schedule as a list
        """
        schedule_str = self.params['roll_schedule']
        if self.roll_type == 'monthly':
            elements = re.findall(r'[A-Z]\*?', schedule_str)
        elif self.roll_type == 'quarterly':
            elements = re.findall(r'Q[A-Z]\*?', schedule_str)
        elif self.roll_type == 'yearly':
            elements = re.findall(r'Y[Z]\*?', schedule_str)
        return elements
    
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
        if date.strftime('%Y-%m-%d') == self.roll_start_date(date):
            # If the date is the start of the rolling window, return the trades to be executed
            roll_start = self.roll_start_date(date)
            roll_end = self.roll_end_date(date)
            roll_dates = self.get_business_dates(roll_start, roll_end)
            rollin = self.rollin_contract(date)
            rollout = self.rollout_contract(date)
            if rollin == rollout:
                return trades
            if self.params.get('roll_constant_size',False):
                quantity = self.params['roll_size']
            else:
                prev_day = pd.to_datetime(previous_business_day(roll_start, cal))
                quantity = portfolio.portfolio_state.balance/risks_on_dates[prev_day][rollin]['close']
            position = portfolio.portfolio_state.positions.get(rollout, 0)
            daily_qty_out = position/ len(roll_dates) if position != 0 else 0
            daily_qty_in = quantity/ len(roll_dates)
            for roll_date in roll_dates:     
                # Only add business days that are not holidays
                in_trade = Trade(rollin, 
                              roll_date, 
                              self.longshort*daily_qty_in, 
                              COMMODINFO[self.params['symbol']]['currency'],
                              'simple_rolling',
                              symbol = self.params['symbol'])
                out_trade = Trade(rollout, 
                               roll_date, 
                               -daily_qty_out, 
                               COMMODINFO[self.params['symbol']]['currency'],
                               'simple_rolling',
                               symbol = self.params['symbol'])
                trades.append(in_trade)
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

        day_in_previous_month = apply_date_rule(date, '-1M')
        return self.rollin_contract(day_in_previous_month)

    def anchor_date(self, date):
        """
        Get the anchor date for the given date based on the anchor_date parameter.
        If anchor_date is 'lbd', the anchor date is the last business day of the month.
        If anchor_date is 'ltd', the anchor date is the last trading day of the month if the roll schedule is 'GHJKMNQUVXZF*', otherwise it is the last business day.
        :param date: str, date in 'YYYY-MM-DD' format
        :return: str, anchor date for the given date
        """
        if self.anchor_date_type == 'lbd':
            return last_business_day(date)
        elif self.anchor_date_type == 'ltd':
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
            raise ValueError(f"Invalid anchor_date type: {self.anchor_date_type}")
        
        
    def roll_start_date(self, date):
        """
        Set the start date for the rolling window.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        cal = self.holiday_calendar
        anchor_date = self.anchor_date(date)
        start_date = apply_date_rule(anchor_date, self.params['roll_start'], cal)
        return start_date
    
    def roll_end_date(self, date):
        """
        Set the end date for the rolling window.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        cal = self.holiday_calendar
        anchor_date = self.anchor_date(date)
        
        end_date = apply_date_rule(anchor_date,self.params['roll_end'], cal)
        return end_date
    
    def get_business_dates(self, start_date, end_date):
        """
        Get the business dates between the start and end dates.
        :param start_date: str, start date in 'YYYY-MM-DD' format
        :param end_date: str, end date in 'YYYY-MM-DD' format
        :return: list of business dates
        """
        return [date for date in pd.date_range(start=start_date, end=end_date, freq='B') if not is_holiday(date, self.holiday_calendar)]
    
    def instrument_on_date(self, date_str):
        """
        Calculate the risk on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: float, risk value
        """
        # Placeholder for risk calculation logic
        prev_date = apply_date_rule(date_str,'-1M') #data in previous month
        date = pd.to_datetime(date_str)
        if date < pd.to_datetime(self.init_date):
            return[]
        prev_roll_end = pd.to_datetime(self.roll_end_date(prev_date))
        roll_start = pd.to_datetime(self.roll_start_date(date_str))
        
        roll_end = pd.to_datetime(self.roll_end_date(date_str))
        # print(date, roll_start, roll_end,prev_roll_end,prev_date)
        if date < roll_start:
            if date <= prev_roll_end:
                res = [
                        {'ticker':self.rollin_contract(prev_date),'type':'Future'}, 
                        {'ticker':self.rollout_contract(prev_date),'type':'Future'},
                ]
            else:
                res = [
                    {'ticker':self.rollout_contract(date_str),'type':'Future'},
                ]
        else:
            if date <= roll_end:
                res = [
                        {'ticker':self.rollin_contract(date_str),'type':'Future'}, 
                        {'ticker':self.rollout_contract(date_str),'type':'Future'},
                ]
            else:
                res = [
                    {'ticker':self.rollin_contract(date_str),'type':'Future'},
                ]
        # print(date, res)
        return res
    

class PreRoll(BaseUnitModule):
    """
    preroll module that short the 
    """

    def __init__(self, params: dict):
        super().__init__()
        self.params = params

    @property
    def holiday_calendar(self):
        """
        Get the holiday calendar for the commodity.
        :return: str, holiday calendar code
        """
        symbol = self.params['symbol']
        cal = COMMODINFO[symbol]['holiday']
        return cal

    @property
    def roll_schedule(self):
        """
        return roll schedule as a list
        """
        schedule_str = self.params['roll_schedule']
        elements = re.findall(r'[A-Z]\*?', schedule_str)
        return elements


    def risk_dates(self, start_date, end_date):
        """
        Get the business days between start_date and end_date based on the holiday calendar.

        :param start_date: str, start date in 'YYYY-MM-DD' format
        :param end_date: str, end date in 'YYYY-MM-DD' format
        :return: list of business days between the two dates
        """
        return business_days_between(start_date, end_date, self.holiday_calendar)
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Check if a trade can be executed on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: [] list of trades to be executed on the date
        """
        cal = self.holiday_calendar
        trades = []
        if date.strftime('%Y-%m-%d') == self.roll_start_date(date):
            # If the date is the start of the rolling window, return the trades to be executed
            roll_start = self.roll_start_date(date)
            roll_end = self.roll_end_date(date)
            roll_days = self.params['roll_days']
            roll_dates_start = self.get_business_dates(roll_start, roll_days)
            roll_dates_end = self.get_business_dates(roll_end, roll_days)
            rollin = self.rollin_contract(date)
            rollout = self.rollout_contract(date)

            prev_day = pd.to_datetime(previous_business_day(roll_start, cal))
            qty_out = portfolio.portfolio_state.balance / risks_on_dates[prev_day][rollout]['close']
            if self.params['roll_method'] == 'weight':
                qty_in = portfolio.portfolio_state.balance / risks_on_dates[prev_day][rollin]['close']
            elif self.params['roll_method'] == 'size':
                qty_in = qty_out
            else:
                raise ValueError('Roll Methold Not Allowed')
            
            daily_qty_out = qty_out / roll_days 
            daily_qty_in = qty_in / roll_days
            for roll_date in roll_dates_start:
                # Only add business days that are not holidays
                in_trade = Trade(
                    rollin,
                    roll_date,
                    daily_qty_in,
                    COMMODINFO[self.params['symbol']]['currency'],
                    'simple_rolling'
                )
                out_trade = Trade(
                    rollout,
                    roll_date,
                    -daily_qty_out,
                    COMMODINFO[self.params['symbol']]['currency'],
                    'simple_rolling'
                )
                trades.append(in_trade)
                trades.append(out_trade)
            # Return the trades to be executed on the date
            for roll_date in roll_dates_end:
                 # Only add business days that are not holidays
                in_trade = Trade(
                    rollin,
                    roll_date,
                    -daily_qty_in,
                    COMMODINFO[self.params['symbol']]['currency'],
                    'simple_rolling'
                )
                out_trade = Trade(
                    rollout,
                    roll_date,
                    daily_qty_out,
                    COMMODINFO[self.params['symbol']]['currency'],
                    'simple_rolling'
                )
                trades.append(in_trade)
                trades.append(out_trade)
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
        year = date.year
        month_code = self.roll_schedule[month - 1]
        if month_code[-1] == '*':
            month_code = month_code[:-1]
            year += 1
        return f"{self.params['symbol']}{month_code}{str(year)[-2:]}"

    def rollout_contract(self, date):
        """
        Get the contract to be rolled out on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :return: str, contract to be rolled out
        """

        day_in_previous_month = apply_date_rule(date, '-1M')
        return self.rollin_contract(day_in_previous_month)

    def roll_start_date(self, date):
        """
        Set the start date for the rolling window.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        cal = self.holiday_calendar
        start_date = apply_date_rule(last_business_day(date), self.params['roll_start'], cal)
        return start_date

    def roll_end_date(self, date):
        """
        Set the end date for the rolling window.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        cal = self.holiday_calendar
        end_date = apply_date_rule(last_business_day(date), self.params['roll_end'], cal)
        return end_date

    def get_business_dates(self, start_date, length):
        """
        Get the roll dates between the start and fixed length.
        :param start_date: str, start date in 'YYYY-MM-DD' format
        :param length: str, end date in 'YYYY-MM-DD' format
        :return: list of business dates
        """
        # Add 10 to ensure we get all the business days if length too long this will not work
        date_range = pd.date_range(start=start_date, periods=length+ 10, freq='B')
        date_range = [date for date in date_range if not is_holiday(date, self.holiday_calendar)]
        return date_range[:length]

    def instrument_on_date(self, date_str):
        """
        Calculate the risk on the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :return: float, risk value
        """
        # Placeholder for risk calculation logic
        date = pd.to_datetime(date_str)
        if date < pd.to_datetime('2017-01-04'):
            return[]
        prev_date = apply_date_rule(date_str,'-1M')
        prev_roll_end = pd.to_datetime(self.roll_end_date(prev_date))
        
        roll_start = pd.to_datetime(self.roll_start_date(date_str))
        roll_end = pd.to_datetime(self.roll_end_date(date_str))
        roll_days = self.params['roll_days']
        roll_dates_end = self.get_business_dates(roll_end, roll_days)[-1]

        prev_roll_dates_end = self.get_business_dates(prev_roll_end, roll_days)[-1]

        rollin = self.rollin_contract(roll_start.strftime('%Y-%m-%d'))
        rollout = self.rollout_contract(roll_start.strftime('%Y-%m-%d'))
        prev_rollin = self.rollin_contract(prev_date)
        prev_rollout = self.rollout_contract(prev_date)

        if date >= roll_start and date <= roll_dates_end:
            res = [
                    {'ticker':rollin,'type':'Future'}, 
                    {'ticker':rollout,'type':'Future'},
                ]
        elif date <= prev_roll_dates_end:
                res = [
                    {'ticker':prev_rollin,'type':'Future'}, 
                    {'ticker':prev_rollout,'type':'Future'},
                ]
        else:
            res = []

        return res