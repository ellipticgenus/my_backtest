from backtester.src.core.units_module.base_module import BaseUnitModule
import pandas as pd
import numpy as np
from my_holidays.holiday_utils import *
from collections import defaultdict
from backtester.src.core.portfolio import Trade
from backtester.src.core.utils.utils import partition_ticker

#need to consider long var and short var or do symmetric. 
def round_to_nearest_5(number):
    """Round a number to the nearest multiple of 5 (works for positive and negative numbers)."""
    return 5 * round(number / 5)

class VarAdjustment(BaseUnitModule):
    """
    Adjust the quantity of a strategy based on the var of the historical holoding.
    """
    def __init__(self, params = {}):
        super().__init__()
        self.params = params
        self.varlimit = params.get('varlimit', 100000)
        self.var_lower_limit = self.varlimit*params.get('lower_limit',0.75)
        self.var_upper_limit = self.varlimit*params.get('upper_limit',1.25)
        self.roll_info = params['roll_info']
        self.roll_schedule = params.get('roll_schedule',7)
        self.future_instruments = params.get('future_instruments', [])
        # not used when we are including strategy, we need to have a look through.
        self.strategy_instruments = params.get('strategy_instruments',[]) 
        # used for vol control
        self.vol_control = params.get('vol_control', False)
        self.vol_period = params.get('vol_period', 40)
        self.vol_target = params.get('vol_target', 25)
        self.vol_upper_limit = params.get('vol_upper_limit', 1.1)*self.vol_target
        self.vol_lower_limit = params.get('vol_lower_limit', 0)*self.vol_target
        self.decay_factor  = params.get('decay_factor', 1)
        self.contract_type = self.params.get('contract_type','monthly')
    @property
    def holiday_calendar(self):
        return self.params['holiday_calendar']

    @property
    def prefix(self):
        prefix = ''
        if self.contract_type == 'quarterly':
            prefix = 'Q'
        elif self.contract_type == 'yearly':
            prefix = 'Y'
        else:
            prefix = ''
        return prefix

    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Adjust the quantity of a strategy based on the var of the historical holding.
        
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :param risks_on_dates: dict, mapping of date to risk values
        :return: [] list of trades to be executed on the date
        """
        trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        holdings_on_date = portfolio.portfolio_state.positions.copy()

        # Update holdings based on trades
        for trade in trades_on_date:
            if trade.type == 'neutral':
                holdings_on_date[trade.ticker] = holdings_on_date.get(trade.ticker, 0) + trade.size

        # Check if strategy instruments are implemented
        if self.strategy_instruments:
            raise NotImplementedError('need to finish this') 
        
        vol_ratio = 1
        if self.vol_control:       
            vol = self.vol_on_date(date, holdings_on_date, risks_on_dates)
            # print(date,vol)
            if  vol > self.vol_upper_limit:
                vol_ratio = self.vol_target / vol

        # Calculate VaR and adjust trades if needed
        var_after = self.var_on_date(date, holdings_on_date, risks_on_dates, self.roll_schedule)
        # print(date, 'var',var_after)
        if not (self.var_lower_limit <= var_after <= self.var_upper_limit):
            ratio = vol_ratio * self.varlimit / var_after
            trades_on_date_new = [
                Trade(
                    ticker,
                    date,
                    round_to_nearest_5(size * ratio - portfolio.portfolio_state.positions.get(ticker, 0)),
                    denominate='USD',
                    trade_type='neutral',
                    symbol = partition_ticker(ticker)[0]
                )
                for ticker, size in holdings_on_date.items() if ticker != 'USD'
            ]
            portfolio.portfolio_state.reset_trades(trades_on_date_new)

        return []

    def var_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        """
        Calculate the Value at Risk (VaR) for a given date based on the holdings and risk data.

        :param date: str, the date for which the VaR is calculated, in 'YYYY-MM-DD' format.
        :param holdings_on_date: dict, mapping of tickers to their corresponding holdings on the given date.
        :param risks_on_dates: dict, mapping of dates to their corresponding risk values, including 'close' prices and returns.
        :param roll_schedule: int, the rolling schedule to determine the nearby contract, default is 7.
        :return: float, the calculated VaR, which is the absolute value of the 5th percentile of the portfolio returns distribution.
        """
        # print(date, holdings_on_date)
        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if tickers is None:
            return self.varlimit
        prev_date = previous_business_day(date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        buz_dates = business_days_until(prev_date, 252, self.holiday_calendar)
        returns = {}
        for ticker in tickers:
            # print(date, ticker)
            nearby = contract_to_nearby(date, ticker[-3:], roll_schedule, contract_type = self.contract_type )
            if self.contract_type != 'monthly':
                symbol = ticker[:-4]
            else:
                symbol = ticker[:-3]
            level = risks_on_dates[prev_date][ticker]['close']
            ret = [holdings_on_date[ticker] * level*risks_on_dates[buz_date][f'{symbol}{self.prefix}_{nearby}_{roll_schedule}']['return'] for buz_date in buz_dates]
            returns[ticker] = ret

        res = pd.DataFrame(returns)
        res['sum'] = res[tickers].sum(axis = 1)
        var = res['sum'].quantile(0.05 )
        return abs(var)

    def vol_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):

        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if tickers is None:
            return self.vol_target
        prev_date = previous_business_day(date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        buz_dates = business_days_until(prev_date, self.vol_period + 15, self.holiday_calendar)
        returns = {}
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] for  ticker in tickers]
        weights = np.array(portfolio_values)/sum(portfolio_values)
        for ticker in tickers:
            # print(date, ticker)
            nearby = contract_to_nearby(date, ticker[-3:], roll_schedule, contract_type = self.contract_type )
            if self.contract_type != 'monthly':
                symbol = ticker[:-4]
            else:
                symbol = ticker[:-3]
            ret = [ risks_on_dates[buz_date][f'{symbol}{self.prefix}_{nearby}_{roll_schedule}']['log_return'] for buz_date in buz_dates]
            returns[ticker] = ret

        res = pd.DataFrame(returns)
        decay_factor = self.decay_factor
        decay_factors = decay_factor ** np.arange(self.vol_period)[::-1]
        decay_factors /= np.sum(decay_factors)

        corr = res.rolling(self.vol_period).corr()
        # need to adjust the correlation. 
        vol = res.rolling(self.vol_period).apply(lambda x: np.sqrt(np.sum(decay_factors*x**2))) * np.sqrt(252)*100
        portfolio_var = []
        for i in range(self.vol_period, len(res)):
            cov_matrix = corr.iloc[i].values * np.outer(vol.iloc[i].values, vol.iloc[i].values)
            portfolio_var.append(weights.T @ cov_matrix @ weights)
        if portfolio_var:
            return np.sqrt(portfolio_var[-1])
        else:
            return self.vol_target     


    def instrument_on_date(self, date):
        instruments = []
        for nearby_future in self.future_instruments:
            for n_nearby, roll_schedule in self.roll_info:
                ticker = f'{nearby_future}{self.prefix}_{n_nearby}_{roll_schedule}'
                instruments.append({'ticker': ticker, 'type': 'NearbyFuture'})
        if date >= pd.to_datetime('2017-01-04'):
            instruments.extend([{'ticker': s, 'type': 'Strategy'} for s in self.strategy_instruments])
        return instruments
    
    def risk_dates(self, start_date, end_date):
        dates = pd.date_range(end = start_date, periods= 262 , freq='B')
        dates = [date for date in dates if not is_holiday(date, self.holiday_calendar)]
        business_days = business_days_between(start_date, end_date, self.holiday_calendar)
        risk_dates = sorted(set(dates+business_days))  
        return risk_dates