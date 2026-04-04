from backtester.src.core.units_module.base_module import BaseUnitModule
import pandas as pd
import numpy as np
from my_holidays.holiday_utils import *
from collections import defaultdict
from backtester.src.core.portfolio import Trade
from backtester.src.core.utils.utils import partition_ticker


def round_to_nearest(number,step=5):
    """Round a number to the nearest multiple of 5 (works for positive and negative numbers)."""
    return step * round(number / step)

class VolTarget(BaseUnitModule):
    def __init__(self, params = {}):
        super().__init__()
        self.params = params
        self.roll_info = params['roll_info']
        self.roll_schedule = params.get('roll_schedule',7)
        self.future_instruments = params.get('future_instruments', [])
        # not used when we are including strategy, we need to have a look through.
        self.strategy_instruments = params.get('strategy_instruments',[]) 
        # used for vol control
        self.vol_period = params.get('vol_period', 20)
        self.vol_target = params.get('vol_target', 500000)
        self.vol_upper_limit = params.get('vol_upper_limit', 1.5)*self.vol_target # with the old dynamic we have upper limit 1.5 and lower limit 0.5
        self.vol_lower_limit = params.get('vol_lower_limit', 0.5)*self.vol_target
        self.decay_factor  = params.get('decay_factor', 1.0)
        self.contract_type = self.params.get('contract_type','monthly')
        self.lookback_periods = params.get('lookback_periods', 262)
    
    @property
    def holiday_calendar(self):
        return self.params['holiday_calendar']

    @property
    def prefix(self):
        return {'quarterly': 'Q', 'yearly': 'Y', 'monthly': ''}[self.contract_type]

    @property
    def round_step(self):
        return {'quarterly': 15, 'yearly': 60, 'monthly': 5}[self.contract_type]

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
        
        vol, total_value = self.vol_on_date(date, holdings_on_date, risks_on_dates,self.roll_schedule)
        # print(date, vol, total_value)
        # Calculate VaR and adjust trades if needed
        ratio = 1
        vol_2 = vol*total_value
        if (not self.vol_lower_limit < vol_2 < self.vol_upper_limit) and total_value != 0:
            ratio =  self.vol_target/vol_2
        # print(volratio)
        trades_on_date_new = [
            Trade(
                ticker,
                date,
                round_to_nearest(max(min(size * ratio,200),-200) - portfolio.portfolio_state.positions.get(ticker, 0),self.round_step),
                denominate='USD',
                trade_type='neutral',
                symbol = partition_ticker(ticker)[0]
            )
            for ticker, size in holdings_on_date.items() if ticker != 'USD'
        ]
        # print([t.size for t in trades_on_date_new])
        portfolio.portfolio_state.reset_trades(trades_on_date_new)

        return []


    def vol_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):

        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if tickers is None:
            return self.vol_target
        prev_date = previous_business_day(date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        buz_dates = business_days_until(prev_date, self.vol_period + 15, self.holiday_calendar)
        returns = {}
        # print(date, tickers)
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] for  ticker in tickers]
        total_value = sum(portfolio_values)
        weights = np.array(portfolio_values)/total_value
        for ticker in tickers:
            # print(date, ticker)
            nearby = contract_to_nearby(date, ticker[-3:], roll_schedule, contract_type = self.contract_type )
            if self.contract_type != 'monthly':
                symbol = ticker[:-4]
            else:
                symbol = ticker[:-3]
            # print(nearby,roll_schedule)
            # print(risks_on_dates[buz_dates[0]][f'{symbol}{self.prefix}_{nearby}_{roll_schedule}'])
            ret = [ risks_on_dates[buz_date][f'{symbol}{self.prefix}_{nearby}_{roll_schedule}']['log_return'] for buz_date in buz_dates]
            # print(ret)
            returns[ticker] = ret

        res = pd.DataFrame(returns)
        decay_factor = self.decay_factor
        decay_factors = decay_factor ** np.arange(self.vol_period)[::-1]
        decay_factors /= np.sum(decay_factors)

        corr = res.rolling(self.vol_period).corr()
        # need to adjust the correlation. 
        vol = res.rolling(self.vol_period).apply(lambda x: np.sqrt(np.sum(decay_factors*x**2))) * np.sqrt(252)
        portfolio_var = []
        for i in range(self.vol_period, len(res)):
            cov_matrix = corr.iloc[i].values * np.outer(vol.iloc[i].values, vol.iloc[i].values)
            portfolio_var.append(weights.T @ cov_matrix @ weights)
        if portfolio_var:
            return np.sqrt(portfolio_var[-1]), abs(total_value)
        else:
            # print(date)
            return self.vol_target, abs(total_value)     


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
        dates = pd.date_range(end = start_date, periods= self.lookback_periods , freq='B')
        # print(dates)
        dates = [date for date in dates if not is_holiday(date, self.holiday_calendar)]
        business_days = business_days_between(start_date, end_date, self.holiday_calendar)
        risk_dates = sorted(set(dates+business_days))  
        return risk_dates