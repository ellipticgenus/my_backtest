from .base_module import BaseUnitModule
import pandas as pd
import numpy as np
from my_holiday.holiday_utils import *
from collections import defaultdict
from backtester_full.src.core.portfolio import Trade
from backtester_full.src.core.utils.utils import partition_ticker
from commodity.commodity import commodity_helper
from ..utils.global_params_helper import GLOBALPARAMS

def final_positions_on_date(date, holdings, strategy_trades):
    final_holdings ={}
    for strategy, pos in holdings.items():
        if strategy in strategy_trades:
            trades = strategy_trades[strategy]
            for trade in trades:
                if trade['date'] == date:
                    pos[trade['ticker']] = pos.get(trade['ticker'], 0) + round(trade['size'],10)
        final_holdings[strategy] = pos.copy() 
    return final_holdings

def round_to_nearest(number, step=5):
    """Round a number to the nearest multiple of  (works for positive and negative numbers)."""
    return step * round(number / step)

class VolTargetStrategy(BaseUnitModule):
    def __init__(self, params = {}):
        super().__init__()
        self.params = params
        self.asset_table = params['asset_table']
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
        self.contract_type = params.get('contract_type','monthly')
        self.use_ticker_filter = params.get('use_ticker_filter', True)
        self.use_expiration_filter = params.get('use_expiration_filter', True) 
        self.ticker_filter = params.get('ticker_filter', ['USD'])
        self.lookback_dates = self.params.get('lookback_dates', 126)
        self.risk_allowance = params['risk_allowance']
        self.daily_limit = round_to_nearest(params.get('daily_limit', 40),self.round_step)
        self.max_units = params.get('max_units', 200)
        self.cap_total_units = params.get('cap_total_units', False)
        self.rebal_finished = True
        self.rebal_freq = params.get('rebal_freq', 1) 
        self.initial_date = params.get('initial_date')
        self.end_date = params.get('end_date')

    @property
    def holiday_calendar(self):
        return self.params['holiday_calendar']

    @property
    def prefix(self):
        return {'quarterly': 'Q', 'yearly': 'Y', 'monthly': ''}[self.contract_type]

    @property   
    def round_step(self):
        return {'quarterly': 15, 'yearly': 60, 'monthly': 5}[self.contract_type]


    def udl_holding_on_date(self, date, risks_on_dates):
        if GLOBALPARAMS['risk_mode'] and date.strftime('%Y-%m-%d') == GLOBALPARAMS['today']:
            strategy_positions = {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}
            strategy_trades = {x['strategy']: risks_on_dates[date][x['strategy']]['trades_for_date'] for x in self.asset_table}
            return final_positions_on_date(date, strategy_positions, strategy_trades)
        else:
            return {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}
        
    
    def apply_ticker_filter(self,positions, tickers):
        return {k:v for k,v in positions.items() if  not (k in tickers)}
    
    def apply_expiration_filter(self, date, positions, expiration_cufoff = 20):
        res = {}
        for k,v in positions.items():
            cmd = commodity_helper.get_commodity(k[:-2])
            if cmd.days_to_expiration(k ,date) >= expiration_cufoff:
                res[k] = v
        return res
        
    def is_udl_trade_day(self, date, risks_on_dates):
        trades = []
        for x in self.asset_table:
            trades += risks_on_dates[date][x['strategy']]['trades_for_date']
        trades = [trade for trade in trades if trade['date'] == date]
        return len(trades)>0

    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Adjust the quantity of a strategy based on the var of the historical holding.
        
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :param risks_on_dates: dict, mapping of date to risk values
        :return: [] list of trades to be executed on the date
        """
        udls_holding = self.udl_holding_on_date( date, risks_on_dates)

        holdings_on_date = portfolio.portfolio_state.positions.copy()
        vols_on_date = {}
        values_on_date = {}
        strategy_vol_on_date = {}
        filtered_pos = udls_holding
        for strategy, positions in udls_holding.items():
            if self.use_ticker_filter:
                positions = self.apply_ticker_filter(positions, self.ticker_filter)
                holdings_on_date = self.apply_ticker_filter(holdings_on_date, self.ticker_filter)
            if self.use_expiration_filter:
                positions = self.apply_expiration_filter(date, positions)
                holdings_on_date = self.apply_expiration_filter(date, holdings_on_date)            

            filtered_pos[strategy] = positions
            vols_on_date[strategy],values_on_date[strategy] = self.vol_on_date(date, positions, risks_on_dates, self.roll_schedule)
            strategy_vol_on_date[strategy] = vols_on_date[strategy]*values_on_date[strategy]
        # print(date, values_on_date,vols_on_date)
        # Calculate VaR and adjust trades if needed

        ratio = 1
        leverate_ratio = {}
        if self.is_udl_trade_day(date, risks_on_dates) or ( not self.rebal_finished ):
            for strategy, risk in strategy_vol_on_date.items():
        
                if risk != 0:
                    leverate_ratio[strategy] = self.vol_target*self.risk_allowance[strategy]/risk
                else:
                    leverate_ratio[strategy] = 0
            final_holdings = {}
            for strategy, leverage in leverate_ratio.items():
                for pos, size in filtered_pos[strategy].items():
                    # print(date, strategy, pos, size,leverage)
                    if pos not in final_holdings:
                        final_holdings[pos] = size*leverage
                    else:
                        final_holdings[pos] += size*leverage  
        else:
            if date not in self.rebal_dates():
                return []
            vol, total_value  = self.vol_on_date(date, holdings_on_date, risks_on_dates, self.roll_schedule, normalize = False)
            if (not self.vol_lower_limit < vol* total_value < self.vol_upper_limit) and total_value != 0 :
                ratio =  self.vol_target/ (vol* total_value)
            # here holdings on date is the position hold by the strategy. filtered by the filter
            final_holdings = holdings_on_date.copy()
            # print(date, vol, total_value, ratio)
        # print(date, final_holdings,holdings_on_date)
        scaler = 1
        if self.cap_total_units:
            total_units = sum(final_holdings.values())
            if abs(total_units) > self.max_units:
                scaler = self.max_units/abs(total_units)
        all_tickers = list(set(list(final_holdings.keys()) + list(holdings_on_date.keys())))
        final_size, self.rebal_finished = self.trades_size(all_tickers,final_holdings, ratio, scaler, portfolio)
        trades_on_date_new = [
            Trade(
                all_tickers[i],
                date,
                final_size[i],
                denominate='USD',
                trade_type='neutral',
                symbol = partition_ticker(all_tickers[i])[0]
            )
            for i in range(len(all_tickers)) if all_tickers[i] != 'USD'
        ]
        # print([t.size for t in trades_on_date_new])

        return trades_on_date_new

    def trades_size(self, tickers,final_holdings, ratio, scaler, portfolio):
        res1 = [max(min(self.daily_limit, round_to_nearest(max(min(final_holdings.get(ticker, 0) * ratio*scaler, self.max_units),-self.max_units) - portfolio.portfolio_state.positions.get(ticker, 0),self.round_step)),-self.daily_limit) 
                for ticker in tickers]
        res2 = [round_to_nearest(max(min(final_holdings.get(ticker, 0) * ratio*scaler, self.max_units),-self.max_units) - portfolio.portfolio_state.positions.get(ticker, 0),self.round_step) for ticker in tickers]
        return res1, res1==res2
    
    def vol_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7,normalize = True):

        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if tickers is None:
            return self.vol_target
        prev_date = previous_business_day(date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        buz_dates = business_days_until(prev_date, self.vol_period + 15, self.holiday_calendar)
        returns = {}
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] for  ticker in tickers]
        total_value = sum(portfolio_values)
        weights = np.array(portfolio_values)/total_value
        total_qty = sum([holdings_on_date[ticker]  for  ticker in tickers])
        if total_qty != 0 and normalize:
            normalized_total_value = total_value/total_qty
        else: 
            normalized_total_value = total_value
        # print(date, holdings_on_date)
        for ticker in tickers:
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
        vol = res.rolling(self.vol_period).apply(lambda x: np.sqrt(np.sum(decay_factors*x**2))) * np.sqrt(252)
        portfolio_var = []
        for i in range(self.vol_period, len(res)):
            num_udls = len(weights)
            corr_used = corr.iloc[i*num_udls:(i+1)*num_udls].values.reshape(num_udls, num_udls)
            cov_matrix = corr_used * np.outer(vol.iloc[i].values, vol.iloc[i].values)
            portfolio_var.append(weights.T @ cov_matrix @ weights)
        # print(date,portfolio_var )
        if portfolio_var:
            return np.sqrt(portfolio_var[-1]), abs(normalized_total_value)
        else:
            # print(date)
            return self.vol_target, abs(normalized_total_value)     


    def instrument_on_date(self, date):
        instruments = []
        if date.strftime('%Y-%m-%d') != GLOBALPARAMS['today']:
            for nearby_future in self.future_instruments:
                for n_nearby, roll_schedule in self.roll_info:
                    ticker = f'{nearby_future}{self.prefix}_{n_nearby}_{roll_schedule}'
                    instruments.append({'ticker': ticker, 'type': 'NearbyFuture'})
        for strategy, start_date in self.strategy_instruments:
            if date > pd.to_datetime(start_date):
                instruments.append({'ticker': strategy, 'type': 'Strategy'})
        
        return instruments
    
    def risk_dates(self, start_date, end_date):
        dates = pd.date_range(end = start_date, periods= self.lookback_dates, freq='B')
        dates = [date for date in dates if not is_holiday(date, self.holiday_calendar)]
        business_days = business_days_between(start_date, end_date, self.holiday_calendar)
        risk_dates = sorted(set(dates+business_days))  
        # print(risk_dates)
        return risk_dates

    def rebal_dates(self):
        business_dates = [pd.to_datetime(date) for date in business_days_between(self.initial_date, self.end_date, self.holiday_calendar)]
        return business_dates[::self.rebal_freq]


class NotionalMatch(VolTargetStrategy):
    def __init__(self, params):
        super().__init__(params)

    def trades_on_date(self, date, portfolio, risks_on_dates):
        return super().trades_on_date(date, portfolio, risks_on_dates)
    

    def notional_on_date(self, date, holdings_on_date, risks_on_dates,normalize = True):

        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if tickers is None:
            return self.vol_target
        prev_date = previous_business_day(date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] for  ticker in tickers]
        total_value = sum(portfolio_values)
        total_qty = sum([holdings_on_date[ticker]  for  ticker in tickers])
        if total_qty != 0 and normalize:
            normalized_total_value = total_value/total_qty
        else: 
            normalized_total_value = total_value
        # print(date, holdings_on_date)
       
        return (normalized_total_value)



    def trades_on_date(self, date, portfolio, risks_on_dates):
        """
        Adjust the quantity of a strategy based on the var of the historical holding.
        
        :param date: str, date in 'YYYY-MM-DD' format
        :param portfolio: Portfolio object
        :param risks_on_dates: dict, mapping of date to risk values
        :return: [] list of trades to be executed on the date
        """
        udls_holding = self.udl_holding_on_date( date, risks_on_dates)

        holdings_on_date = portfolio.portfolio_state.positions.copy()
        values_on_date = {}
        filtered_pos = udls_holding
        for strategy, positions in udls_holding.items():
            if self.use_ticker_filter:
                positions = self.apply_ticker_filter(positions, self.ticker_filter)
                holdings_on_date = self.apply_ticker_filter(holdings_on_date, self.ticker_filter)
            if self.use_expiration_filter:
                positions = self.apply_expiration_filter(date, positions)
                holdings_on_date = self.apply_expiration_filter(date, holdings_on_date)            

            filtered_pos[strategy] = positions
            values_on_date[strategy] = self.notional_on_date(date, positions, risks_on_dates)
        # print(date, values_on_date)
        # Calculate VaR and adjust trades if needed

        ratio = 1
        leverate_ratio = {}
        # print(date, self.is_udl_trade_day(date, risks_on_dates),self.rebal_finished)
        if self.is_udl_trade_day(date, risks_on_dates) or ( not self.rebal_finished ):
            for strategy, risk in values_on_date.items():
        
                if risk != 0:
                    leverate_ratio[strategy] = self.risk_allowance[strategy]/risk
                else:
                    leverate_ratio[strategy] = 0
            final_holdings = {}
            for strategy, leverage in leverate_ratio.items():
                for pos, size in filtered_pos[strategy].items():
                    # print(date, strategy, pos, size,risk, leverage)
                    if pos not in final_holdings:
                        final_holdings[pos] =size*leverage
                    else:
                        final_holdings[pos] +=leverage  
        else:
            if date not in self.rebal_dates():
                return []
            total_value  = self.notional_on_date(date, holdings_on_date, risks_on_dates, normalize = False)
            if (not self.vol_lower_limit < abs(total_value) < self.vol_upper_limit) and total_value != 0 :
                ratio =  self.vol_target/ abs(total_value)
            # here holdings on date is the position hold by the strategy. filtered by the filter
            final_holdings = holdings_on_date.copy()
            # print(date, vol, total_value, ratio)
        # print(date, final_holdings,holdings_on_date)
        scaler = 1
        if self.cap_total_units:
            total_units = sum(final_holdings.values())
            if abs(total_units) > self.max_units:
                scaler = self.max_units/abs(total_units)
        all_tickers = list(set(list(final_holdings.keys()) + list(holdings_on_date.keys())))
        final_size, self.rebal_finished = self.trades_size(all_tickers,final_holdings, ratio, scaler, portfolio)
        trades_on_date_new = [
            Trade(
                all_tickers[i],
                date,
                final_size[i],
                denominate='USD',
                trade_type='neutral',
                symbol = partition_ticker(all_tickers[i])[0]
            )
            for i in range(len(all_tickers)) if all_tickers[i] != 'USD'
        ]
        # print([t.size for t in trades_on_date_new])

        return trades_on_date_new