from backtester.src.core.units_module.base_module import BaseUnitModule
import pandas as pd
from my_holidays.holiday_utils import *
from backtester.src.core.portfolio import Trade
from backtester.src.core.utils.utils import partition_ticker
import numpy as np
from abc import  abstractmethod
from backtester.src.core.units_module.utils.kalman import KalmanTrendEstimator
from backtester.src.core.units_module.utils.signal_utils import *
from backtester.src.core.utils.global_params_helper import GLOBALPARAMS
import copy

CONTRACT_TYPE = {'Q':'quarterly', 'Y': 'yearly', '':'monthly'}

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



class TS_Signal(BaseUnitModule):
    def __init__(self, params):
        super().__init__()
        self.params = params
        self.lookbacks = params['lookbacks']
        self.returns = params.get('returns', 1)
        self.roll_info = params.get('roll_info',[(0,7),(1,7)])
        self.contract_type = params.get('contract_type','monthly') 
        self.future_instruments = params.get('future_instruments', [])
        self.initial_date = params['initial_date']
        self.end_date = params['end_date']
        self.decay_factor  = params.get('decay_factor', 1.0)
        self.rebal_freq = params.get('rebal_freq', 1)  
        self.inverse_signal = params.get('inverse_signal', 1) 
        self.floor_nearby = params.get('floor_nearby', False)
        self.zero_delay = params.get('zero_delay', False)   
    @property
    def holiday_calendar(self):
        return self.params['holiday_calendar']
    
    @property
    def prefix(self):
        return {'quarterly': 'Q', 'yearly': 'Y', 'monthly': ''}[self.contract_type]

    @abstractmethod
    def trades_on_date(self, _date, portfolio, risks_on_dates):
        pass

     

    def instrument_on_date(self, date):
        instruments = []
        for nearby_future in self.future_instruments:
            for n_nearby, roll_schedule in self.roll_info:
                ticker = f'{nearby_future}{self.prefix}_{n_nearby}_{roll_schedule}'
                instruments.append({'ticker': ticker, 'type': 'NearbyFuture'})
        
        return instruments

    @abstractmethod
    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        pass

    def prepare_ts_data(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        tickers =[ticker for ticker in  list(holdings_on_date.keys()) if ticker != 'USD' and holdings_on_date[ticker] != 0]
        if not tickers:
            return pd.DataFrame(), np.array([]), 0
        returns = {}
        if self.zero_delay:
            prev_date = _date
        else:
            prev_date = previous_business_day(_date, self.holiday_calendar)
            prev_date = pd.to_datetime(prev_date)
        buz_dates = business_days_until(prev_date, max(self.lookbacks) + 8, self.holiday_calendar)
        # print(buz_dates)
        portfolio_values = [holdings_on_date[ticker] * risks_on_dates[prev_date][ticker]['close'] for  ticker in tickers if ticker != 'USD']
        total_value = sum(portfolio_values)
        weights = np.array(portfolio_values)/total_value
        for ticker in tickers:
            symbol,month,_=partition_ticker(ticker)
            if len(month) == 2:
                suffix = month[0]
            else:
                suffix = ''
            nearby = contract_to_nearby(_date, ticker[-3:], roll_schedule, contract_type = CONTRACT_TYPE[suffix] )
            if self.floor_nearby:
                nearby = max(0, nearby)


            ret = [ risks_on_dates[buz_date][f'{symbol}{suffix}_{nearby}_{roll_schedule}']['return'] for buz_date in buz_dates]
            returns[ticker] = ret
        total_return = pd.DataFrame(returns)
        return total_return, weights, total_value

    def generate_trade(self, positions, _date):
        trades = []
        for ticker, size in positions.items():
            if ticker != 'USD':
                trades.append(Trade(
                    ticker,
                    _date,
                    -size *2,
                    denominate='USD',
                    trade_type='neutral',
                    symbol = partition_ticker(ticker)[0]
                ))
        return trades

    def lookback_dates(self, _date):
        """
        Generate a list of lookback dates to calculate the risk metric
        :param date: str, date in 'YYYY-MM-DD' format
        :return: list of str, dates in 'YYYY-MM-DD' format
        """
        period = max(self.lookbacks) + 10
        dates = pd.date_range(end = _date, periods=period + 10, freq='B')
        dates = [date for date in dates if not is_holiday(date, self.holiday_calendar)]
        return dates[-period:]

    def risk_dates(self, start_date, end_date):
        """
        Generate a list of dates to calculate the risk metric over a given period
        :param start_date: str, start date in 'YYYY-MM-DD' format
        :param end_date: str, end date in 'YYYY-MM-DD' format
        :return: list of str, dates in 'YYYY-MM-DD' format
        """
        extra_dates = self.lookback_dates(start_date)
        business_dates = business_days_between(start_date, end_date, self.holiday_calendar)
        risk_dates = sorted(set(extra_dates+business_dates))
        return risk_dates
    
    def rebal_dates(self):
        #extra dates so that we can calculate trade on today in risk mode.
        end_date = pd.to_datetime(self.end_date)+pd.Timedelta(days=10)
        end_date = end_date.strftime('%Y-%m-%d')
        business_dates = [pd.to_datetime(date) for date in business_days_between(self.initial_date, end_date, self.holiday_calendar)]
        
        return business_dates[::self.rebal_freq]
    


class TS_Skew(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        skew , total_values= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        if skew*total_values>0:
            portfolio.portfolio_state.flip_trades_direction()
            # print(date, skew, total_values)
            return self.generate_trade(holdings_on_date,   date)
        else:
            return []
        
    def signals_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        skew_signal = 0
        for lookback in self.lookbacks:
            # print((portfolio_return.rolling(lookback).skew()))
            skew_signal += (portfolio_return.rolling(lookback).skew()).iloc[-1]
        return skew_signal/len(self.lookbacks), total_value
    

class TS_Trend(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        trend , total_values= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        # print(date, trend)
        # print(date, trend, total_values)
        if self.inverse_signal*trend * total_values <0:
            portfolio.portfolio_state.flip_trades_direction()
            
            return self.generate_trade(holdings_on_date, date)
        else:
            return []
        
    def signals_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        # print(portfolio_return)

        trend_signal = 0
        for lookback in self.lookbacks:
            decay_factor = self.decay_factor
            decay_factors = decay_factor ** np.arange(lookback)[::-1]
            decay_factors /= np.sum(decay_factors)
            # print((portfolio_return.rolling(lookback).skew()))
            trend_signal += (portfolio_return['cumreturn'].rolling(lookback).apply(lambda x: np.sum(decay_factors*x)/np.mean(x.iloc[-self.returns:]))).rolling(5).mean().iloc[-1]
        return trend_signal/len(self.lookbacks)-1, total_value
    

class TS_Trend_COT(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
        self.symbol = params['symbol']
        self.cot_source = params['cot_source']
        self.cot_path = params['cot_path']
        self.include_eex = params.get('include_eex', False)
        self.signal_source = params['signal_source']

        self.cot_data = self.get_cot_data()
        
    def get_cot_data(self):
        cot_data_sgx = pd.read_csv(self.cot_path) 
        cot_data_sgx['date'] = pd.to_datetime(cot_data_sgx['Clear Date'])
        cot_data_sgx = cot_data_sgx[cot_data_sgx['Symbol'] == self.symbol]
        cot_data_sgx = cot_data_sgx.set_index('date')
        cot_data_sgx.fillna(0, inplace=True)
        cot_data_eex = pd.read_csv('C:/Users/yuhang.hou/projects/test_Freight_FM/data/series/EEX/EEX_COT.csv')
        cot_data_eex['date'] = pd.to_datetime(cot_data_eex['Clear Date'])
        cot_data_eex = cot_data_eex.set_index('date')
        cot_data_eex.fillna(0, inplace=True)
        cot_data_eex = cot_data_eex[cot_data_eex['Symbol'] == self.symbol]
        col_list = [  'Open Interest', 'Physicals Long', 'Physicals Short', 'Managed Money Long', 'Managed Money Short', 'Financial Institutions Long', 'Financial Institutions Short']
        # print(cot_data_eex)
        cot_data = cot_data_sgx[col_list].copy()
        if self.include_eex:
            cot_data += cot_data_eex[col_list ]
        cot_data.fillna(0, inplace=True)
        cot_data.reset_index(inplace=True)
        cot_data['MM Net'] = cot_data['Managed Money Long'] - cot_data['Managed Money Short']
        cot_data['MM Ratio'] = cot_data['MM Net'] / cot_data['Open Interest']
        cot_data['FI Net'] = cot_data['Financial Institutions Long'] - cot_data['Financial Institutions Short']
        cot_data['FI Ratio'] = cot_data['FI Net'] / cot_data['Open Interest']
        cot_data['P Net'] = cot_data['Physicals Long'] - cot_data['Physicals Short']
        cot_data['P Ratio'] = cot_data['P Net'] / cot_data['Open Interest']
        # print(cot_data)
        cot_data['MM ZScore'] = (cot_data['MM Ratio'] - cot_data['MM Ratio'].rolling(26).mean()) / cot_data['MM Ratio'].rolling(20).std(ddof=0)
        cot_data['P ZScore'] = (cot_data['P Ratio'] - cot_data['P Ratio'].rolling(26).mean()) / cot_data['P Ratio'].rolling(20).std(ddof=0)
        cot_data['FI ZScore'] = (cot_data['FI Ratio'] - cot_data['FI Ratio'].rolling(26).mean()) / cot_data['FI Ratio'].rolling(20).std(ddof=0)
        # print(cot_data[['MM Ratio','MM ZScore','Managed Money Long', 'Managed Money Short']].tail(50))

        return cot_data

    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        trend , total_values= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        # print(date, trend)
        print(date, trend, total_values)
        if self.inverse_signal*trend * total_values <0:
            portfolio.portfolio_state.flip_trades_direction()
            
            return self.generate_trade(holdings_on_date, date)
        else:
            return []
        
    def signals_on_date(self, date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        _, _, total_value = self.prepare_ts_data(date, holdings_on_date, risks_on_dates, roll_schedule)
        cot = self.cot_data[self.cot_data['date'] < date].copy()
        # print(cot['date'].max(), date)
        trend_signal = 0
        for lookback in self.lookbacks:
            # trend_signal = cot['spec'].diff(lookback).diff(lookback).iloc[-1]
            # trend_signal = cot['MM Ratio'].pct_change(lookback).iloc[-1]
            if self.signal_source == 'physical':
                trend_signal = cot['P ZScore'].iloc[-1]
            elif self.signal_source == 'mm':
                trend_signal = cot['MM ZScore'].iloc[-1]
            else:
                trend_signal = cot['FI ZScore'].iloc[-1]
            # trend_signal = cot['P Ratio'].pct_change(lookback).iloc[-1]
        return trend_signal, total_value

    def rebal_dates(self):
        end_date = pd.to_datetime(self.end_date)+pd.Timedelta(days=10)
        end_date = end_date.strftime('%Y-%m-%d')
        business_dates = [pd.to_datetime(date) for date in business_days_between(self.initial_date, end_date, self.holiday_calendar)]
        if self.cot_source == 'SGX':
            business_dates = [ date for date in business_dates if date.weekday() == 2 ]
        else:
            business_dates = [date for date in business_dates if date.weekday() == 1]
        return business_dates
    
class TS_Trend_KalmanFilter(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
        self.flter_ratio = params.get('filter_ratio',0.5)
        self.return_lookback = params.get('return_lookback', 5)
        self.noise_ratio = params.get('noise_ratio', 0.05)
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        trend , total_values, noise_std= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        # print(date, trend)
        # print(date, trend, total_values)
        if self.inverse_signal*trend * total_values <0 and abs(trend)>(noise_std*self.flter_ratio):
            portfolio.portfolio_state.flip_trades_direction()
            return self.generate_trade(holdings_on_date, date)
        else:
            return []
        
    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        obs_noise = np.std(portfolio_return)**2
        portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        if len(portfolio_return)<10:
            return 0, total_value, 0

        trend_signal = 0
        data_to_use = portfolio_return['cumreturn'].values
        kf = KalmanTrendEstimator(
            process_noise=self.noise_ratio*obs_noise,
            observation_noise=obs_noise,
            initial_level= data_to_use[0],
            initial_trend=0
        )
        estimates = []
        # predicts = []
        for measurement in data_to_use:
            estimates.append(kf.update(measurement))
            # predicts.append(kf.predict())

        trend_signal = (estimates[-1]['level'] - estimates[-self.return_lookback]['level'])/estimates[-self.return_lookback]['level']
        
        diff = [estimates[i]['level'] - data_to_use[i] for i in range(len(estimates))]
        # print(diff)
        noise_std = np.std(diff)/estimates[-1]['level']
        # trend_signal = estimates[-1]['trend']
        # trend_signal = (predicts[-1]-data_to_use[-1])/data_to_use[-1]
        print(_date,obs_noise,trend_signal,total_value,noise_std)
        return trend_signal, total_value, noise_std
    

class TS_Trend_KalmanFilter_Zscore(TS_Trend_KalmanFilter):
    def __init__(self, params):
        super().__init__(params)
        self.strategy_instruments = params.get('strategy_instruments',[]) 
        self.previous_holding = {}
        self.asset_table = params['asset_table']
        self.roll_schedule = params.get('roll_schedule', 7)


    def is_udl_trade_day(self, date, risks_on_dates):
        trades = []
        for x in self.asset_table:
            trades += risks_on_dates[date][x['strategy']]['trades_for_date']
        trades = [trade for trade in trades if trade['date'] == date]
        return len(trades)>0

    def udl_holding_on_date(self, date, risks_on_dates):
        if GLOBALPARAMS['risk_mode'] and date.strftime('%Y-%m-%d') == GLOBALPARAMS['today']:
            strategy_positions = {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}
            strategy_trades = {x['strategy']: risks_on_dates[date][x['strategy']]['trades_for_date'] for x in self.asset_table}
            return final_positions_on_date(date, strategy_positions, strategy_trades)
        else:
            return {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}

    def trades_on_date(self, date, portfolio, risks_on_dates):

        udls_holding = self.udl_holding_on_date( date, risks_on_dates)
        # print(date, udls_holding)
        rebal_dates = self.rebal_dates()
        holding_strategies = self.previous_holding
        if date not in rebal_dates:
            return []

        holdings_on_date = portfolio.portfolio_state.positions.copy()

        trend_on_date = {}
        values_on_date = {}
        
        for strategy, positions in udls_holding.items():
            trend_on_date[strategy],values_on_date[strategy] = self.signals_on_date(date, positions, risks_on_dates,self.roll_schedule)
        for strategy, trend in trend_on_date.items():
          
            if trend*values_on_date[strategy]>0:
                holding_strategies[strategy] = 1
            else:
                holding_strategies[strategy] = -1

        self.previous_holding = holding_strategies
        

        final_size,all_tickers = self.size_on_date( holding_strategies, holdings_on_date,udls_holding)
        # print(date,all_tickers, holding_strategies)
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
        
    def size_on_date(self,  holding_strategies, holdings_on_date,udls_holding):
        agg_size = {}
        for strategy, holding in holding_strategies.items():
            for ticker, size in udls_holding[strategy].items():
                agg_size[ticker] = holding*size+ agg_size.get(ticker,0)
        final_size = []
        all_tickers = list(set(list(agg_size.keys()) + list(holdings_on_date.keys())))
        for ticker in all_tickers:
            final_size.append(agg_size.get(ticker,0)-holdings_on_date.get(ticker,0))
        return final_size, all_tickers
    
    def generate_trade(self, positions, _date,_size):
        trades = []
        ticker_ratios = {}
        for ticker, size in positions.items():
            if ticker != 'USD':
                trades.append(Trade(
                    ticker,
                    _date,
                    _size -size ,
                    denominate='USD',
                    trade_type='neutral',
                    symbol = partition_ticker(ticker)[0]
                ))
                ticker_ratios[ticker] = _size/size
        print(_date, ticker_ratios)    
        return trades, ticker_ratios

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

    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        
        portfolio_return = total_return.dot(weights)
        obs_noise = np.std(portfolio_return)**2
        portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        portfolio_return['zscore'] = ( portfolio_return['cumreturn']- portfolio_return['cumreturn'].rolling(60).mean())/ portfolio_return['cumreturn'].rolling(60).std()

        if len(portfolio_return)<10:
            return 0,  total_value

        _signal = portfolio_return['zscore'].rolling(5).mean().diff(3).iloc[-1]
        
        # data_to_use = portfolio_return['zscore'].dropna().values
        # kf = KalmanTrendEstimator(
        #     process_noise=self.noise_ratio*obs_noise,
        #     observation_noise=obs_noise,
        #     initial_level= data_to_use[0],
        #     initial_trend=0
        # )
        # estimates = []
        # # predicts = []
        # for measurement in data_to_use:
        #     estimates.append(kf.update(measurement))
        #     # predicts.append(kf.predict())
        # level = pd.Series([res['level'] for res in estimates])
        # trend_signal = level.pct_change(self.return_lookback).dropna()
        
        # trend_vol = trend_signal.std()
        # # print(trend_signal)
        # signal = trend_signal.iloc[-1]
        # trend_speed = (trend_signal.iloc[-1]-trend_signal.iloc[-self.return_lookback])/signal
        # diff = [estimates[i]['level'] - data_to_use[i] for i in range(len(estimates))]
        # # print(diff)
        # noise_std = np.std(diff)/estimates[-1]['level']
        print(_date, 'trend_signal', _signal, 'total_value', total_value,)
        return _signal ,total_value #, trend_vol, total_value, noise_std ,trend_speed   
    
class TS_Trend_KalmanFilter_Shift(TS_Trend_KalmanFilter):
    def __init__(self, params):
        super().__init__(params)
        self.flter_ratio = params.get('filter_ratio',0.5)
    
    def trades_on_date(self, _date, portfolio, risks_on_dates):

        trades_on_date = portfolio.portfolio_state.trades_for_date
        dates = [trade.date for trade in trades_on_date]
        if _date in dates:
            trade = [trade for trade in trades_on_date if 'last_rollin_date'in trade.extra_info and _date == trade.date][0]
            holding_to_use = {trade.ticker: trade.size}
            trend , total_values, noise_std= self.signals_on_date(_date, holding_to_use, risks_on_dates)
            cal = self.holiday_calendar
            new_trades = []
            
            if self.inverse_signal*trend * total_values <0 and abs(trend)>(noise_std*self.flter_ratio):
                for trade in trades_on_date:
                    # print(type(trade.date), type( trade.extra_info['last_rollin_date']))
                    if 'last_rollin_date' in trade.extra_info and trade.date< trade.extra_info['last_rollin_date']:
                        temp_trade = copy.deepcopy(trade)
                        temp_trade.date = pd.to_datetime(apply_date_rule(trade.date, '1B', cal))
                        new_trades.append(temp_trade)
                    else:
                        new_trades.append(trade)
                portfolio.portfolio_state.reset_trades(new_trades)

        return []

class TS_Trend_KalmanFilter_CS(TS_Trend_KalmanFilter):
    """
    CS stand for cross sectiton
    """
    def __init__(self, params):
        super().__init__(params)
        self.strategy_instruments = params.get('strategy_instruments',[]) 
        self.cache_data = {}
        self.asset_table = params['asset_table']
        self.basket_size = params.get('basket_size', 2)



    def is_udl_trade_day(self, date, risks_on_dates):
        trades = []
        for x in self.asset_table:
            trades += risks_on_dates[date][x['strategy']]['trades_for_date']
        trades = [trade for trade in trades if trade['date'] == date]
        return len(trades)>0

    def udl_holding_on_date(self, date, risks_on_dates):
        if GLOBALPARAMS['risk_mode'] and date.strftime('%Y-%m-%d') == GLOBALPARAMS['today']:
            strategy_positions = {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}
            strategy_trades = {x['strategy']: risks_on_dates[date][x['strategy']]['trades_for_date'] for x in self.asset_table}
            return final_positions_on_date(date, strategy_positions, strategy_trades)
        else:
            return {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}

    def trades_on_date(self, date, portfolio, risks_on_dates):

        udls_holding = self.udl_holding_on_date( date, risks_on_dates)
        # print(date, udls_holding)
        rebal_dates = self.rebal_dates()
        holdings_on_date = portfolio.portfolio_state.positions.copy()
        if date not in rebal_dates:
            holding_strategies = self.cache_data['previous_holding']
            final_size,all_tickers = self.size_on_date( holding_strategies, holdings_on_date,udls_holding)
            # return []
        else:
            print(date, portfolio.portfolio_state.balance)
            holding_strategies = {}

            trend_on_date = {}
            values_on_date = {}
            for strategy, positions in udls_holding.items():
                trend_on_date[strategy],values_on_date[strategy] = self.signals_on_date(date, positions, risks_on_dates,15)
            sorted_strat = sorted(trend_on_date, key=trend_on_date.get, reverse=True)
            top_bsk = sorted_strat[:self.basket_size]
            bottom_bsk = sorted_strat[-self.basket_size:]
            long_signal = sum([trend_on_date[x] for x in top_bsk])/len(top_bsk)
            short_signal = sum([trend_on_date[x] for x in bottom_bsk])/len(bottom_bsk)

            print(date, long_signal, short_signal)

            for idx in range(self.basket_size):
                holding_strategies[top_bsk[idx]] = 0
                holding_strategies[bottom_bsk[idx]] = 0
                if long_signal > 0:
                    holding_strategies[top_bsk[idx]] = 1
                if short_signal < 0:
                    if long_signal > 0 and values_on_date[bottom_bsk[idx]] != 0:
                        holding_strategies[bottom_bsk[idx]] = -values_on_date[top_bsk[idx]]/values_on_date[bottom_bsk[idx]]
                    else:
                        holding_strategies[bottom_bsk[idx]] = -1

                    # if values_on_date[bottom_bsk[idx]] != 0:
                    #     holding_strategies[bottom_bsk[idx]] = -values_on_date[top_bsk[idx]]/values_on_date[bottom_bsk[idx]]
                    # else:
                    #     holding_strategies[bottom_bsk[idx]] = -1
                    
            self.cache_data['top'] = top_bsk
            self.cache_data['bottom'] = bottom_bsk
            self.cache_data['previous_holding'] = holding_strategies
            final_size,all_tickers = self.size_on_date( holding_strategies, holdings_on_date,udls_holding)
        # print(date,all_tickers, holding_strategies)
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
        
    def size_on_date(self,  holding_strategies, holdings_on_date,udls_holding):
        agg_size = {}
        for strategy, holding in holding_strategies.items():
            for ticker, size in udls_holding[strategy].items():
                agg_size[ticker] = holding*size+ agg_size.get(ticker,0)
        final_size = []
        all_tickers = list(set(list(agg_size.keys()) + list(holdings_on_date.keys())))
        for ticker in all_tickers:
            final_size.append(agg_size.get(ticker,0)-holdings_on_date.get(ticker,0))
        return final_size, all_tickers
    
    def generate_trade(self, positions, _date,_size):
        trades = []
        ticker_ratios = {}
        for ticker, size in positions.items():
            if ticker != 'USD':
                trades.append(Trade(
                    ticker,
                    _date,
                    _size -size ,
                    denominate='USD',
                    trade_type='neutral',
                    symbol = partition_ticker(ticker)[0]
                ))
                ticker_ratios[ticker] = _size/size
        # print(_date, ticker_ratios)    
        return trades, ticker_ratios

    def instrument_on_date(self, date):
        instruments = []
        if date.strftime('%Y-%m-%d') != GLOBALPARAMS['today']:
            for row in self.future_instruments:
                ticker = f'{row[0]}_{row[1]}_{row[2]}'
                instruments.append({'ticker': ticker, 'type': 'NearbyFuture'})
        for strategy, start_date in self.strategy_instruments:
            if date > pd.to_datetime(start_date):
                instruments.append({'ticker': strategy, 'type': 'Strategy'})
        
        return instruments

    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        # obs_noise = np.std(portfolio_return)**2
        # portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        if len(portfolio_return)<10:
            return 0,total_value

        # data_to_use = portfolio_return['cumreturn'].values
        # kf = KalmanTrendEstimator(
        #     process_noise=self.noise_ratio*obs_noise,
        #     observation_noise=obs_noise,
        #     initial_level= data_to_use[0],
        #     initial_trend=0
        # )
        # estimates = []
        # for measurement in data_to_use:
        #     estimates.append(kf.update(measurement))
        # level = pd.Series([res['level'] for res in estimates])

       
        trend_signal_short = portfolio_return.rolling(self.return_lookback).mean()
        trend_signal_long = portfolio_return.rolling(self.return_lookback*2).mean()
        signal = trend_signal_short.iloc[-1]-trend_signal_long.iloc[-1]
      
        return signal, total_value


class TS_Trend_KalmanFilter1(TS_Trend_KalmanFilter):
    def __init__(self, params):
        super().__init__(params)
        self.strategy_instruments = params.get('strategy_instruments',[]) 
        self.previous_holding = {}
        self.asset_table = params['asset_table']
        self.roll_schedule = params.get('roll_schedule', 7)


    def is_udl_trade_day(self, date, risks_on_dates):
        trades = []
        for x in self.asset_table:
            trades += risks_on_dates[date][x['strategy']]['trades_for_date']
        trades = [trade for trade in trades if trade['date'] == date]
        return len(trades)>0

    def udl_holding_on_date(self, date, risks_on_dates):
        if GLOBALPARAMS['risk_mode'] and date.strftime('%Y-%m-%d') == GLOBALPARAMS['today']:
            strategy_positions = {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}
            strategy_trades = {x['strategy']: risks_on_dates[date][x['strategy']]['trades_for_date'] for x in self.asset_table}
            return final_positions_on_date(date, strategy_positions, strategy_trades)
        else:
            return {x['strategy']: risks_on_dates[date][x['strategy']]['positions'] for x in self.asset_table}

    def trades_on_date(self, date, portfolio, risks_on_dates):

        udls_holding = self.udl_holding_on_date( date, risks_on_dates)
        # print(date, udls_holding)
        rebal_dates = self.rebal_dates()
        holding_strategies = self.previous_holding
        if date not in rebal_dates:
            return []

        holdings_on_date = portfolio.portfolio_state.positions.copy()

        trend_on_date = {}
        vol_on_date = {}
        noise_on_date = {}
        values_on_date = {}
        acceleration_on_date = {}
        
        for strategy, positions in udls_holding.items():
            trend_on_date[strategy],vol_on_date[strategy],values_on_date[strategy],noise_on_date[strategy],acceleration_on_date[strategy] = self.signals_on_date(date, positions, risks_on_dates,self.roll_schedule)
        for strategy, trend in trend_on_date.items():
            if abs(trend)>(noise_on_date[strategy]*self.flter_ratio): #abs(trend)>vol_on_date[strategy] and 
                if trend*values_on_date[strategy]>0:
                    holding_strategies[strategy] = 1
                else:
                    holding_strategies[strategy] = -1

            elif acceleration_on_date[strategy] * trend < 0:
                # print(date,'unwind triggered')
                holding_strategies[strategy] = 0
            else:
                continue
        self.previous_holding = holding_strategies
        

        final_size,all_tickers = self.size_on_date( holding_strategies, holdings_on_date,udls_holding)
        # print(date,all_tickers, holding_strategies)
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
        
    def size_on_date(self,  holding_strategies, holdings_on_date,udls_holding):
        agg_size = {}
        for strategy, holding in holding_strategies.items():
            for ticker, size in udls_holding[strategy].items():
                agg_size[ticker] = holding*size+ agg_size.get(ticker,0)
        final_size = []
        all_tickers = list(set(list(agg_size.keys()) + list(holdings_on_date.keys())))
        for ticker in all_tickers:
            final_size.append(agg_size.get(ticker,0)-holdings_on_date.get(ticker,0))
        return final_size, all_tickers
    
    def generate_trade(self, positions, _date,_size):
        trades = []
        ticker_ratios = {}
        for ticker, size in positions.items():
            if ticker != 'USD':
                trades.append(Trade(
                    ticker,
                    _date,
                    _size -size ,
                    denominate='USD',
                    trade_type='neutral',
                    symbol = partition_ticker(ticker)[0]
                ))
                ticker_ratios[ticker] = _size/size
        # print(_date, ticker_ratios)    
        return trades, ticker_ratios

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

    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        obs_noise = np.std(portfolio_return)**2
        portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        if len(portfolio_return)<10:
            return 0,0,  total_value, 0,0

        trend_signal = 0
        data_to_use = portfolio_return['cumreturn'].values
        kf = KalmanTrendEstimator(
            process_noise=self.noise_ratio*obs_noise,
            observation_noise=obs_noise,
            initial_level= data_to_use[0],
            initial_trend=0
        )
        estimates = []
        # predicts = []
        for measurement in data_to_use:
            estimates.append(kf.update(measurement))
            # predicts.append(kf.predict())
        level = pd.Series([res['level'] for res in estimates])
        trend_signal = level.pct_change(self.return_lookback).dropna()
        
        trend_vol = trend_signal.std()
        signal = trend_signal.iloc[-1]
        trend_speed = (trend_signal.iloc[-1]-trend_signal.iloc[-self.return_lookback])/signal
        diff = [estimates[i]['level'] - data_to_use[i] for i in range(len(estimates))]
        # print(diff)
        noise_std = np.std(diff)/estimates[-1]['level']
        print(_date,signal, trend_vol, total_value, noise_std ,trend_speed)
        return signal, trend_vol, total_value, noise_std ,trend_speed      

class TS_Trend_COT1(TS_Trend_KalmanFilter1):
    def __init__(self, params):
        super().__init__(params)
        self.symbol = params['symbol']
        self.cot_source = params['cot_source']
        self.cot_path = params['cot_path']
        self.include_eex = params.get('include_eex', False)
        self.signal_source = params['signal_source']
        self.zscore_boundry = params.get('zscore_boundry', 1.5)
        self.inverse_signal_trend = params.get('inverse_signal_trend', self.inverse_signal)
        self.cot_data = self.get_cot_data()
        
    def get_cot_data(self):
        cot_data_sgx = pd.read_csv(self.cot_path) 
        cot_data_sgx['date'] = pd.to_datetime(cot_data_sgx['Clear Date'])
        cot_data_sgx = cot_data_sgx[cot_data_sgx['Symbol'] == self.symbol]
        cot_data_sgx = cot_data_sgx.set_index('date')
        cot_data_sgx.fillna(0, inplace=True)
        cot_data_eex = pd.read_csv('C:/Users/yuhang.hou/projects/test_Freight_FM/data/series/EEX/EEX_COT.csv')
        cot_data_eex['date'] = pd.to_datetime(cot_data_eex['Clear Date'])
        cot_data_eex = cot_data_eex.set_index('date')
        cot_data_eex.fillna(0, inplace=True)
        cot_data_eex = cot_data_eex[cot_data_eex['Symbol'] == self.symbol]
        cot_data_eex = cot_data_eex.loc[cot_data_eex.index.isin(cot_data_sgx.index)] 
        col_list = [  'Open Interest', 'Physicals Long', 'Physicals Short', 'Managed Money Long', 'Managed Money Short', 'Financial Institutions Long', 'Financial Institutions Short']
        # print(cot_data_eex)
        cot_data = cot_data_sgx[col_list].copy()
        if self.include_eex:
            col_list += ['Physicals Long Risk', 'Physicals Short Risk']
            cot_data = cot_data_eex[col_list ].copy()
        cot_data['Managed Money Long'] = cot_data_sgx['Managed Money Long'].add(cot_data_eex['Managed Money Long'],fill_value=0)
        cot_data['Managed Money Short'] = cot_data_sgx['Managed Money Short'].add(cot_data_eex['Managed Money Short'],fill_value=0)
        cot_data.fillna(0, inplace=True)
        cot_data.reset_index(inplace=True)
        cot_data['MM Net'] = cot_data['Managed Money Long'] - cot_data['Managed Money Short']
        cot_data['MM Ratio'] = cot_data['MM Net'] / cot_data['Open Interest']
        cot_data['FI Net'] = cot_data['Financial Institutions Long'] - cot_data['Financial Institutions Short']
        cot_data['FI Ratio'] = cot_data['FI Net'] / cot_data['Open Interest']
        cot_data['P Net'] = cot_data['Physicals Long'] - cot_data['Physicals Short']
        cot_data['P Ratio'] = cot_data['P Net'] / cot_data['Open Interest']
        if self.include_eex:
            cot_data['P Net Risk'] = cot_data['Physicals Long Risk'] - cot_data['Physicals Short Risk']
            cot_data['P Risk Ratio'] = cot_data['P Net Risk'] / cot_data['Open Interest']
            cot_data['P Risk ZScore'] = (cot_data['P Risk Ratio'] - cot_data['P Risk Ratio'].rolling(26).mean()) / cot_data['P Risk Ratio'].rolling(20).std(ddof=0)

        # print(cot_data)
        cot_data['MM ZScore'] = (cot_data['MM Ratio'] - cot_data['MM Ratio'].rolling(26).mean()) / cot_data['MM Ratio'].rolling(20).std(ddof=0)
        cot_data['P ZScore'] = (cot_data['P Ratio'] - cot_data['P Ratio'].rolling(26).mean()) / cot_data['P Ratio'].rolling(20).std(ddof=0)
        cot_data['FI ZScore'] = (cot_data['FI Ratio'] - cot_data['FI Ratio'].rolling(26).mean()) / cot_data['FI Ratio'].rolling(20).std(ddof=0)
        # print(cot_data[['MM Ratio','MM ZScore','Managed Money Long', 'Managed Money Short']].tail(50))

        return cot_data


    def trades_on_date(self, date, portfolio, risks_on_dates):
        udl_trading_day = self.is_udl_trade_day(date, risks_on_dates)
        udls_holding = self.udl_holding_on_date( date, risks_on_dates)
        # print(date, udls_holding)
        rebal_dates = self.rebal_dates()
        holding_strategies = self.previous_holding
        if date not in rebal_dates and not udl_trading_day:
            return []

        holdings_on_date = portfolio.portfolio_state.positions.copy()

        zscore_on_date = {}
        delta_on_date = {}
        
        
        for strategy, positions in udls_holding.items():
            zscore_on_date[strategy], delta_on_date[strategy]= self.signals_on_date(date)
        
        for strategy, zscore in zscore_on_date.items():
            
            if abs(zscore)>self.zscore_boundry : #abs(trend)>vol_on_date[strategy] and 
                if self.inverse_signal*zscore>0:
                    if delta_on_date[strategy]<-0.3:
                        holding_strategies[strategy] = 1.5
                    else:
                        holding_strategies[strategy] = 1
                else:
                    if delta_on_date[strategy]>0.5:
                        holding_strategies[strategy] = -1.5
                    else:
                        holding_strategies[strategy] = -1
                # holding_strategies[strategy] = 0
            elif abs(zscore)>self.zscore_boundry/2 :
                if self.inverse_signal_trend*delta_on_date[strategy]>0.5:
                    holding_strategies[strategy] = -0.5
                elif self.inverse_signal_trend*delta_on_date[strategy]<-0.5:
                    holding_strategies[strategy] = 0.5
                else:
                    holding_strategies[strategy] = 0
            else:
                holding_strategies[strategy] = 0
            print(date, zscore, delta_on_date[strategy],holding_strategies[strategy])
        self.previous_holding = holding_strategies
        

        final_size,all_tickers = self.size_on_date( holding_strategies, holdings_on_date,udls_holding)
        # print(date,all_tickers, holding_strategies)
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
        
        
    def signals_on_date(self, date):
        cot = self.cot_data[self.cot_data['date'] < date].copy()
        trend_signal = 0

        if self.signal_source == 'physical':
            trend_signal = cot['P ZScore']
        elif self.signal_source == 'mm':
            trend_signal = cot['MM ZScore']
        elif self.signal_source == 'physical_risk':
            trend_signal = cot['P Risk ZScore']
        else:
            trend_signal = cot['FI ZScore']
        delta = trend_signal.diff(1).iloc[-1]
        signal = trend_signal.iloc[-1]
        return signal,delta

    def rebal_dates(self):
        end_date = pd.to_datetime(self.end_date)+pd.Timedelta(days=10)
        end_date = end_date.strftime('%Y-%m-%d')
        business_dates = [pd.to_datetime(date) for date in business_days_between(self.initial_date, end_date, self.holiday_calendar)]
        if self.cot_source == 'SGX':
            business_dates = [ date for date in business_dates if date.weekday() == 2 ]
        else:
            business_dates = [date for date in business_dates if date.weekday() == 1]
        return business_dates

class TS_Reversion(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
        self.return_lookback = params.get('return_lookback', 5)
        self.filter_ratio = params.get('filter_ratio',0.5)

    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        total_value,  deviation,  trend_signal, rsi   = self.signals_on_date(date, holdings_on_date, risks_on_dates)
        # print(date, trend)
        if total_value > 0:
            if rsi> 80 and deviation> 0.8:
                    portfolio.portfolio_state.flip_trades_direction()
                    return self.generate_trade(holdings_on_date, date)
            elif trend_signal < 0 :
                portfolio.portfolio_state.flip_trades_direction()
                return self.generate_trade(holdings_on_date, date)
            else:
                return []
                
        else:
            if rsi< 20 and deviation < 0.2:
                    portfolio.portfolio_state.flip_trades_direction()
                    return self.generate_trade(holdings_on_date, date)
            elif trend_signal > 0 :
                portfolio.portfolio_state.flip_trades_direction()
                return self.generate_trade(holdings_on_date, date)
            else:
                return []
        return []
        
    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        if len(portfolio_return)<10:
            return  total_value,0.5 , 0, 50
        # print(portfolio_return)
        # trend signal part
        trend_signal = 0
        data_to_use = portfolio_return['cumreturn'].values
        kf = KalmanTrendEstimator(
            process_noise=0.01,
            observation_noise=1.0,
            initial_level= data_to_use[0],
            initial_trend=0
        )
        estimates = []
        # predicts = []
        for measurement in data_to_use:
            estimates.append(kf.update(measurement))
            # predicts.append(kf.predict())

        trend_signal = (estimates[-1]['level'] - estimates[-self.return_lookback]['level'])/estimates[-self.return_lookback]['level']

        rsi = RSI(data_to_use, 10)

        data_to_use = portfolio_return['cumreturn']
        bollinger_bands_data = bollinger_bands(data_to_use)

        deviation = bollinger_bands_data.iloc[-1]['%B']
        
        return total_value,  deviation,  trend_signal, rsi       
class TS_Reversion_Bollinger_RSI(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
        self.return_lookback = params.get('return_lookback', 5)
        self.filter_ratio = params.get('filter_ratio',0.5)
        self.rsi_high = params.get('rsi_high', 55)
        self.rsi_low = 100 - self.rsi_high
        self.bollinger_high = params.get('bollinger_high', 0.5)
        self.bollinger_low = 1 - self.bollinger_high
    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()
        # trades_on_date = portfolio.portfolio_state.trades_for_date.copy()
        # print(date, trades_on_date)
        # trades_on_date = [trade for trade in trades_on_date if trade.date == date]
        if date not in rebal_dates:
            return []
    
        #    Check if strategy instruments are implemented
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        # print(date, holdings_on_date)       
        total_value,deviation, trend_signal, rsi= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        self.data_to_store[date] = {}
        signal_to_store = {'total_value':total_value,'deviation':deviation, 'trend_signal': trend_signal, 'rsi': rsi}
        self.data_to_store[date]['signal_to_store'] = signal_to_store
        # print(date, trend)
        if total_value>0:
            if rsi < self.rsi_low and deviation < self.bollinger_low:
                portfolio.portfolio_state.flip_trades_direction()
                return self.generate_trade(holdings_on_date, date)
            elif trend_signal < 0 :
                portfolio.portfolio_state.flip_trades_direction()
                return self.generate_trade(holdings_on_date, date)
            else:
                return []
        else:
            if rsi >self.rsi_high and deviation > self.bollinger_high :
                portfolio.portfolio_state.flip_trades_direction()
                return self.generate_trade(holdings_on_date, date)
           
            elif trend_signal > 0 :
                portfolio.portfolio_state.flip_trades_direction()
                return self.generate_trade(holdings_on_date, date)
            else:
                return []
        
    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        # total_return, weights, total_value = self.prepare_ts_data(date, holdings_on_date, risks_on_dates, roll_schedule)
        # portfolio_return = total_return.dot(weights)
        # portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        # if len(portfolio_return)<10:
        #     return 0, total_value,0 ,0,0
        # # print(portfolio_return)
        # data_to_use = portfolio_return['cumreturn']
        # bollinger_bands_data = bollinger_bands(data_to_use)
        # level = data_to_use.values[-1]
        # mid = bollinger_bands_data.iloc[-1]['Middle']
        # profitiablity =  abs(mid-level)/mid>0.3
        # deviation = bollinger_bands_data.iloc[-1]['%B']
        # trend, noise = trend_kalman(data_to_use.values, self.return_lookback)
        
        # return deviation-0.5, total_value, profitiablity,trend, noise
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        portfolio_return = total_return.dot(weights)
        portfolio_return['cumreturn'] = (1+portfolio_return).cumprod()
        if len(portfolio_return)<10:
            return  total_value, 0.5,0 , 50

        trend_signal = 0
        data_to_use = portfolio_return['cumreturn'].values
        kf = KalmanTrendEstimator(
            process_noise=0.01,
            observation_noise=1.0,
            initial_level= data_to_use[0],
            initial_trend=0
        )
        estimates = []
        # predicts = []
        for measurement in data_to_use:
            estimates.append(kf.update(measurement))
            # predicts.append(kf.predict())

        trend_signal = (estimates[-1]['level'] - estimates[-self.return_lookback]['level'])/estimates[-self.return_lookback]['level']
        bollinger_bands_data = bollinger_bands(portfolio_return['cumreturn'])
        deviation = bollinger_bands_data.iloc[-1]['%B']

        rsi = RSI(portfolio_return['cumreturn'], 10).iloc[-1]

        # print(_date, total_value,deviation, trend_signal, rsi)
        return total_value,deviation, trend_signal, rsi
    


class TS_ML_Signal(TS_Signal):
    def __init__(self, params):
        super().__init__(params)
        self.signal = pd.DataFrame()


    
    def trades_on_date(self, date, portfolio, risks_on_dates):
        rebal_dates = self.rebal_dates()

        if date not in rebal_dates:
            return []
       
        holdings_on_date = portfolio.portfolio_state.positions.copy() 
        total_value, trend_signal= self.signals_on_date(date, holdings_on_date, risks_on_dates)
        self.data_to_store[date] = {}
        signal_to_store = {'total_value':total_value, 'trend_signal': trend_signal}
        self.data_to_store[date]['signal_to_store'] = signal_to_store
        print(date, trend_signal, total_value,holdings_on_date)
        if total_value*trend_signal<0:
            portfolio.portfolio_state.flip_trades_direction()
            return self.generate_trade(holdings_on_date, date)
        else:
            return []      
        
    def signals_on_date(self, _date, holdings_on_date, risks_on_dates, roll_schedule = 7):
        # print(_date)
        total_return, weights, total_value = self.prepare_ts_data(_date, holdings_on_date, risks_on_dates, roll_schedule)
        prev_date = previous_business_day(_date, self.holiday_calendar)
        prev_date = pd.to_datetime(prev_date)
        if len(self.signal)==0:
            signal = pd.read_csv('C:/Users/yuhang.hou/projects/test_Freight_FM/data/series/C5TC/signal.csv')
            signal['date'] = pd.to_datetime(signal['date'])
        
            signal.set_index('date', inplace=True)
            self.signal= signal
        # signal = signal.at[_date,'Predicted']
        try:
            trend_signal = self.signal.at[prev_date,'Predicted']
        except:
            print(_date, 'no data is found')
            trend_signal = 0


        return total_value, trend_signal