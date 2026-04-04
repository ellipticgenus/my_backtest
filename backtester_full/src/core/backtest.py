from backtester.src.core.portfolio import Portfolio
from backtester.src.core.units_module.rolling import SimpleRolling, PreRoll
from backtester.src.core.units_module.dynamic_rolling import DynamicRolling
from backtester.src.core.units_module.var_adjustment import  VarAdjustment
from backtester.src.core.units_module.vol_target import VolTarget
from backtester.src.core.units_module.vol_target_strategy import *
from backtester.src.core.units_module.skew import Skew
from backtester.src.core.units_module.trend import Trend
from backtester.src.core.units_module.ts_signal import *

from my_holidays.holiday_utils import *
from datetime import datetime, timedelta,date
from backtester.src.core.price_module.future_price import FuturePrice
from backtester.src.core.price_module.nearby_price import NearbyPrice
from backtester.src.core.price_module.strategy_price import StrategyPrice
from backtester.src.core.cost_module.tc import TC
from backtester.src.core.utils.utils import get_unique_dicts
from backtester.src.core.utils.global_params_helper import GLOBALPARAMS
from commodity.commodity import commodity_helper


class Backtester:
    def __init__(self, config, risk_mode =False):
        self.config = config
        self.risk_mode = risk_mode
        GLOBALPARAMS['risk_mode'] = risk_mode
        self.portfolio = Portfolio(config.get('initial_cash', 0),config.get('expiration_cost', False))
        self.holiday_calendar = config.get('holiday_calendar', 'UK_ENG')
        self.trading_calendar = config.get('trading_calendar', self.holiday_calendar)
        self.initial_date = self.config['initial_date']
        self.today = date.today().strftime('%Y-%m-%d')
        self.end_date = self.config.get('end_date', previous_business_day(self.today, self.holiday_calendar)) 
        self.contract_types = config.get('contract_types', ['monthly'])
        self.modules = []
        self.price_modules = []
        self.cost_modules = []
        units_modules = config.get('unitsmodules', [])
        self.have_strategy_udl = 'vol_target_strategy' in str( units_modules)
        GLOBALPARAMS['today']= self.today
        GLOBALPARAMS['path'] = config.get('path', 'C:/Users/yuhang.hou/projects/test_Freight_FM')

        for module in units_modules:
            module_params = module.get('params', {})|self.dates_info
            if module.get('type') == 'simple_rolling':
                self.modules.append( SimpleRolling(module_params))
            if module.get('type') == 'preroll':
                self.modules.append( PreRoll(module_params))
            if module.get('type') == 'dynamic_rolling':
                self.modules.append( DynamicRolling(module_params))
            if module.get('type') == 'var_adjustment':
                self.modules.append( VarAdjustment(module_params))
            if module.get('type') == 'vol_target':
                self.modules.append( VolTarget(module_params))
            if module.get('type') == 'vol_target_strategy':
                self.modules.append( VolTargetStrategy(module_params))
            if module.get('type') == 'notional_match':
                self.modules.append( NotionalMatch(module_params))
            if module.get('type') == 'skew':
                self.modules.append( Skew(module_params))
            if module.get('type') == 'trend':
                self.modules.append( Trend(module_params))
            if module.get('type') == 'ts_trend_cot':
                self.modules.append( TS_Trend_COT(module_params))
            if module.get('type') == 'ts_trend_cot1':
                self.modules.append( TS_Trend_COT1(module_params))
            if module.get('type') == 'ts_trend_kalman_filter':
                self.modules.append( TS_Trend_KalmanFilter(module_params))
            if module.get('type') == 'ts_trend_kalman_filter1':
                self.modules.append( TS_Trend_KalmanFilter1(module_params))
            if module.get('type') == 'ts_trend_kalman_filter_shift':
                self.modules.append( TS_Trend_KalmanFilter_Shift(module_params))
            if module.get('type') == 'ts_trend_kalman_filter_cs':
                self.modules.append( TS_Trend_KalmanFilter_CS(module_params))
            if module.get('type') == 'ts_reversion':
                self.modules.append( TS_Reversion(module_params))
            if module.get('type') == 'ts_reversion_bollinger_rsi':
                self.modules.append( TS_Reversion_Bollinger_RSI(module_params))
            if module.get('type') == 'ts_ml_signal':
                self.modules.append( TS_ML_Signal(module_params))
            if module.get('type') == 'ts_trend_zscore':
                self.modules.append( TS_Trend_KalmanFilter_Zscore(module_params))

        for price_module in config.get('price_modules', []):
            if price_module.get('type') == 'Future':
                self.price_modules.append(FuturePrice(price_module.get('params', {})))
            if price_module.get('type') == 'NearbyFuture':
                params = price_module.get('params', {})   
                self.price_modules.append(NearbyPrice(price_module.get('params', {})))
            if price_module.get('type') == 'Strategy':
                self.price_modules.append(StrategyPrice(price_module.get('params', {})))

        for cost_module in config.get('cost_modules',[]):
            if cost_module['type'] == 'TC':
                self.cost_modules.append(TC(cost_module.get('params',{})))
            else:
                raise ValueError(f"Current cost module is not supported")
        
        self.future_instruments = config.get('future_instruments', [])

        for future in self.future_instruments:
            cmd = commodity_helper.get_commodity(future)
            cmd.get_last_trading_days()                    


    @property
    def dates_info(self):
        return {
            'initial_date': self.initial_date,
            'end_date': self.end_date,
            'holiday_calendar': self.holiday_calendar,
            'trading_calendar': self.trading_calendar
        }
    
    def instrument_on_date(self, date_str):
        """
        Calculate the instruments needed on the given date.
        :param date_str: str, date in 'YYYY-MM-DD' format
        :return: list of instruments
        """
        instruments = []
        for module in self.modules: 
            instruments.extend(module.instrument_on_date(date_str))
            
        return instruments
    
    def instrument_curve(self,start_date, end_date):
        """
        instruments hold for a given period
        """
        dates = []
        for module in self.modules:
            dates.extend(module.risk_dates(start_date, end_date))
        dates = sorted(set(dates))
        
        instruments_on_dates = {}
        prev_instrument = []
        for date_str in dates[::-1]:
            curr_instrument = self.instrument_on_date(date_str)
            total_instrument = prev_instrument + curr_instrument
            instruments_on_dates[date_str] = get_unique_dicts(total_instrument)
            prev_instrument = curr_instrument
        return instruments_on_dates

    def risk_curve(self, instruments_on_dates ):
        """
        Calculate the risk curve for a given instrument curve.
        
        :param instruments_on_dates: dict, mapping of date to list of instruments
        :return: dict, mapping of date to risk values
        """
        risks_on_dates = {}
        for p_module in self.price_modules:
            risks_on_dates = p_module.risks_on_dates(instruments_on_dates, risks_on_dates)
        return risks_on_dates

    def run(self):
        # Implement the backtesting logic here
        """
        Run the backtesting logic for the given configuration.
        
        :return: Portfolio object with the final portfolio state
        """
        if GLOBALPARAMS['risk_mode']:
            dates = business_days_between(self.initial_date, self.today,self.holiday_calendar)
            instruments_on_dates = self.instrument_curve(self.initial_date, self.today)
        else:
            dates = business_days_between(self.initial_date, self.end_date,self.holiday_calendar)
            instruments_on_dates = self.instrument_curve(self.initial_date, self.end_date)
            
        # Iterate through each date in the trading calendar
        risks_on_dates = self.risk_curve(instruments_on_dates)
        self.portfolio.add_risks_curve(risks_on_dates)
        # print(GLOBALPARAMS['risk_mode'], self.have_strategy_udl,risks_on_dates.keys())
        # return()
        for _date in dates:
            self.portfolio.portfolio_state.trades_for_date = []
            if len( self.portfolio.portfolio_state.pending_trades) != 0:
            # always do pending trades first
            # this is important for the logic of the portfolio   
                self.portfolio.portfolio_state.trades_for_date =  self.portfolio.portfolio_state.pending_trades +  self.portfolio.portfolio_state.trades_for_date
                self.portfolio.portfolio_state.pending_trades = []
            for u_module in self.modules:
                trades = u_module.trades_on_date(_date, self.portfolio, risks_on_dates)
                if trades:
                    self.portfolio.add_trades(trades)
            # print(date, [trade.__dict__ for trade in trades])
            for c_module in self.cost_modules:
                
                trades = c_module.trades_on_date(_date, self.portfolio, risks_on_dates)
                if trades:
                    # print(date,1)
                    self.portfolio.add_trades(trades)          
            self.portfolio.age_trades(_date)
            self.store_data(_date)
            # print(date, [trade.__dict__ for trade in self.portfolio.portfolio_state.trades_for_date])
            # Update portfolio state
        return self.portfolio
    

    def store_data(self,_date):
        for module in self.modules:
            if _date in module.data_to_store:
                if 'units_module' in self.portfolio.history[_date]:
                    self.portfolio.history[_date]['units_module'] .update(module.data_to_store[_date])
                else: 
                    self.portfolio.history[_date]['units_module'] = module.data_to_store[_date]


