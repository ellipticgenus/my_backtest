from backtester_full.src.core.utils.utils import partition_ticker
from commodity.commodity import commodity_helper
from commodity.commodconfig import COMMODINFO
import pandas as pd
from datetime import datetime, timedelta,date

class Trade:
    def __init__(self, 
                 ticker, 
                 _date, 
                 size, 
                 denominate='USD', 
                 source=None, 
                 trade_type = 'neutral', 
                 extra_info=None, 
                 underlier_type = 'future',
                 symbol = ''):
        """
        Initialize a trade with the given parameters.
        :param ticker: str, the ticker symbol of the asset being traded
        :param date: str, date of the trade in 'YYYY-MM-DD' format
        :param size: float, size of the trade (positive for buy, negative for sell)
        :param denominate: str, denomination of the trade (default is 'usd')
        :param source: str, optional, source of the trade (e.g., 'manual', 'algorithmic')
        :param trade_type: str, type of the trade (e.g., 'neutral', 'additive')
        :param extra_info: dict, optional, additional information about the trade
        :param underlier_type: string, optional, the type of the ticker that is  traded
        """
        self.ticker = ticker
        self.date = _date
        self.size = size
        self.denominate = denominate  # Assuming all trades are denominated in USD
        self.source = source
        self.type = trade_type
        self.extra_info = extra_info if extra_info is not None else {}
        self.underlier_type = underlier_type
        self.symbol = symbol
    

class PortfolioState:
    def __init__(self, initial_balance=100000, expiration_cost = False):
        """
        Initialize the portfolio with a given initial balance.
        :param initial_balance: float, initial balance of the portfolio
               positions: dict, mapping of ticker to position size
               history: dict, contains, position, level of underlier, on date.               
        """

        self.balance = initial_balance
        self.trades_for_date = []
        self.positions = {'USD':initial_balance}
        self.pending_trades = []
        self.risk_curve = {}
        self.expiration_cost = expiration_cost 
        self.commodities = {}

    def add_trades(self, trades):
        """
        Add trades to the portfolio.
        """
        for trade in trades:
            if trade.size!=0:
                self.trades_for_date.append(trade)

    def add_risk_curve(self, risk_curve):
        """
        Add underlier level to the portfolio.
        :param level: float, the level of the underlier
        """
        self.risk_curve = risk_curve

    def age_trades(self, _date):
        """
        Age trades in the portfolio by the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        risk_curve = self.risk_curve
        for trade in self.trades_for_date:
            if trade.date == _date:
                # If the trade is for the current date, process it
                if trade.type == 'neutral':
                    # print(date, trade.ticker, trade.size)
                    self.positions[trade.ticker] = self.positions.get(trade.ticker, 0) + round(trade.size,10)
                    self.positions['USD'] = self.positions.get('USD', 0) - trade.size * risk_curve[_date][trade.ticker]['close']
                # currently we assume everything is usd
                elif trade.type == "additive":
                    if trade.denominate == 'USD':
                        self.positions['USD'] += trade.size
                    else:
                        raise ValueError('Only support USD at the moment')
                else:
                    raise ValueError('trade type unsupport, please check')

            else:
                self.pending_trades.append(trade)
        
        # handle expiration trades
        # previous code is added specifically for freight. now its should be gone
        for ticker, size in self.positions.items():   
            symbol = partition_ticker(ticker)[0]
            if symbol:
                cmd = commodity_helper.get_commodity(symbol)
                if _date == pd.to_datetime(cmd.last_trading_day(ticker[len(symbol):])):
                    raise ValueError('Last trading day should never be touched')
                    self.positions[ticker] = 0
                    self.positions['USD'] = self.positions.get('USD', 0) + size * risk_curve[_date][ticker]['close']
                    #expiration cost 
                    if self.expiration_cost:
                        self.positions["USD"] = self.positions.get('USD', 0) - abs(size)*COMMODINFO[symbol]['expiration_cost']
                        denominate = COMMODINFO[symbol]['currency']
                        self.trades_for_date.append(Trade(
                            denominate,
                            _date,
                            - abs(size)*COMMODINFO[symbol]['expiration_cost'],
                            denominate,
                            'ExpirationCost',
                            trade_type= 'additive',
                            underlier_type='cash'
                        ))
                    self.trades_for_date.append(Trade(ticker, 
                               _date, 
                               -size, 
                               COMMODINFO[symbol]['currency'],
                               'Expiration',
                               symbol = symbol))

        # make sure we dont have position with zero quantity.
        self.positions =  {k: v for k, v in self.positions.items() if abs(v) > 1e-10}

        # self.trades_for_date = []
        balance = 0
        # print(date, self.positions)
        for pos, qty in self.positions.items():
            if pos !="USD":
                # print(_date, pos, risk_curve[_date])
                balance += qty*risk_curve[_date][pos]['close']
            else:
                balance += qty
        self.balance = balance
    
    def reset_trades(self, new_trades_for_date):
        self.trades_for_date = new_trades_for_date

    def flip_trades_direction(self):
        """
        Flip the direction of all the trades in the portfolio, i.e. convert buys to sells and vice versa.
        :return: None

        only use it when you know what you are doing
        """
        new_trades_for_date = []
        for trade in self.trades_for_date:
            trade.size = -trade.size
            new_trades_for_date.append(trade)
        self.trades_for_date = new_trades_for_date

    def adjust_all_trades_size(self, _size_map):
        """
        Flip the direction of all the trades in the portfolio, i.e. convert buys to sells and vice versa.
        :return: None

        only use it when you know what you are doing
        """
        new_trades_for_date = []
 
        for ticker, _ratio in _size_map.items():
            for trade in self.trades_for_date:
                if trade.ticker == ticker:   
                    trade.size = trade.size*_ratio
                    new_trades_for_date.append(trade)
        self.trades_for_date = new_trades_for_date

    def to_nested_dict(self,_date):
        """
        Convert the portfolio state to a nested dictionary.
        :return: dict, nested dictionary representation of the portfolio state
        """
        data =  {
            'date': _date,
            'balance': self.balance,
            'positions': self.positions.copy(),
            'trades_for_date': [trade.__dict__ for trade in self.trades_for_date.copy()],
            'pending_trades': [trade.__dict__ for trade in self.pending_trades.copy()],
            'risk_curve': self.risk_curve.get(_date,{}),
        }
        return data
class Portfolio:
    def __init__(self, initial_balance: float = 100, expiration_cost = False):
        self.portfolio_state = PortfolioState(initial_balance, expiration_cost)
        self.history = {}
        self.risks_curve = {}
    
    def levels(self):
        return [{'date':k, 'level':v["balance"]} for k,v in self.history.items() ]

    def add_trades(self, trades: list):
        """
        Add a trade to the portfolio.
        :param trade: Trade, the trade to be added
        """
        self.portfolio_state.add_trades(trades)

    def add_risks_curve(self, risk_curve: dict):
        """
        Add underlier levels to the portfolio for a specific date.
        :param date: str, date in 'YYYY-MM-DD' format
        :param levels: dict, mapping of ticker to level of the underlier
        """
        self.portfolio_state.add_risk_curve( risk_curve)

    def age_trades(self, _date):
        """
        Age trades in the portfolio by the given date.
        :param date: str, date in 'YYYY-MM-DD' format
        """
        if _date.strftime('%Y-%m-%d') != date.today().strftime('%Y-%m-%d'):
            self.portfolio_state.age_trades(_date)
        self.history[_date] = self.portfolio_state.to_nested_dict(_date)
