# module used to calculate transaction costs for futures trading
from backtester_full.src.core.portfolio import Trade
from commodity.commodconfig import COMMODINFO
from backtester_full.src.core.utils.global_params_helper import GLOBALPARAMS


class TC:
    def __init__(self, params):
        self.params = params

    def trades_on_date(self, date, portfolio, risks_on_dates):
        trades = portfolio.portfolio_state.trades_for_date
        if self.params['type'] == 'future':
            tchandler = TCFuture(self.params)
            return tchandler.trades_on_date(date, trades,risks_on_dates) 
        else:
            raise ValueError('Unsupported tc type')

class TCFuture:
    def __init__(self,params):
        self.params = params
        self.rate_map = {item['symbol']:item['rate_type'] for item in self.params['asset_table']}

    def rates_on_date(self, date, risks_on_dates):
        rates_on_date = {}
        for row in self.params["asset_table"]:
            if row['rate_type'] == 'fixed':
                rates_on_date[row['symbol']] = {
                    'long': row.get('long_rate', row.get('rate', 0)),
                    'short': row.get('short_rate', row.get('long_rate', row.get('rate', 0)))
                }
            elif row['rate_type'] == 'cash':
                rates_on_date[row['symbol']] = {
                    'long': row.get('long_rate', row.get('rate', 0)),
                    'short': row.get('short_rate', row.get('long_rate', row.get('rate', 0)))
                }
            else:
                raise ValueError(f"Unsupported rate type {row['rate_type']}")
        return rates_on_date 

    def trades_on_date(self, date, trades, risks_on_dates):
        # print(date, risks_on_dates[date])
        rates = self.rates_on_date(date, risks_on_dates)
        tc_trades = []
        tc_netted = {}
        #adding a netting module here. 
        for trade in trades:
            
            if trade.date != date:
                continue
            if trade.date.strftime('%Y-%m-%d') == GLOBALPARAMS['today']: 
                continue
        
            if trade.underlier_type == 'future':
                symbol = trade.symbol
                if symbol not in tc_netted:
                    tc_netted[symbol] = {}
                if trade.ticker not in tc_netted[symbol]:
                    tc_netted[symbol][trade.ticker] = 0
                if self.rate_map[symbol] == 'fixed':       
                    # print(date, trade.ticker, trade.size)             
                    price = risks_on_dates[date][trade.ticker]['close']
                    # we might need to change here when we are having a big basket
                    if trade.size > 0:
                        tc_netted[symbol][trade.ticker] = -trade.size*price*rates[symbol]['long']
                    else:
                        tc_netted[symbol][trade.ticker] = trade.size*price*rates[symbol]['short']
                elif self.rate_map[symbol] == 'cash':
                    if trade.size > 0:
                        tc_netted[symbol][trade.ticker] = -trade.size*rates[symbol]['long']
                    else:
                        tc_netted[symbol][trade.ticker] = trade.size*rates[symbol]['short']
                else:
                    raise ValueError("Unsupported rate type")
        for symbol, data in tc_netted.items():
            for ticker, size in data.items():
                if size != 0:
                    denominate = COMMODINFO[symbol]['currency']
                    trade = Trade(
                        denominate,
                        date,
                        -abs(size),
                        denominate,
                        'TC',
                        trade_type= 'additive',
                        underlier_type='future'
                    )
                    tc_trades.append(trade)
        return tc_trades


