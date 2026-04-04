from backtester.src.core.price_module.tradables import Future
from backtester.src.core.price_module.utils import reshape_instruments_on_dates

class FuturePrice():
    def __init__(self,params = {}):
        """
        Initialize the FuturePrice object with a DataFrame of prices and contract months.
        """
        self.params = params

    def risks_on_dates(self, instruments_on_dates, risks_on_dates = {}):
        """
        Get the price for a specific date and contract month.
        
        :param date: Date in 'YYYY-MM-DD' format
        :param contract_month: Contract month to get the price for
        :return: Price for the specified date and contract month, or NaN if not available
        """
        
        instruments_maps = reshape_instruments_on_dates(instruments_on_dates)
        instruments_history_curves = self.instruments_history_curves(instruments_maps)
        
        for date, instruments in instruments_on_dates.items():
            
            risks_on_dates[date] = {}
            for instrument in instruments:
                i_type = instrument['type']
                if i_type!='Future': continue
                ticker = instrument['ticker']
                cols = instrument.get('cols', ['close'])
                # print(date, ticker)
                risks_on_dates[date][ticker] = instruments_history_curves[i_type][ticker].get_data(date, cols)
                # print( risks_on_dates[date][ticker] )
        return risks_on_dates
    
    def instruments_history_curves(self, instruments_maps):
        """
        Get the historical curve of instruments.
        
        :param instruments_maps: Dictionary mapping instrument types to their tickers
        :return: Dictionary with instrument types as keys and their historical prices as values
        """
        instruments_history = {}
        for type, tickers in instruments_maps.items():
            instruments_history[type] = {}
            if type == 'Future':
                for ticker in tickers:
                    future = Future(ticker)
                    instruments_history[type][ticker] = future
            else:
                continue
           
        return instruments_history