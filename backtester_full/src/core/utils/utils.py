import json
import pickle
import pandas as pd
from commodity.commodconfig import COMMODINFO

def get_unique_dicts(list_of_dicts):
    seen = set()
    unique_dicts = []
    for d in list_of_dicts:
        # Convert dict to sorted JSON string for consistent comparison
        dict_str = json.dumps(d, sort_keys=True)
        if dict_str not in seen:
            seen.add(dict_str)
            unique_dicts.append(d)
    return unique_dicts

def load_and_process(index, risk_mode = False):
    if risk_mode:
        risk_tail = '_risk'
    else:
        risk_tail = ''
    with open(f'../backtester/strategy/backtest/{index}{risk_tail}.pkl', 'rb') as f:  # Note 'rb' mode
        history1 = pickle.load(f)

    curves = []
    for date,data in history1.items():
        temp = {}
        # print(data)
        temp['date'] = date
        trades = data['trades_for_date']
        temp['tc'] = sum([trade['size'] for trade in trades if trade['source']=='TC'])
        temp['ec'] = sum([trade['size'] for trade in trades if trade['source']=='ExpirationCost'])
        temp['level'] = data['balance']
        curves.append(temp)
    final = pd.DataFrame(curves)
    final['tc_cumsum'] = -final['tc'].cumsum() - final['ec'].cumsum()
    final.set_index('date',inplace=True)
    return final


def partition_ticker(ticker):
    """
    Take a ticker string and break it down into the underlying symbol, month code, and year.
    ticker : str  The ticker string to be parsed
    Returns: Containing the underlying symbol, month code, and year of the ticker
    """
    match = ''
    for s in COMMODINFO:
        if ticker.startswith(s):
            match = s
            break
    if len(match):
        month = ticker[len(match):-2]
        year = 2000 + int( ticker[-2:])
        return match, month, year
    else:
        return match, '', 2000