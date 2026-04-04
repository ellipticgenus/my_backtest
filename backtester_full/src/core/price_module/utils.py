from collections import defaultdict

def reshape_instruments_on_dates(nested_dict):
    """
    Reorganizes a date->instruments dictionary into instrument->tickers format.
    
    Args:
        nested_dict: {
            date1: [
                {"ticker": "A", "type": "stock"},
                {"ticker": "B", "type": "bond"}
            ],
            date2: [
                {"ticker": "A", "type": "stock"},
                {"ticker": "C", "type": "future"}
            ]
        }
        
    Returns:
        {
            "stock": ["A"],
            "bond": ["B"],
            "future": ["C"]
        }
    """
    instrument_map = defaultdict(list)
    
    for date, instruments in nested_dict.items():
        for instrument in instruments:
            ticker = instrument["ticker"]
            inst_type = instrument["type"]
            
            # Avoid duplicate tickers for the same instrument type
            if ticker not in instrument_map[inst_type]:
                instrument_map[inst_type].append(ticker)
    
    return dict(instrument_map)