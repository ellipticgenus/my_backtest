CMD_TRADING_LIST = {
    'CBT': ['S','C','SM','BO','W'],
    'SGX': []
}


EXPIRATION_RULES_1 = {
'F': '0m',
'G': '0m',
'H': '0m',
'J': '0m',
'K': '0m',
'M': '0m',
'N': '0m',
'Q': '0m',
'U': '0m',
'V': '0m',
'X': '0m',
'Z': '0X+1B-1B',
}

RULES_2 = { k:'-1m' for k in 'FGHJKMNQUVXZ' }

RULES_3 = {
    'F': '-1m+10B',
    'G': '-1m+10B',
    'H': '-1m+10B',
    'J': '-1m+10B',
    'K': '-1m+10B',
    'M': '-1m+10B',
    'N': '-1m+10B',
    'Q': '-1m+10B',
    'U': '-1m+10B',
    'V': '-1m+10B',
    'X': '-1m+10B',
    'Z': '-1m+10B',
}
RULES_4 = {
    'F': '0m-3B',
    'G': '0m-3B',
    'H': '0m-3B',
    'J': '0m-3B',
    'K': '0m-3B',
    'M': '0m-3B',
    'N': '0m-3B',
    'Q': '0m-3B',
    'U': '0m-3B',
    'V': '0m-3B',
    'X': '0m-3B',
    'Z': '0m-3B',
}

RULES_5 = { k:'-1m+1B-5B' for k in 'FGHJKMNQUVXZ' }

# expiration date
RULES_6 = { k:'-1m+15d+1B-1B' for k in 'FGHJKMNQUVXZ' }
RULES_7 = { k:'-1m+17d+1B-1B' for k in 'FGHJKMNQUVXZ' }

CRUSH_DECOMPOSITION  = {
    'J':{'S':'F', 'SM':'F', 'BO':'F'},
    'H':{'S':'H', 'SM':'H', 'BO':'H'},
    'K':{'S':'K', 'SM':'K', 'BO':'K'},
    'N':{'S':'N', 'SM':'N', 'BO':'N'},
    'Q':{'S':'Q', 'SM':'Q', 'BO':'Q'},
    'U':{'S':'U', 'SM':'U', 'BO':'U'},
    'V':{'S':'X', 'SM':'V', 'BO':'V'},
    'Z':{'S':'F*', 'SM':'Z', 'BO':'Z'},
}

COMMODINFO = {
    'AK': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FHKNUX',
        'liquid_expiration': 'FHKNUX', # from function
        'first_notice_rule': RULES_3,
    },
    'BP': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FGHJKMNQUVXZ',
        'liquid_expiration': 'FGHJKMNQUVXZ', #   from function
        'first_notice_rule': RULES_3,
    },
    'AE': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FHKMNUXZ',
        'liquid_expiration': 'FKUX', #   from function
        'first_notice_rule': RULES_3,
    },
    'SH': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FHKMNUXZ',
        'liquid_expiration': 'FKU', #  from function
        'first_notice_rule': RULES_3,
    },
    'AC': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FHKNUX',
        'liquid_expiration': 'FHKNUX', #   from function
        'first_notice_rule': RULES_3,
    },
    'PAL': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FGHJKMNQUVXZ',
        'liquid_expiration': 'FKU', #   from function
        'first_notice_rule': RULES_3,
    },
    'LHD': {
        'holiday': 'DCE',
        'currency': 'CNY',
        'expiration_rule': None,
        'valid_expiration': 'FHKNUX',
        'liquid_expiration': 'FHKNUX', # from function
        'first_notice_rule': RULES_4,
    },

    'S': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'FHKNQUX',
        'liquid_expiration': 'FHKNX', # follow bcom
        'first_notice_rule': RULES_2,
    },
    'SR': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'JHKNQUVZ',
        'liquid_expiration': 'JHKNQUVZ', 
        'crush_decomposition': CRUSH_DECOMPOSITION,
        'first_notice_rule': RULES_2,
    },
    'BO': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'FHKNQUVZ',
        'liquid_expiration': 'FHKNZ',
        'first_notice_rule': RULES_2,
    },
    'SM': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'FHKNQUVZ',
        'liquid_expiration': 'FHKNZ',
        'first_notice_rule': RULES_2,
    },
    'C': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'HKNUZ',
        'liquid_expiration': 'HKNUZ',
        'first_notice_rule': RULES_2,
    },
    'W': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'HKNUZ',
        'liquid_expiration': 'HKNUZ',
        'first_notice_rule': RULES_2,
    },
    'KW': {
        'holiday': 'CBT',
        'currency': 'USD',
        'expiration_rule': RULES_6,
        'valid_expiration': 'HKNUZ',
        'liquid_expiration': 'HKNUZ',
        'first_notice_rule': RULES_2,
    },

    'CT': {
        'holiday': 'ICE',
        'currency': 'USD',
        'expiration_rule': RULES_7,
        'valid_expiration': 'HKNUZ',
        'liquid_expiration': 'HKNUZ',
        'first_notice_rule': RULES_5,
    }
}


NUM_TO_MONTH = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}

MONTH_TO_NUM  = {  'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12 }

MONTH_PREFIX ={
    'monthly': '',
    'quarterly':'Q',
    'yearly':'Y'
}