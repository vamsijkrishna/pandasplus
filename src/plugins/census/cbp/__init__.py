import pandas as pd
import numpy as np

from src.pipeline.consts import MODE

def get_year(var_map):
    yr = int(var_map['year'])
    return yr + 2000 if yr < 100 else yr

def process(df, settings=None, pk=[], var_map={}):
    COUNTY = 'county'
    STATE = 'state'
    NATION = 'nation'
    MSA = 'msa'

    mode = COUNTY if not MODE in settings else settings[MODE]
    
    df.columns = [col.lower() for col in df.columns]
    year = get_year(var_map)

    if year <= 2006:
        df['emp_nf'] = None
        df['ap_nf'] = None

    if mode == COUNTY:
        df['geo'] = "05000US" + df['fipstate'].astype(str).str.zfill(2) + df['fipscty'].astype(str).str.zfill(3)
    elif mode == MSA:
        df['geo'] = "31000US" + df['msa'].astype(str).str.zfill(5)
    else:
        if (mode == STATE and year >= 2010) or (mode == NATION and year >= 2008):
            df = df[df['lfo'] == '-'].copy()
        if mode == STATE:
            df['geo'] = '04000US' + df['fipstate'].str.zfill(2)
        elif mode == NATION:
            df['geo'] = '01000US'

    if int(var_map['year']) < 100:
        df['year'] = df['year'].astype(int) + 2000
    df['naics'] = df['naics'].str.replace(r'\/|-', '')
    df.loc[df.naics.str.len() == 0, 'naics'] = 0
    # df.loc[df['empflag'].isnull(), 'empflag'] = None
    df.loc[df['empflag'].notnull(), 'empflag'] = df['empflag'].astype(str)

    return df