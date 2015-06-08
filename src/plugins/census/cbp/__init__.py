import pandas as pd
import numpy as np

from src.pipeline.consts import MODE

def process(df, settings=None, pk=[], var_map={}):
    COUNTY = 'county'
    STATE = 'state'

    mode = COUNTY if not MODE in settings else settings[MODE]
    if mode == COUNTY:
        df['geo'] = df['fipstate'].astype(str).str.zfill(2) + df['fipscty'].astype(str).str.zfill(3)
    else:
        df['geo'] = df['fipstate'].str.zfill(2)

    if int(var_map['year']) < 100:
        df['year'] = df['year'].astype(int) + 2000
    df['naics'] = df['naics'].str.replace(r'\/|-', '')
    df.loc[df.naics.str.len() == 0, 'naics'] = 0
    df['empflag'] = df['empflag'].astype(str)
    return df