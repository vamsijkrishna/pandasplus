weight_col = "PWGTP"
import pandas as pd
import numpy as np
from src.pipeline.consts import COLUMNS

def process(df, settings=None, pk=[]):
    df = _prepare(df, settings)
    return _calculate(df, settings, pk)

def _replace(tdf, col, val1, val2):
    if col in tdf.columns:
        tdf.occp02 = tdf.occp02.replace(val1, val2)
    return tdf

def _prepare(df, settings=None):
    to_replace = ["naicsp02", "naicsp07", "socp00", "socp10"]
    for col in to_replace:
        df.loc[df[col].isin(['N.A.//', 'N.A.']), col] = np.nan
    
    return df

def _calculate(df, settings, pk):
    df["value"] = df[ weight_col ]
    df = df[df['POWSP'] == 39].copy()
    df['geo'] = -1
    df.loc[df['PUMA'] > 0, 'geo'] = df['PUMA']

    cols = settings[COLUMNS]
    # df.geo = str(state) + df.geo.astype(str).str.zfill(5)

    # -- Group by the PK and add up the person weights
    aggs = { weight_col + str(i) : pd.Series.sum for i in range(1,81) }
    for col in cols:
        aggs[col] = pd.Series.sum
    aggs["value"] = pd.Series.sum
    df = df.groupby(pk).agg(aggs)
    df = df.reset_index()
    
    ''' Compute Margin of Error '''
    rws = ["rw_" + str(i) for i in range(1, 81) ]
    for i in range(1, 81):
        df["rw_" + str(i)] = (df["value"] - df[weight_col + str(i)]) ** 2

    for i in range(1, 81):
        df["variance"] = df[rws].sum(axis=1) * (4.0 / 80)
        df["se"] = df["variance"] ** 0.5
        df["moe"] = df["se"] * 1.645 # 90% confidence interval

    pwgtps = [weight_col + str(i) for i in range(1,81)]
    df.drop(["variance", "se"] + rws + pwgtps, axis=1, inplace=True)

    return df