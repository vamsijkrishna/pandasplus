weight_col = "PWGTP"
import pandas as pd
import numpy as np
from src.pipeline.consts import COLUMNS
#, VALUE

from src.plugins.census.pums.classfication_converter import naics_convert, occ_convert
from src.plugins.census.pums import statistics
from src.plugins.census.pums import puma_converter

def process(df, settings=None, pk=[], var_map={}):
    print "SCHL" in df.columns


    df = _prepare(df, settings, pk)
    df = _convert_pumas(df, pk, var_map)
    df, pk = _post_process(df, settings, pk, var_map=var_map)
    df = statistics.compute(df, settings, pk)

    return df

def _post_process(df, settings, pk, var_map={}):
    print "PK=", pk

    df = naics_convert(df, var_map)
    df = occ_convert(df, var_map)
    # TODO: PK update
    # pk[pk.index(soc00)] = "socp10"
    return df, pk

def _replace(tdf, col, val1, val2):
    if col in tdf.columns:
        tdf.occp02 = tdf.occp02.replace(val1, val2)
    return tdf

def _prepare(df, settings=None, pk=[]):
    # -- FIRST filter out anyone under the age of 16
    #    and any wage not greater than 0.
    df = df[(df.AGEP >= 16) & (df.WAGP > 0)].copy()
    to_replace = ["naicsp02", "naicsp07", "socp00", "socp10"]
    for col in to_replace:
        df.loc[df[col].isin(['N.A.////', 'N.A.//', 'N.A.']), col] = np.nan

    df.loc[df.ST.str.len() == 0, 'ST'] = None
    df.loc[df.ST.notnull(), 'ST'] = df[df.ST.notnull()].ST.astype(int).astype(str).str.zfill(2)
    df.loc[df.ST.isnull(), 'ST'] = 'XX'

    # df.loc[df.POWPUMA.str.len() == 0, 'POWPUMA'] = None
    df.loc[df.PUMA.notnull(), 'PUMA'] = df[df.PUMA.notnull()].PUMA.astype(int).astype(str).str.zfill(5)
    df.loc[df.PUMA.isnull(), 'PUMA'] = 'XXXXX'
    # if "geo" in pk:
    return df

def _convert_pumas(df, pk, var_map):
    # TODO: only need to do this if we have a geography in the PK
    if "PUMA00" in df.columns and "PUMA10":
        raise Exception("TODO!")
    elif "PUMA" in df.columns and not "PUMA00" in df.columns and not "PUMA10" in df.columns:
        # -- only need to run update IFF year < 2012
        if int(var_map["year"]) < 2012:
            # df = puma_converter.update_puma(df, "PUMA")
            df['geo'] = df.ST + df.PUMA
            df = puma_converter.update_puma(df, "geo")
        else:
            print "NO PUMA conversion required...simply renaming column..."
            df.rename(columns={"POWPUMA": "POWPUMA10"}, inplace=True)
    else:
        raise Exception("Invalid PUMA structure.")
    return df
