import pandas as pd
import numpy as np
from src.pipeline.consts import COLUMNS
from src.pipeline.exceptions import InvalidSettingException
from src.plugins.census.pums.classfication_converter import naics_convert, occ_convert
from src.plugins.census.pums import statistics
from src.plugins.census.pums import puma_converter
from src.pipeline.consts import MODE
weight_col = "PWGTP"
SUMLEVEL = 'sumlevel'
GEO = 'geo_id'
PUMA = 'puma'
STATE = 'state'
NATION = 'nation'

def lookup_sumlevel_by_name(name):
    sumlevel_map = {PUMA : "79500US", STATE: "04000US", NATION: "01000US"}
    if name in sumlevel_map:
        return sumlevel_map[name]
    raise InvalidSettingException("Unknown sumlevel: {}".format(name))

def process(df, settings=None, pk=[], var_map={}):
    print "SCHL" in df.columns
    print "AGEP" in df.columns, " HAS AGEP?"
    print df.columns

    df = _convert_pumas(df, pk, var_map)
    df = _prepare(df, settings, pk)
    df, pk = _post_process(df, settings, pk, var_map=var_map)
    df = statistics.compute(df, settings, pk)

    return df

def _post_process(df, settings, pk, var_map={}):
    print "PK=", pk
    needs_naics = "NAICSP12" in pk
    needs_occ = "SOCP12" in pk
    if needs_naics:
        df = naics_convert(df, var_map)
    if needs_occ:
        df = occ_convert(df, var_map)
    return df, pk

def _replace(tdf, col, val1, val2):
    if col in tdf.columns:
        tdf.occp02 = tdf.occp02.replace(val1, val2)
    return tdf

def _prepare(df, settings=None, pk=[]):
    # -- FIRST filter out anyone under the age of 16
    #    and any wage not greater than 0.
    if not MODE in settings or settings[MODE] == statistics.PERSON:
        df = df[(df.AGEP >= 16) & (df.WAGP > 0)].copy()

    sumlevel_name = settings[SUMLEVEL] if SUMLEVEL in settings else 'puma'
    sumlevel = lookup_sumlevel_by_name(sumlevel_name)

    df[GEO] = None

    to_replace = ["naicsp02", "naicsp07", "naicsp12", "socp00", "socp10"]
    for col in to_replace:
        if col in df.columns:
            df.loc[df[col].isin(['N.A.////', 'N.A.//', 'N.A.']), col] = np.nan

    df.loc[df.ST.str.len() == 0, 'ST'] = None
    df.loc[df.ST.notnull(), GEO] = sumlevel + df.ST.astype(int).astype(str).str.zfill(2)
    df.loc[df.ST.isnull(), 'ST'] = sumlevel + 'XX'

    # df.loc[df.POWPUMA.str.len() == 0, 'POWPUMA'] = None
    if sumlevel_name == PUMA:
        df.loc[df.PUMA.notnull(), GEO] = df[GEO] + df.PUMA.astype(int).astype(str).str.zfill(5)
        df.loc[df.PUMA.isnull(), GEO] = df[GEO] + 'XXXXX'
    elif sumlevel_name == NATION:
        df[GEO] = sumlevel

    return df

def _convert_pumas(df, pk, var_map):
    # TODO: only need to do this if we have a geography in the PK
    print "Converting PUMAs..."
    if "PUMA00" in df.columns and "PUMA10" in df.columns:
        df = puma_converter.update_puma(df, "PUMA00")
    elif "PUMA" in df.columns and not "PUMA00" in df.columns and not "PUMA10" in df.columns:
        # -- only need to run update IFF year < 2012
        if int(var_map["year"]) < 2012:
            df = puma_converter.update_puma(df, "PUMA00")
        else:
            print "NO PUMA conversion required...simply renaming column..."
            df.rename(columns={"PUMA": "PUMA10"}, inplace=True)
    else:
        raise Exception("Invalid PUMA structure.")
    return df
