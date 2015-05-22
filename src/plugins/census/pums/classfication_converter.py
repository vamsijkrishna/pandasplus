import os
import pandas as pd

# soc_map = pd.read_csv('data/occ02to10.csv', converters={"OCCP02": str, "OCCP10": str})
# occ1012 = pd.read_csv('data/occ10to12.csv', converters={"OCCP10": str, "OCCP12": str})
COL_RATE = "Total Conversion Rate"
SOC_00 = "socp00"
SOC_10 = "socp10"

NAICS_02 = "NAICSP02"
NAICS_07 = "NAICSP07"

MALE_VAL = 1
FEMALE_VAL = 2

HS_VAL = 20
BA_VAL = 21
ADV_VAL = 22

data_dir = os.path.dirname(__file__)

def get_path(target):
    return os.path.join(data_dir, target)

soc_direct_map = pd.read_csv(get_path('data/SOC_00_to_10_direct.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_direct_map = soc_direct_map.to_dict(orient="records")
soc_direct_map = {x[SOC_00] : x[SOC_10] for x in soc_direct_map}

naics_direct_map = pd.read_csv(get_path('data/NAICS_02_to_07_direct.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_direct_map = naics_direct_map.to_dict(orient="records")
naics_direct_map = {x[NAICS_02] : x[NAICS_07] for x in naics_direct_map}

soc_hs_m_map = pd.read_csv(get_path('data/SOC_00_to_10_HS_M.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_hs_f_map = pd.read_csv(get_path('data/SOC_00_to_10_HS_F.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_ba_m_map = pd.read_csv(get_path('data/SOC_00_to_10_BA_M.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_ba_f_map = pd.read_csv(get_path('data/SOC_00_to_10_BA_F.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_adv_m_map = pd.read_csv(get_path('data/SOC_00_to_10_ADV_M.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_adv_f_map = pd.read_csv(get_path('data/SOC_00_to_10_ADV_F.csv'), converters={"SOCP00": str, "SOCP10": str})

naics_hs_m_map = pd.read_csv(get_path('data/NAICS_02_to_07_HS_M.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_hs_f_map = pd.read_csv(get_path('data/NAICS_02_to_07_HS_F.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_ba_m_map = pd.read_csv(get_path('data/NAICS_02_to_07_BA_M.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_ba_f_map = pd.read_csv(get_path('data/NAICS_02_to_07_BA_F.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_adv_m_map = pd.read_csv(get_path('data/NAICS_02_to_07_ADV_M.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_adv_f_map = pd.read_csv(get_path('data/NAICS_02_to_07_ADV_F.csv'), converters={"NAICSP02": str, "NAICSP07": str})

xforms = {SOC_00: SOC_10, NAICS_02: NAICS_07}

def _transform(df, trans_df, on_col, value_col):
    df = pd.merge(df, trans_df, on=on_col, how="left")
    df.loc[df[COL_RATE].isnull(), xforms[on_col]] = df[on_col]
    df.loc[df[COL_RATE].isnull(), COL_RATE] = 1
    df[value_col] = df[value_col] * df[COL_RATE]
    return df.drop([on_col, COL_RATE], axis=1)

# def occ_00_to_10(df, occ_trans_df, value_col):
    # return occ_trans(df, occ_trans_df, SOC_00, value_col)

def _convert(df, value_col, start_col):
    ''' Use conversion tables to transform across classifications '''
    if not start_col in df.columns:
        print start_col, "Not in", df.columns
        return df

    if start_col == SOC_00:
        direct_map = soc_direct_map
        hs_m_map = soc_hs_m_map
        hs_f_map = soc_hs_f_map
        ba_m_map = soc_ba_m_map
        ba_f_map = soc_ba_f_map
        adv_m_map = soc_adv_m_map
        adv_f_map = soc_adv_f_map
    else:
        direct_map = naics_direct_map
        hs_m_map = naics_hs_m_map
        hs_f_map = naics_hs_f_map
        ba_m_map = naics_ba_m_map
        ba_f_map = naics_ba_f_map
        adv_m_map = naics_adv_m_map
        adv_f_map = naics_adv_f_map

    # -- First apply direct transformation then split into groups
    df.loc[df[start_col].isin(direct_map.keys()), start_col] = df[start_col].map(soc_direct_map)

    # -- Intelligently split the dataframe into six parts based on gender & edu
    HS = (df.SCHL <= HS_VAL)
    BA = (df.SCHL == BA_VAL)
    ADV = (df.SCHL >= ADV_VAL)
    MALE = (df.SEX == MALE_VAL)
    FEMALE = (df.SEX == FEMALE_VAL)
    
    HS_MALE = HS & MALE
    HS_FEMALE = HS & FEMALE
    BA_MALE = BA & MALE
    BA_FEMALE = BA & FEMALE
    ADV_MALE = ADV & MALE
    ADV_FEMALE = ADV & FEMALE

    EVERYTHING_ELSE = ~HS_MALE & ~HS_FEMALE & ~BA_MALE & ~BA_FEMALE & ~ADV_MALE & ~ADV_FEMALE
    
    if df[EVERYTHING_ELSE][value_col].count() > 0:
        raise Exception("*** ERROR! Unaccounted for people")

    rules = [ (HS_MALE, hs_m_map), (HS_FEMALE, hs_f_map),
                (BA_MALE, ba_m_map), (BA_FEMALE, ba_f_map),
                (ADV_MALE, adv_m_map), (ADV_FEMALE, adv_f_map)]

    new_df = pd.DataFrame()
    for (rule, rule_map) in rules:
        tmpdf = df[rule].copy()
        tmpdf = _transform(tmpdf, rule_map, start_col, value_col)
        new_df = pd.concat([new_df, tmpdf])

    return new_df

def occ_convert(df, value_col):
    return _convert(df, value_col, SOC_00)

def naics_convert(df, value_col):
    return _convert(df, value_col, NAICS_02)

if __name__ == '__main__':
    moi = pd.DataFrame({"x": [100, 100], NAICS_02: ["51M", "51M"], "SEX":  [1, 2], "SCHL": [12, 12]})
    print "Original:"
    print moi.head()
    print
    res = occ_convert(moi, "x")
    res = naics_convert(res, "x")
    print "Converted:"
    print res
