import os
import pandas as pd
import numpy as np
from numpy.random import choice

COL_RATE = "Total Conversion Rate"
SOC_00 = "socp00"
SOC_10 = "socp10"
SOC_12 = "socp12"

NAICS_02 = "NAICSP02"
NAICS_07 = "NAICSP07"
NAICS_12 = "NAICSP12"

MALE_VAL = 1
FEMALE_VAL = 2

data_dir = os.path.dirname(__file__)

def get_path(target):
    return os.path.join(data_dir, target)

soc_direct_map = pd.read_csv(get_path('data/SOC_00_to_10_direct.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_direct_map = soc_direct_map.to_dict(orient="records")
soc_direct_map = {x[SOC_00] : x[SOC_10] for x in soc_direct_map}

soc12_direct_map = pd.read_csv(get_path('data/SOC_10_to_12_direct.csv'), converters={"SOCP12": str, "SOCP10": str})
soc12_direct_map = soc12_direct_map.to_dict(orient="records")
soc12_direct_map = {x[SOC_10] : x[SOC_12] for x in soc12_direct_map}

naics_direct_map = pd.read_csv(get_path('data/NAICS_02_to_07_direct.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_direct_map = naics_direct_map.to_dict(orient="records")
naics_direct_map = {x[NAICS_02] : x[NAICS_07] for x in naics_direct_map}

soc_hs_m_map = pd.read_csv(get_path('data/SOC_00_to_10_HS_M.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_hs_f_map = pd.read_csv(get_path('data/SOC_00_to_10_HS_F.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_ba_m_map = pd.read_csv(get_path('data/SOC_00_to_10_BA_M.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_ba_f_map = pd.read_csv(get_path('data/SOC_00_to_10_BA_F.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_adv_m_map = pd.read_csv(get_path('data/SOC_00_to_10_ADV_M.csv'), converters={"SOCP00": str, "SOCP10": str})
soc_adv_f_map = pd.read_csv(get_path('data/SOC_00_to_10_ADV_F.csv'), converters={"SOCP00": str, "SOCP10": str})

soc12_hs_m_map = pd.read_csv(get_path('data/SOC_10_to_12_HS_M.csv'), converters={"SOCP12": str, "SOCP10": str})
soc12_hs_f_map = pd.read_csv(get_path('data/SOC_10_to_12_HS_F.csv'), converters={"SOCP12": str, "SOCP10": str})
soc12_ba_m_map = pd.read_csv(get_path('data/SOC_10_to_12_BA_M.csv'), converters={"SOCP12": str, "SOCP10": str})
soc12_ba_f_map = pd.read_csv(get_path('data/SOC_10_to_12_BA_F.csv'), converters={"SOCP12": str, "SOCP10": str})
soc12_adv_m_map = pd.read_csv(get_path('data/SOC_10_to_12_ADV_M.csv'), converters={"SOCP12": str, "SOCP10": str})
soc12_adv_f_map = pd.read_csv(get_path('data/SOC_10_to_12_ADV_F.csv'), converters={"SOCP12": str, "SOCP10": str})


naics_hs_m_map = pd.read_csv(get_path('data/NAICS_02_to_07_HS_M.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_hs_f_map = pd.read_csv(get_path('data/NAICS_02_to_07_HS_F.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_ba_m_map = pd.read_csv(get_path('data/NAICS_02_to_07_BA_M.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_ba_f_map = pd.read_csv(get_path('data/NAICS_02_to_07_BA_F.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_adv_m_map = pd.read_csv(get_path('data/NAICS_02_to_07_ADV_M.csv'), converters={"NAICSP02": str, "NAICSP07": str})
naics_adv_f_map = pd.read_csv(get_path('data/NAICS_02_to_07_ADV_F.csv'), converters={"NAICSP02": str, "NAICSP07": str})

naics_12_all = pd.read_csv(get_path('data/NAICS_07_to_12_ALL.csv'), converters={"NAICSP07": str, "NAICSP12": str})
naics_12_direct_map = pd.read_csv(get_path('data/NAICS_07_to_12_direct.csv'), converters={"NAICSP07": str, "NAICSP12": str})
naics_12_direct_map = naics_12_direct_map.to_dict(orient="records")
naics_12_direct_map = {x[NAICS_07] : x[NAICS_12] for x in naics_12_direct_map}

xforms = {SOC_00: SOC_10, SOC_10: SOC_12, NAICS_02: NAICS_07, NAICS_07: NAICS_12}

def get_soc_mode(var_map):
    year, est = map(int, [var_map["year"], var_map["est"]])
    if year >= 2012:
        return SOC_12
    elif year >= 2009:
        return SOC_10
    else:
        return SOC_00

def get_naics_mode(var_map):
    year = int(var_map["year"])
    est = int(var_map["est"])

    if year in [2005, 2006, 2007]: # N02 
        return NAICS_02
    elif year >= 2008 and year <= 2012: #N07
        return NAICS_07
    elif year > 2012: #N12
        return NAICS_12

    raise Exception("Unknown NAICS year/estimate")

def is_old_school(var_map):
    return (int(var_map["year"]) - int(var_map["est"]) + 1) <= 2007

def randomizer12(code):
    rand_val = np.random.random_sample()
    tmpdf = naics_12_all[naics_12_all[NAICS_07] == code]
    if tmpdf.empty:
        return code
    idx = choice(range(len(tmpdf)), p=tmpdf[COL_RATE].values)
    return tmpdf.iloc[idx][NAICS_12]

def randomizer(code, rule_map, start_col):
    rand_val = np.random.random_sample()

    tmpdf = rule_map[rule_map[start_col] == code]

    if tmpdf.empty:
        return code

    idx = choice(range(len(tmpdf)), p=tmpdf[COL_RATE].values)
    return tmpdf.iloc[idx][xforms[start_col]]

def _convert(df, start_col, school_mode):
    ''' Use conversion tables to transform across classifications '''

    HS_VAL, BA_VAL, ADV_VAL = [20, 21, 22] if not school_mode else [12, 13, 14]

    if not start_col in df.columns:
        raise Exception( start_col, "Not in", df.columns )
        return df

    if start_col == SOC_10:
        direct_map = soc12_direct_map
        hs_m_map = soc12_hs_m_map
        hs_f_map = soc12_hs_f_map
        ba_m_map = soc12_ba_m_map
        ba_f_map = soc12_ba_f_map
        adv_m_map = soc12_adv_m_map
        adv_f_map = soc12_adv_f_map
    elif start_col == SOC_00:
        direct_map = soc_direct_map
        hs_m_map = soc_hs_m_map
        hs_f_map = soc_hs_f_map
        ba_m_map = soc_ba_m_map
        ba_f_map = soc_ba_f_map
        adv_m_map = soc_adv_m_map
        adv_f_map = soc_adv_f_map
    elif start_col == NAICS_02:
        direct_map = naics_direct_map
        hs_m_map = naics_hs_m_map
        hs_f_map = naics_hs_f_map
        ba_m_map = naics_ba_m_map
        ba_f_map = naics_ba_f_map
        adv_m_map = naics_adv_m_map
        adv_f_map = naics_adv_f_map
    else:
        raise Exception("BAD START COLUMN")
    # -- First apply direct transformation then split into groups
    df.loc[df[start_col].isin(direct_map.keys()), start_col] = df[start_col].map(direct_map)

    # print direct_map
    # print df.head()
    # print start_col
    # print df[start_col]
    # print df[start_col].isin(direct_map.keys())

    # print "HEL" , df.loc[df[start_col].isin(direct_map.keys())]
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
    
    if not df[EVERYTHING_ELSE][start_col].empty:
        print df[EVERYTHING_ELSE]
        raise Exception("*** ERROR! Unaccounted for people")

    rules = [ (HS_MALE, hs_m_map), (HS_FEMALE, hs_f_map),
                (BA_MALE, ba_m_map), (BA_FEMALE, ba_f_map),
                (ADV_MALE, adv_m_map), (ADV_FEMALE, adv_f_map)]

    for (rule, rule_map) in rules:
        rule = rule & df[start_col].notnull()
        df.loc[rule, xforms[start_col]] = df[rule][start_col].apply(lambda x: randomizer(x, rule_map, start_col))

    return df


def _convert_07_to_12(df):
    print df.head()
    # -- First apply direct transformation then split into groups
    start_col = NAICS_07
    direct_matches = df[start_col].isin(naics_12_direct_map.keys())
    df.loc[direct_matches, xforms[start_col]] = df[start_col].map(naics_12_direct_map)
    df.loc[~direct_matches, xforms[start_col]] = df[~direct_matches][start_col].apply(randomizer12)
    df.drop(start_col, axis=1, inplace=True)
    return df

def occ_convert(df, var_map):
    school_mode = is_old_school(var_map)
    soc_mode = get_soc_mode(var_map)

    # TODO: VERIFY: school_mode might need to vary for 5 year estimates

    if SOC_00 in df.columns:
        print "Spotted SOCP00 in columns...first convert this."
        df = _convert(df, SOC_00, school_mode)
        df = _convert(df, SOC_10, school_mode)
        df.drop(SOC_00, axis=1, inplace=True)
    elif SOC_10 in df.columns:
        print "Spotted SOCP10 in columns...first convert this."
        print df, "HERE!!!"
        df = _convert(df, SOC_10, school_mode)
        df.drop(SOC_10, axis=1, inplace=True)
    else:
        print "*** single SOCP ****"
        df.rename(columns={"SOCP": soc_mode}, inplace=True)
        if soc_mode == NAICS_02:
            df = _convert(df, SOC_00, school_mode)
            df = _convert(df, SOC_10, school_mode)
            df.drop(SOC_00, axis=1, inplace=True)
        elif soc_mode == SOC_10:
            df = _convert(df, SOC_10, school_mode)
            df.drop(SOC_10, axis=1, inplace=True)
        elif soc_mode == SOC_12:
            pass # Nothing to do!
    df.rename(columns={"SOCP12": "soc"}, inplace=True)
    return df

def handle_class(df, vintages, standard):
    for yr in vintages:
        vintage = standard + yr
        if vintage in df.columns:
            df.loc[df[vintage].notnull(), standard] = df[vintage]
            del df[vintage]
    return df

def naics_convert(df, var_map):
    df.rename(columns={"naicsp07": NAICS_07, "naicsp02": NAICS_02}, inplace=True)
    # print df.columns

    year = int(var_map["year"])
    est = int(var_map["est"])
    school_mode = is_old_school(var_map)
    naics_mode = get_naics_mode(var_map)

    # print "BEFORE"
    # print; print;
    # print df
    # print; print;

    if NAICS_02 in df.columns:
        print "Spotted NAICSP02 in columns...first convert this."
        df = _convert(df, NAICS_02, school_mode)
        df = _convert_07_to_12(df)
        df.drop(NAICS_02, axis=1, inplace=True)
    elif NAICS_07 in df.columns:
        print "Spotted NAICSP07 in columns...first convert this."
        df = _convert_07_to_12(df)
        df.drop(NAICS_07, axis=1, inplace=True)
    else:
        print "*** single NAICSP ****"
        df.rename(columns={"NAICSP": naics_mode}, inplace=True)
        if naics_mode == NAICS_02:
            print "Detected NAICS02...converting..."
            df = _convert(df, NAICS_02, school_mode)
            df = _convert_07_to_12(df)
            df.drop(NAICS_02, axis=1, inplace=True)
        elif naics_mode == NAICS_07:
            print "Detected NAICS07. Converting to NAICS12..."
            df = _convert_07_to_12(df)
            df.drop(NAICS_07, axis=1, inplace=True)
        elif naics_mode == NAICS_12:
            print "Nothing to do!"
            pass # Nothing to do!
    df.rename(columns={"NAICSP12": "naics"}, inplace=True)
    return df

if __name__ == '__main__':
    print "Testing industry conversion..."
    moi = pd.DataFrame({"x": [100], NAICS_07: ["4431M"], })
    moi = pd.DataFrame({"x": [100,44], "NAICSP02": [np.nan, "451M"], "NAICSP07": ["32712", np.nan], "SEX":  [2,2], "SCHL": [10,10]})
    res = naics_convert(moi, {"year": 2010, "est": 5})
    print "Converted:"
    print res
    print "Testing occupation conversion..."
    print
    moi = pd.DataFrame({"x": [100,44], "socp00": [np.nan, "472020"], "socp10": ["472XXX", np.nan], "SEX":  [2,2], "SCHL": [10,10]})
    print "Starting with"
    print moi
    print "####"
    res = occ_convert(moi, {"year": 2006, "est": 3})
    print "CONVERTED TO"
    print res