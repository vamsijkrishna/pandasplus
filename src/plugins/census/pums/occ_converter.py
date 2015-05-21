import pandas as pd

# soc_map = pd.read_csv('data/occ02to10.csv', converters={"OCCP02": str, "OCCP10": str})
# occ1012 = pd.read_csv('data/occ10to12.csv', converters={"OCCP10": str, "OCCP12": str})
COL_RATE = "Total Conversion Rate"
SOC_00 = "SOCP00"
SOC_10 = "SOCP10"

direct_map = pd.read_csv('data/SOC_00_to_10_direct.csv', converters={"SOCP00": str, "SOCP10": str})
direct_map = direct_map.to_dict(orient="records")
direct_map = {x[SOC_00] : x[SOC_10] for x in direct_map}

hs_m_map = pd.read_csv('data/SOC_00_to_10_HS_M.csv', converters={"SOCP00": str, "SOCP10": str})
hs_f_map = pd.read_csv('data/SOC_00_to_10_HS_F.csv', converters={"SOCP00": str, "SOCP10": str})
ba_m_map = pd.read_csv('data/SOC_00_to_10_BA_M.csv', converters={"SOCP00": str, "SOCP10": str})
ba_f_map = pd.read_csv('data/SOC_00_to_10_BA_F.csv', converters={"SOCP00": str, "SOCP10": str})
adv_m_map = pd.read_csv('data/SOC_00_to_10_ADV_M.csv', converters={"SOCP00": str, "SOCP10": str})
adv_f_map = pd.read_csv('data/SOC_00_to_10_ADV_F.csv', converters={"SOCP00": str, "SOCP10": str})


# COL_12 = "OCCP12"

MALE_VAL = 1
FEMALE_VAL = 2

HS_VAL = 20
BA_VAL = 21
ADV_VAL = 22

xforms = {SOC_00: SOC_10}

def occ_trans(df, occ_trans_df, on_col, value_col):
    df = pd.merge(df, occ_trans_df, on=on_col, how="left")
    df.loc[df[COL_RATE].isnull(), xforms[on_col]] = df[on_col]
    df.loc[df[COL_RATE].isnull(), COL_RATE] = 1
    df[value_col] = df[value_col] * df[COL_RATE]
    return df.drop([on_col, COL_RATE], axis=1)

def occ_00_to_10(df, occ_trans_df, value_col):
    return occ_trans(df, occ_trans_df, SOC_00, value_col)

def convert(df, value_col):
    ''' Use conversion tables to transform SOCP00 to SOCP10 across
        classifications '''
    # -- First apply direct transformation then split into groups
    df.loc[df[SOC_00].isin(direct_map.keys()), SOC_00] = df[SOC_00].map(direct_map)

    # -- Intelligently split the dataframe into six parts based on gender & edu
    HS = (moi.SCHL <= HS_VAL)
    BA = (moi.SCHL == BA_VAL)
    ADV = (moi.SCHL >= ADV_VAL)
    MALE = (moi.SEX == MALE_VAL)
    FEMALE = (moi.SEX == FEMALE_VAL)
    
    HS_MALE = HS & MALE
    HS_FEMALE = HS & FEMALE
    BA_MALE = BA & MALE
    BA_FEMALE = BA & FEMALE
    ADV_MALE = ADV & MALE
    ADV_FEMALE = ADV & FEMALE

    EVERYTHING_ELSE = ~HS_MALE & ~HS_FEMALE & ~BA_MALE & ~BA_FEMALE & ~ADV_MALE & ~ADV_FEMALE
    
    if df[EVERYTHING_ELSE][value_col].count() > 0:
        raise Exception("*** WARNING! Unaccounted for people")

    rules = [ (HS_MALE, hs_m_map), (HS_FEMALE, hs_f_map)]
    # BA_MALE, BA_FEMALE, ADV_MALE, ADV_FEMALE ]

    new_df = pd.DataFrame()
    for (rule, rule_map) in rules:
        tmpdf = df[rule].copy()
        tmpdf = occ_00_to_10(tmpdf, rule_map, value_col)
        new_df = pd.concat([new_df, tmpdf])

    return new_df

if __name__ == '__main__':
    moi = pd.DataFrame({"x": [100, 100], SOC_00: ["119061", "119061"], "SEX":  [1, 2], "SCHL": [12, 12]})
    print "Original:"
    print moi.head()
    print
    res = convert(moi, "x")
    print "Converted:"
    print res
