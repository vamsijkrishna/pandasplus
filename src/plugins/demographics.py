def map_d_hedu(df):
    MALE, FEMALE = 0, 1
    gender_dict = {MALE: 'A', FEMALE: 'B'}
    WHITE=1;BLACK=2;MULTI=3;ASIAN=4;INDIAN=5;UNIDENTIFIED=6;UNKNOWN=9
    color_dict = {UNIDENTIFIED: 'H' , INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', UNKNOWN:'H', -1:'H', 0:'H'}
    FEDERAL, STATE, LOCAL, PROFIT_PRIVATE, NONPROFIT_PRIVATE, SPECIAL = 1, 2, 3, 4, 5, 6
    school_type_dict = {FEDERAL:'P', STATE:'Q', LOCAL:'R', PROFIT_PRIVATE:'S', NONPROFIT_PRIVATE:'T', SPECIAL:'U'}

    df.loc[df.variable == 'Gender', 'd_id'] = df.loc[df.variable == 'Gender', 'd_id'].map(gender_dict)
    df.loc[df.variable == 'Ethnicity', 'd_id'] = df.loc[df.variable == 'Ethnicity', 'd_id'].map(color_dict)
    df.loc[df.variable == 'Adm_category', 'd_id'] = df.loc[df.variable == 'Adm_category', 'd_id'].map(school_type_dict)
    return df

def map_d_sc(df):
    MALE, FEMALE = 0, 1
    gender_dict = {MALE: 'A', FEMALE: 'B'}

    WHITE=1; BLACK=2; MULTI=3; ASIAN=4; INDIAN=5; UNKNOWN = 9
    color_dict = {INDIAN:'C', WHITE:'D', BLACK:'E', ASIAN:'F', MULTI:'G', 9:'H', -1:'H', 0:'H'}

    URBAN, RURAL = 1, 2
    loc_dict = {URBAN:'N', RURAL:'O'}

    FEDERAL, STATE, LOCAL, PRIVATE = 1, 2, 3, 4
    school_type_dict = {FEDERAL:'P', STATE:'Q', LOCAL:'R', PRIVATE:'S'}

    df.loc[df.variable == 'Gender', 'd_id'] = df.loc[df.variable == 'Gender', 'd_id'].map(gender_dict)
    df.loc[df.variable == 'Color', 'd_id'] = df.loc[df.variable == 'Color', 'd_id'].map(color_dict)
    df.loc[df.variable == 'Location', 'd_id'] = df.loc[df.variable == 'Location', 'd_id'].map(loc_dict)
    df.loc[df.variable == 'Adm_Dependency', 'd_id'] = df.loc[df.variable == 'Adm_Dependency', 'd_id'].map(school_type_dict)

    return df