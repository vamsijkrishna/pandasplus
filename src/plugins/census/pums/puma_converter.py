import pandas as pd
from numpy.random import choice
from classfication_converter import get_path, COL_RATE

POWPUMA00 = 'POWPUMA00'
POWPUMA10 = 'POWPUMA10'
UNKNOWN = 'XXXXXXX'
GEO = 'geo'
POWSP= 'POWSP'

puma_map = pd.read_csv(get_path('data/PUMA_00_to_10.csv'), converters={"POWSP": str, "POWPUMA00": str, "POWPUMA10": str})
puma_map[POWPUMA00] = puma_map.POWPUMA00.str.replace('G', '')
puma_map[POWPUMA10] = puma_map.POWPUMA10.str.replace('G', '')

puma_coder = puma_map.groupby(POWPUMA00).agg({"SUM_H7V001": pd.Series.sum})
puma_coder = puma_coder.reset_index()
puma_coder.rename(columns={"SUM_H7V001": "total_sum"}, inplace=True)

puma_map = pd.merge(puma_map, puma_coder, on=POWPUMA00, how="left")
puma_map[COL_RATE] = puma_map["SUM_H7V001"] / puma_map["total_sum"]
puma_map.drop(["FREQUENCY", "PERCPOP10", "SUM_H7V001", "total_sum"], axis=1, inplace=True)


mymap = {}

def randomizer(code):
    if code == UNKNOWN:
        return UNKNOWN
    tmpdf = puma_map[puma_map.POWPUMA00 == code]
    if tmpdf.empty: # nothing in crosswalk, then it hasn't changed
        # bad_list = ["3904400", "0100900", "1301400", "4202190", "4804690", "1301100", "1200100"]
        mymap[code] = True
        # if code in bad_list or len(code) == len("00100900") : return "FF"
        # raise Exception("WTF:", code)
        return "FF"

    selected_idx = choice(range(len(tmpdf)), p=tmpdf[COL_RATE].values)
    return tmpdf.iloc[selected_idx].POWPUMA10

def update_puma(df, on_col):
    df[GEO] = df[POWSP] + df[on_col]
    df[GEO] = df[GEO].apply(randomizer)
    df.drop(on_col, axis=1, inplace=True)
    print [x for x in mymap.keys() if len(x) == 7]
    return df

if __name__ == '__main__':
    moi = pd.DataFrame({POWPUMA00: ["0100200"], "val": [20] })
    print update_puma(moi, POWPUMA00)


'''
3904400, 0100900, 1301400, 4202190,

'''