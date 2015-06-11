import pandas as pd
from numpy.random import choice
from classfication_converter import get_path, COL_RATE

PUMA00 = 'PUMA00'
PUMA10 = 'PUMA10'
UNKNOWN = 'XXXXXXX'
GEO = 'geo'
POWSP= 'POWSP'

puma_map = pd.read_csv(get_path('data/PUMA_00_to_10.csv'), converters={"POWSP": str, "PUMA00": str, "PUMA10": str})
puma_map[PUMA00] = puma_map.PUMA00.str.replace('G', '')
puma_map[PUMA10] = puma_map.PUMA10.str.replace('G', '')

puma_coder = puma_map.groupby(PUMA00).agg({"SUM_H7V001": pd.Series.sum})
puma_coder = puma_coder.reset_index()
puma_coder.rename(columns={"SUM_H7V001": "total_sum"}, inplace=True)

puma_map = pd.merge(puma_map, puma_coder, on=PUMA00, how="left")
puma_map[COL_RATE] = puma_map["SUM_H7V001"] / puma_map["total_sum"]
puma_map.drop(["FREQUENCY", "PERCPOP10", "SUM_H7V001", "total_sum"], axis=1, inplace=True)



mymap = {}

def randomizer(code):
    if code == UNKNOWN:
        return UNKNOWN
    tmpdf = puma_map[puma_map.PUMA00 == code]
    if tmpdf.empty: # nothing in crosswalk, then it hasn't changed
        raise Exception("MISSING!!!", code)
    
    selected_idx = choice(range(len(tmpdf)), p=tmpdf[COL_RATE].values)
    return tmpdf.iloc[selected_idx].PUMA10

def update_puma(df, on_col):
    df[on_col] = df[on_col].apply(randomizer)
    # df.drop(on_col, axis=1, inplace=True)
    print [x for x in mymap.keys() if len(x) == 7]
    return df

if __name__ == '__main__':
    moi = pd.DataFrame({PUMA00: ["0101100"], "val": [20] })
    print update_puma(moi, PUMA00)


'''
3904400, 0100900, 1301400, 4202190,

'''