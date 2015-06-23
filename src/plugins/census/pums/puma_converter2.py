import numba
import pandas as pd
from numpy.random import choice
from src.plugins.census.pums.classfication_converter import get_path, COL_RATE

PUMA = 'PUMA'
PUMA00 = 'PUMA00'
PUMA10 = 'PUMA10'
UNKNOWN = 'XXXXXXX'
KATRINA_PUMA = '2277777'
POWSP= 'POWSP'

puma_map = pd.read_csv(get_path('data/PUMA_00_to_10.csv'), converters={"POWSP": str, "PUMA00": str, "PUMA10": str})
puma_map[PUMA00] = puma_map.PUMA00.str.replace('G', '')
puma_map[PUMA10] = puma_map.PUMA10.str.replace('G', '')
# cut out extra 0 from IPUMS PUMA10 field
puma_map[PUMA10] = puma_map[PUMA10].str.slice(0, 2) + puma_map[PUMA10].str.slice(3, None)

puma_coder = puma_map.groupby(PUMA00).agg({"SUM_H7V001": pd.Series.sum})
puma_coder = puma_coder.reset_index()
puma_coder.rename(columns={"SUM_H7V001": "total_sum"}, inplace=True)

puma_map = pd.merge(puma_map, puma_coder, on=PUMA00, how="left")
puma_map[COL_RATE] = puma_map["SUM_H7V001"] / puma_map["total_sum"]
puma_map.drop(["FREQUENCY", "PERCPOP10", "SUM_H7V001", "total_sum"], axis=1, inplace=True)

puma_map = puma_map.set_index("PUMA00")

@numba.jit
def randomizer(code):
    if code == UNKNOWN:
        return UNKNOWN
    if code == KATRINA_PUMA:
        return KATRINA_PUMA

    tmpdf = puma_map.ix[code]

    # if tmpdf.empty:
        # return code
    if isinstance(tmpdf, pd.Series):
        return tmpdf.PUMA10

    selected_idx = choice(range(len(tmpdf)), p=tmpdf[COL_RATE].values)
    return tmpdf.iloc[selected_idx].PUMA10

@numba.jit
def update_puma(df, on_col, has_puma10=False):
    to_drop = [] if on_col == PUMA else [on_col]
    df[on_col] = df[on_col].apply(randomizer)
    df.loc[df[on_col].notnull(), PUMA] = df[on_col]
    if has_puma10:
        df.loc[df[PUMA10].notnull(), PUMA] = df[PUMA10]
        to_drop.append(PUMA10)
    df.drop(to_drop, axis=1, inplace=True)
    return df

if __name__ == '__main__':
    moi = pd.DataFrame({PUMA: ["5100600"], "val": [40]})
    print update_puma(moi, PUMA)
