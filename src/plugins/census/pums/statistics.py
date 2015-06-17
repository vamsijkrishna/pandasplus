import pandas as pd

from src.pipeline.exceptions import InvalidSettingException
from src.pipeline.consts import VALUES

def compute(df, settings={}, pk=[]):
    weight_col = "PWGTP"

    count_value = "num_ppl"
    df[count_value] = df[ weight_col ]
    if not VALUES in settings:
        raise InvalidSettingException("Must have PUMS values list")
    values = settings[VALUES]
    num_wts = 80 + 1

    aggs = {c : pd.Series.sum for c in df.columns if weight_col in c}
    aggs[count_value] = pd.Series.sum

    for value in values:
        val_wt_col = "{}_wt".format(value)
        df[val_wt_col] = df[value] * df[weight_col]
        aggs[val_wt_col] = pd.Series.sum

        for i in range(1, num_wts):
            rval_wt_col = "{}_wt_{}".format(value, i)
            df[rval_wt_col] = df[value] * df[weight_col + str(i)]
            aggs[rval_wt_col] = pd.Series.sum

    df = df.groupby(pk).agg(aggs)
    df = df.reset_index()

    rws = ["rw_" + str(i) for i in range(1, 81) ]
    for i in range(1, 81):
        df["rw_" + str(i)] = (df[count_value] - df[weight_col + str(i)]) ** 2

    for i in range(1, 81):
        variance,se,moe = [count_value + v for v in ["_variance", "_se", "_moe"]]
        df[variance] = df[rws].sum(axis=1) * (4.0 / 80)
        df[se] = df[variance] ** 0.5
        df[moe] = df[se] * 1.645 # 90% confidence interval

    for value in values:
        val_wt_col = "{}_wt".format(value)
        df[val_wt_col] = df[val_wt_col] / df[weight_col]

        for i in range(1, num_wts):
            rval_wt_col = "{}_wt_{}".format(value, i)
            df[rval_wt_col] = df[rval_wt_col] / df[weight_col + str(i)]

    # print df[[c for c in df.columns if "_wt_" in c]],"WWWW"

        rwts = []
        for i in range(1, num_wts):
            rval_wt_col = "{}_wt_{}".format(value, i)
            delta_col = "{}_delta_{}".format(value, i)
            df[delta_col] = (df[rval_wt_col] - df[val_wt_col]) ** 2
            rwts.append(delta_col)

        variance,se,moe = [value + v for v in ["_variance", "_se", "_moe"]]
        df[variance] = df[rwts].sum(axis=1) * (4.0 / 80.0)
        df[se] = df[variance] ** 0.5
        df[moe] = df[se] * 1.645 # 90% confidence interval

    cols_to_keep = pk + ["num_ppl", "num_ppl_moe"]
    cols_to_keep += ["{}_wt".format(value) for value in values]
    cols_to_keep += ["{}_moe".format(value) for value in values]
    to_drop = [c for c in df.columns if c not in cols_to_keep]
    df.drop(to_drop, axis=1, inplace=True)

    return df

if __name__ == '__main__':
    moi = pd.read_csv("/tmp/test.csv")
    setts = {}
    print _compute(moi, setts,    pk=["name"])


