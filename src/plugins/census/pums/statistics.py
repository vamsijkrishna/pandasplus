import pandas as pd

def compute(df, settings={}, pk=[]):
    weight_col = "PWGTP"
    value = "WAGP"
    num_wts = 80 + 1
    df = df[(df[value] > 0) & (df.POWSP == "39")].copy()

    val_wt_col = "{}_wt".format(value)
    df[val_wt_col] = df[value] * df[weight_col]
    aggs = {c : pd.Series.sum for c in df.columns if weight_col in c}
    aggs[val_wt_col] = pd.Series.sum

    for i in range(1, num_wts):
        rval_wt_col = "{}_wt_{}".format(value, i)
        df[rval_wt_col] = df[value] * df[weight_col + str(i)]
        aggs[rval_wt_col] = pd.Series.sum

    df = df.groupby(pk).agg(aggs)
    df = df.reset_index()
    df[val_wt_col] = df[val_wt_col] / df[weight_col]

    for i in range(1, num_wts):
        rval_wt_col = "{}_wt_{}".format(value, i)
        df[rval_wt_col] = df[rval_wt_col] / df[weight_col + str(i)]

    print df[[c for c in df.columns if "_wt_" in c]],"WWWW"

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

    cols_to_drop = [variance, se] + [c for c in df.columns if "delta" in c or "PWGTP" in c or "_wt_" in c]
    df.drop(rwts + cols_to_drop, axis=1, inplace=True)
    return df

if __name__ == '__main__':
    moi = pd.read_csv("/tmp/test.csv")
    setts = {}
    print _compute(moi, setts,    pk=["name"])


