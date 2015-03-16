# growth.py
import pandas as pd

def do_growth(tbl, tbl_prev, pk, cols, years_ago=1, delta_col=None):
    '''Growth rate'''

    if not delta_col:
        delta_col = "Year" if "Year" in pk else "year"

    pk_minus_year = list(set(pk).difference(set([delta_col])))
    tbl_prev = tbl_prev[pk_minus_year+cols]

    tbl = tbl.merge(tbl_prev, on=pk_minus_year, how='left', suffixes=["_new", "_old"])
    
    for orig_col_name in cols:
        print tbl.columns,  tbl_prev.columns

        new_col_name = orig_col_name + "_growth"
        if years_ago > 1:
            new_col_name = "{0}_{1}".format(new_col_name, years_ago)
        
        tbl[new_col_name] = ((1.0 * tbl[orig_col_name+"_new"]) / (1.0*tbl[orig_col_name + "_old"])) ** (1.0/years_ago) - 1

    for colname in cols:
        del tbl[colname + "_old"]
        tbl.rename(columns={ c+"_new" : c for c in cols }, inplace=True)

    return tbl