# -*- coding: utf-8 -*-
# Adapted from https://github.com/alexandersimoes/ps_calcs

import sys
import numpy as np
import pandas as pd
import numpy as np
from pandas.tools.pivot import pivot_table


def calc_complexity(builder, df, settings, depths, var_map):
    ''' Given a dataframe with RCA, compute PCI ECI '''
    geo = settings["geo"]
    hs = settings["product"]
    rca_col = settings["rca"]
    time_col = settings["time"]

    rca_df =  df[[geo, hs, rca_col]]

    master_eci = pd.DataFrame()
    master_pci = pd.DataFrame()

    for depth in depths:
        final_cond = reduce(lambda x,y : x & y, depth)
        print "Computing complexity at depth", depth
        my_rca = rca_df[final_cond]    
        my_rca["rca"] = (my_rca[rca_col] >= 1).astype(int)
        moi = pivot_table(my_rca, values='rca', index='province and city', columns='HS8')
        moi = moi.fillna(0)

        eci, pci = _complexity(moi)
        
        eci = pd.DataFrame(eci).rename(columns={0: "eci"})
        print "ECI", eci
        eci = eci.reset_index()
        pci = pd.DataFrame(pci).rename(columns={0: "pci"})
        pci = pci.reset_index()
        eci[time_col] = var_map[time_col]
        pci[time_col] = var_map[time_col]
        print "ECI2", eci
        # print "PCI2", pci
        master_eci = pd.concat([master_eci, eci])
        master_pci = pd.concat([master_pci, pci])

    builder.save("eci", master_eci, var_map)
    builder.save("pci", master_pci, var_map)

        
    # hullo = df[(df.HS8.str.len() == 2) & (df['province and city'].str.len() == 4 )]
    

def _complexity(rcas, drop=True):
  
  rcas_clone = rcas.copy()
  
  # drop columns / rows only if completely nan
  rcas_clone = rcas_clone.dropna(how="all")
  rcas_clone = rcas_clone.dropna(how="all", axis=1)
  
  if rcas_clone.shape != rcas.shape:
    print "[Warning] RCAs contain columns or rows that are entirely comprised of NaN values."
    if drop:
      rcas = rcas_clone
  
  kp = rcas.sum(axis=0)
  kc = rcas.sum(axis=1)
  kp0 = kp.copy()
  kc0 = kc.copy()

  for i in range(1, 20):
    kc_temp = kc.copy()
    kp_temp = kp.copy()
    kp = rcas.T.dot(kc_temp) / kp0
    if i < 19:
      kc = rcas.dot(kp_temp) / kc0
  
  geo_complexity = (kc - kc.mean()) / kc.std()
  prod_complexity = (kp - kp.mean()) / kp.std()

  return geo_complexity, prod_complexity


if __name__ == '__main__':
    df = pd.read_csv("/Users/jspeiser/Downloads/ygp.tsv", sep="\t", converters={"HS8": str}, encoding="utf-8-sig")
    hullo = df[(df.HS8.str.len() == 2) & (df['province and city'].str.len() == 4 )]
    print calculate_complexity(hullo, {"geo": "province and city", "product": "HS8", "rca_col": u'$ amount出口_rca'})
