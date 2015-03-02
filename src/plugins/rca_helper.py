import numpy as np
import pandas as pd
import os, sys

def calc_rca(df, index=None, column=None, values=None, where={}, depths=[]):
    print "RCA Calculation **********************"
    # -- compute RCA for the deepest nesting level
    df = df.reset_index()

    if type(index) == str:
        index = [index]

    rpk = index + [column]

    all_rcas = pd.DataFrame()

    for rca_pk in depths:
        conds = [ df[colname] == value for colname,value in where.items() ]
        conds += rca_pk

        # TODO: fix bug with multiple values
        # rca_values = pd.DataFrame()
        for value in values:

            final_cond = reduce(lambda x,y : x & y, conds)
            df_to_pivot = df[final_cond]

            print index,column, value
            print df_to_pivot.head()
            rca_df = df_to_pivot.pivot_table(index=index, columns=column, values=value)
            rca_df = _rca(rca_df)
            ## -- merge RCAs back into table...
            rca_df = pd.DataFrame(rca_df.stack())
            print rca_df.head(), "RCA_DF"

            rca_df = rca_df.reset_index()
            rca_df = rca_df.rename(columns={0: value + u"_rca"})
            all_rcas = pd.concat([all_rcas, rca_df])

    df = pd.merge(df, all_rcas, how='left', left_on=rpk, right_on=rpk)

    return df


def _rca(tbl, populations=None):
  # fill missing values with zeros
  tbl = tbl.fillna(0)
  # get sum over columns
  col_sums = tbl.sum(axis=1)
  # we now need to transpose or "reshape" this array so that
  # it is in the form of one long column
  col_sums = col_sums.reshape((len(col_sums), 1))

  # create the numerator matrix for the final RCA calculation by
  # dividing each value by its row's sum
  rca_numerator = np.divide(tbl, col_sums)

  # get the sum over all the rows
  row_sums = tbl.sum(axis=0)

  # if populations is set create the denominator based on that for POP RCA
  if populations.__class__ == pd.DataFrame or populations.__class__ == pd.Series:

    # create the denominator matrix for the final RCA calculation
    # by dividing the industry sums by a single value (the matrix total sum)
    rca_denominator = populations / float(populations.sum())
    
    # rca_denominator = rca_denominator.reshape((len(rca_denominator), 1))
    # print rca_numerator.shape
    
    # rca_denominator = pd.DataFrame(rca_denominator, columns=[rca_numerator.columns[0]])
    # rca_denominator = rca_denominator.reindex(index=rca_numerator.index)
    # rca_denominator = rca_denominator.reindex(columns=rca_numerator.columns, method="ffill")
    # print rca_decnominator.ix["ac"]

    # lastly we get the RCAs by dividing the numerator matrix by denominator
    rcas = rca_numerator.T / rca_denominator
    rcas = rcas.T

  else:
    # get total of all the values in the matrix
    total_sum = tbl.sum().sum()

    # create the denominator matrix for the final RCA calculation
    # by dividing the industry sums by a single value (the matrix total sum)
    rca_denominator = row_sums / total_sum

    # lastly we get the RCAs by dividing the numerator matrix by denominator
    rcas = rca_numerator / rca_denominator
  
  return rcas


if __name__ == '__main__':
    df = pd.DataFrame({"city": ["CHI", "BRA", "CHI"], "hs": ["111", "111", "112"], "imports": [5, 65, 10]})
    df = calc_rca(df, index="city", column="hs", values=["imports"])
    print df
