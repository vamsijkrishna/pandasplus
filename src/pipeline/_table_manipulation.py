import pandas as pd
from consts import *

def add_column_lengths(table, pk, table_config):
    depths = table_config[DEPTHS] if DEPTHS in table_config else {}
    # if there are more than more depth levels, add a depth column
    make_len = { colname : len(col_depths) > 1 for colname,col_depths in depths.items() } 
    for column in pk:
        if column in make_len and make_len[column]:
            table[column + "_len"] = pd.Series( map(lambda x: len(str(x)), table.index.get_level_values(column)), index = table.index)
    return table

def add_summary_rows(table, pk, config):
    table = table.reset_index()
    for colname,value in config.items():
        new_pk = list(set(pk).difference([colname]))
        addtl_data = table.groupby(new_pk).sum().reset_index()
        addtl_data[colname] = value
        table = pd.concat([table, addtl_data])
    return table
