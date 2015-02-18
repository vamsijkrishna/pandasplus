# kungfu.py
from importlib import import_module
import pandas as pd
# import src.plugins
from consts import *


def check_requirements(settings, df_cols):
    if REQUIRED in settings:
        req_cols = settings[REQUIRED]
        for rcol in req_cols:
            if rcol not in df_cols:
                print "Skipping COND_MAP because %s not in %s" % (rcol, df_cols)
                return False
    return True

def process_operation(df, colname, settings, agg=None, pk=[]):

    if settings[TYPE] == MELT and colname in pk:
        pluginpath = settings[FUNC]

        pk_m_col = list( set(pk).difference([colname]) )

        preserved = settings[PRESERVE] if PRESERVE in settings else []

        settings["id_vars"] = list(set(pk_m_col + agg.keys() + preserved))

        kwargs = {k:v for k,v in settings.items() if k not in [FUNC, TYPE, PRESERVE]}

        df = pd.melt(df, **kwargs)
        path,methodname = pluginpath.rsplit('.', 1)
        module = import_module(path)
        applyfunc = getattr(module, methodname)

        df = applyfunc(df)
    elif settings[TYPE] == CAST:
        t = settings[AS]
        types = {"str" : str, "float": float, "int": int}
        df[colname] = df[colname].astype(types[t])
    elif settings[TYPE] == MAP:
        mymap = settings[MAP]
        target = settings[TARGET]
        df[colname] = df[target].map(mymap)
    elif settings[TYPE] == COND_MAP:

        if not check_requirements(settings, df.columns):
            return df

        cond = settings[COND]
        cond = eval(cond)

        if FUNC in settings:
            target = settings[TARGET]

            func = eval(settings[FUNC])
            
            df.loc[cond, colname] = df.loc[cond,target].map(func)
        elif VALUE in settings:
            val = settings[VALUE]
            df.loc[cond, colname] = val
        elif COPY_COL in settings:
            col = settings[COPY_COL]
            df.loc[cond, colname] = df[col]

    elif settings[TYPE] == ZFILL and ((colname in agg) or (colname in pk)): # -- do selective application
        size = settings[SIZE]
        df[colname] = df[colname].astype(str).str.pad(size)
        df[colname] = df[colname].str.replace(' ', '0')
    elif settings[TYPE] == MATHOP:
        if settings[REQUIRED] in df.columns:
            df[colname] = eval(settings[FUNC])
    elif settings[TYPE] == DROP:
        if colname in df.columns:
            del df[colname]
    elif settings[TYPE] == CLONE:
        df[colname] = settings[SOURCE]
    elif settings[TYPE] == UNSTACK:
        target = settings[TARGET]
        target_index = len(pk)
        target_pk = pk + [target]
        df = df.reset_index()
        df = df.groupby(target_pk).agg(agg)
        df = df.unstack(target_index)
        agg_map = {u''.join(t):t[0] for t in df.columns}
        df.columns = [u''.join(t) for t in df.columns ]
        # update agg!
        print agg_map
        for k,v in agg_map.items():
            tmp = agg[v]
            agg[k] = tmp
        for k,v in agg_map.items():
            if v in agg:
                del agg[v]

        df = df.reset_index()

    return df
