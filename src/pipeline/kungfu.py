# kungfu.py
from importlib import import_module
import pandas as pd
# import src.plugins
from consts import *
import goslate
from database import DB

def str_to_func(pluginpath):
    path,methodname = pluginpath.rsplit('.', 1)
    module = import_module(path)
    applyfunc = getattr(module, methodname)
    return applyfunc

def check_requirements(settings, df_cols):
    if REQUIRED in settings:
        req_cols = settings[REQUIRED]
        for rcol in req_cols:
            if rcol not in df_cols:
                print "Skipping COND_MAP because %s not in %s" % (rcol, df_cols)
                return False
    return True

def process_operation(df, colname, settings, agg=None, pk=[], var_map={}):

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
        if isinstance(mymap, str):
            mymap = eval(mymap)
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

    elif settings[TYPE] == ZFILL and ((colname in pk) or (colname in agg)): # -- do selective application
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
        # print agg_map
        for k,v in agg_map.items():
            tmp = agg[v]
            agg[k] = tmp
        for k,v in agg_map.items():
            if v in agg:
                del agg[v]

        df = df.reset_index()
    elif settings[TYPE] == FUNC and colname in pk:
        func = str_to_func(settings[FUNC])
        df[colname] = df[colname].map(func)
    elif settings[TYPE] == TRANSLATE:
        gs = goslate.Goslate()
        target = settings[TARGET]
        func = lambda x: gs.translate(x, target)
        df[colname] = df[colname].map(func)
    elif settings[TYPE] == DBLOOKUP and colname in pk:
        db = DB()
        lookup_map = db.make_dict(**settings)
        # print lookup_map
        df[colname] = df[colname].map(lookup_map)
    elif settings[TYPE] == SLICE and colname in pk:
        df[colname] = df[colname].astype(str).str.slice(0, settings[LENGTH])
    elif settings[TYPE] == PREPEND and colname in pk:
        df.loc[df[colname].str.len() == 0, colname] = 'XXXXXX'
        df[colname] = settings[VALUE] + df[colname].astype(unicode)
    elif settings[TYPE] == REPLACE and colname in pk:
        target = settings[TARGET]
        value = settings[VALUE]
        df[colname] = df[colname].str.replace(target, value)
    elif settings[TYPE] == FRAME_FUNC:
        func = str_to_func(settings[FUNC])
        df = func(df, settings, pk=pk, var_map=var_map)
    elif settings[TYPE] == CONCAT_AND_FILL:
        if ZFILL in settings:
            size = settings[ZFILL]
            df[colname] = df[colname].astype(str).str.strip()
            df[colname] = df[colname].str.pad(size)
            df[colname] = df[colname].str.replace(' ', '0')
        if PREFIX in settings:
            df[colname] = settings[PREFIX] + df[colname]
        if POSTFIX in settings:
            df[colname] = df[colname] + settings[POSTFIX]
    return df
