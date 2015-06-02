import pandas as pd
import numpy as np

def process(df, settings=None, pk=[], var_map={}):
    print "SCHL" in df.columns

    df = _prepare(df, settings, pk)
    df, pk = _post_process(df, settings, pk, var_map=var_map)
    df = statistics.compute(df, settings, pk)

    return df