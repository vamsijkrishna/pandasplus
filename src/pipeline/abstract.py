from consts import *
import re
import itertools
import os
from src.plugins.helpers import get_file
import pandas as pd
import bz2
import StringIO

class BaseBuilder(object):
    def __init__(self, config):
        self.config = config
        self.coerce = self._get_config([GLOBAL, TYPE], optional=True, default={})
        for k in self.coerce:
            self.coerce[k] = eval(self.coerce[k])
        self.preview = {}

    def _get_config(self, name, optional=False, default=None):
        if type(name) == str:
            name = [name]
        mydata = self.config
        for n in name:
            if not n in mydata:
                if not optional:
                    raise InvalidSettingException("%s not in configuration!" % (name))
                else:
                    return default
            mydata = mydata[n]
        return mydata

    def _get_setting(self, name, setts, default=None, optional=True):
        if name in setts:
            return setts[name]
        if not optional:
            raise InvalidSettingException("Couldn't find %s in %s" % (name, setts))
        return default

    def _output_str(self, var_map=None):
        output_path = self._get_config([GLOBAL, OUTPUT])
        if var_map:
            for var, val in var_map.items():
                output_path = output_path.replace("<{}>".format(var), val)
            # output_path = os.path.join(output_path, str(var))
        output_path = os.path.expandvars(output_path)
        return output_path

    def _check_file(self, file_path, var_map):
        output_path = self._output_str(var_map=var_map)

        gconf = self._get_config([GLOBAL])
        if FTP_PATHS in gconf or WEB_PATHS in gconf:
            print "Attempting file and (if needed) FTP check", output_path
            grab_if_needed(file_path, gconf)

    def _check_hdf_cache(self, input_file, var_map):
        output_path = self._output_str(var_map=var_map)
        # print "CHECK HDF"
        # print input_file
        # print output_path, "OUTPUT PATH"
        file_name = os.path.basename(input_file)
        target = os.path.join(output_path, file_name + ".h5")

        if not os.path.exists(output_path):
            # -- make directory if it doesn't exist
            os.makedirs(output_path)

        if not os.path.exists(target):
            return (False, target)
        df = pd.read_hdf(target, HDF_CACHE)
        return (df, target)

    def _file_to_df(self, file_obj):
        delim = self._get_config([GLOBAL, SEPERATOR], optional=True, default=";")
        dec = self._get_config([GLOBAL, DECIMAL], optional=True, default=",")
        encoding = self._get_config([GLOBAL, ENCODING], optional=True, default="utf-8-sig")
        columns = self._get_config([GLOBAL, COLUMNS], optional=True)
        na_values = self._get_config([GLOBAL, NA_VALUES], optional=True)

        if columns:
            df = pd.read_csv(file_obj, names=columns, header=None, sep=delim, encoding=encoding, decimal=dec, converters=self.coerce, na_values=na_values)
        else:
            df = pd.read_csv(file_obj, header=0, sep=delim, encoding=encoding, decimal=dec, converters=self.coerce, na_values=na_values)
        return df

    def _to_df(self, input_file, use_cache=True, var_map=None, save_to_cache=True):
        hdf_df, target = self._check_hdf_cache(input_file, var_map)
        if hdf_df is not False and use_cache:
            print "Reading from HDF file..."
            return hdf_df

        self._check_file(input_file, var_map)
        print "Opening file..."
        input_file = get_file(input_file)
        df = self._file_to_df(input_file)
        if use_cache and save_to_cache:
            print "Saving dataframe in HDF file..."
            df.to_hdf(target, HDF_CACHE, append=False)

        return df

    def _str_save(self, table_name, tbl, var_map=None):
        print "** In memory save!"
        output = StringIO.StringIO()
        encoding = "utf-8-sig"
        tbl.to_csv(output, sep="\t", index=False, encoding=encoding)
        output.seek(0)
        self.preview[table_name] = output

    def save(self, table_name, tbl, var_map=None):
        print "** Saving", table_name, "..."
        mode = self._get_config([GLOBAL, MODE], optional=True)
        print mode, "=mode"
        if mode == PREVIEW:
            self._str_save(table_name, tbl, var_map=var_map)
        else:
            output_path = self._output_str(var_map)

            # -- check if output path exists, if not create it
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            file_name = table_name + ".tsv.bz2"
            new_file_path = os.path.abspath(os.path.join(output_path, file_name))
            encoding = "utf-8-sig" #self._get_config([GLOBAL, ENCODING], optional=True, default="utf-8-sig")
            tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=False, encoding=encoding)
            print "** Save complete."
            self._import_to_db(new_file_path)

                # self._run_helper(df, var_map=var_map)
