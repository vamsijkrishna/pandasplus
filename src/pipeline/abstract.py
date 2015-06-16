from consts import *
import re
import itertools
import os
from sqlalchemy import create_engine
from src.plugins.helpers import get_file, raw_file_handle
from src.pipeline import db
from src.fetcher import fetch

import pandas as pd
import bz2
import StringIO

import pandas.io.sql
# from src.pipeline.db import insert_monkey
# pandas.io.sql.SQLTable.insert = insert_monkey


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

    def format_vars(self, target, var_map):
        for var, val in var_map.items():
            target = target.replace("<{}>".format(var), val)
        return target

    def _get_setting(self, name, setts, default=None, optional=True):
        if name in setts:
            return setts[name]
        if not optional:
            raise InvalidSettingException("Couldn't find %s in %s" % (name, setts))
        return default

    def _output_str(self, var_map={}):
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
            for var, val in var_map.items():
                file_path = file_path.replace("<{}>".format(var), val)
            return fetch.grab_if_needed(file_path, gconf, var_map)
        return file_path

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
        if encoding == False:
            encoding = None
        columns = self._get_config([GLOBAL, COLUMNS], optional=True)
        na_values = self._get_config([GLOBAL, NA_VALUES], optional=True)
        # -- explicitly setting index_col to False forces
        #    pandas to not treat first column as an index
        index_col = self._get_config([GLOBAL, INDEX_COL], optional=True, default=False)
        usecols = self._get_config([GLOBAL, USECOLS], optional=True, default=None)

        print na_values
        df = pd.read_csv(file_obj, header=0, sep=delim, encoding=encoding, decimal=dec, converters=self.coerce, na_values=na_values, index_col=index_col, usecols=usecols)
        if columns:
            df = df[columns].copy()
        return df

    def _multi_files_to_df(self, file_obj, archive_files, var_map={}):
        '''
        archive_files can either be a string representing a suffix or a list. If archive_files is
        a string, all files within the zip with that suffix will be concatenated.
        '''
        df = pd.DataFrame()
        if isinstance(archive_files, str):
            suffix = archive_files
            archive_files = [filename for filename in file_obj.filelist if filename.endswith(suffix)]

        for filename in archive_files:
            filename = self.format_vars(filename, var_map)
            archive_fileobj = file_obj.open(filename)
            tmpdf = self._file_to_df(archive_fileobj)
            df = pd.concat([df, tmpdf])
        return df

    def _to_df(self, input_file, use_cache=True, var_map={}, save_to_cache=True):
        hdf_df, target = self._check_hdf_cache(input_file, var_map)
        if hdf_df is not False and use_cache:
            print "Reading from HDF file..."
            return hdf_df

        print "looking here", input_file
        input_file = self._check_file(input_file, var_map)
        print "Trying to open", input_file

        archive_files = self._get_config([GLOBAL, ARCHIVE_FILES], optional=True)

        if archive_files:
            archive = raw_file_handle(input_file)
            df = self._multi_files_to_df(archive, archive_files, var_map)
        else:
            input_file = get_file(input_file)
            df = self._file_to_df(input_file)

        if use_cache and save_to_cache:
            print "Saving dataframe in HDF file..."
            df.to_hdf(target, HDF_CACHE, append=False)

        return df

    def _str_save(self, table_name, tbl, var_map={}):
        print "** In memory save!"
        output = StringIO.StringIO()
        encoding = "utf-8-sig"
        tbl.to_csv(output, sep="\t", index=False, encoding=encoding)
        output.seek(0)
        self.preview[table_name] = output

    def _import_to_db(self, df, file_path, table_name, cols):
        should_import = self._get_config([GLOBAL, DB_IMPORT], optional=True, default=False)
        if should_import:
            
            print "Preparing to import to database..."
            joiner = "." if self._get_config([GLOBAL, USE_SCHEMA], optional=True) else "_"
            table_name = self._get_config([GLOBAL, NAME]) + joiner + table_name
            database_settings = self._get_config([GLOBAL, DB_SETTINGS])
            db.make_schema(df, table_name, database_settings)
            encoding = self._get_config([GLOBAL, ENCODING], default="utf-8", optional=True)
            db.write_table(file_path, table_name, cols, database_settings, encoding)
            print "** DB import complete..."

    # todo refactor
    # def _import_to_db(self, table_name, df):
    #     from sqlalchemy import create_engine
    #     should_import = self._get_config([GLOBAL, DB_IMPORT], optional=True, default=False)
    #     if should_import:
    #         print "Preparing to import to database..."
    #         schema_name = self._get_config([GLOBAL, NAME])
    #         database_settings = self._get_config([GLOBAL, DB_SETTINGS])
    #         user = database_settings[USER]
    #         pw = os.environ.get(database_settings[PW_ENV_VAR], None)
    #         host = database_settings[HOST]
    #         dbname = database_settings[DB_NAME]
    #         con_url = 'postgresql://{}:{}@{}/{}'.format(user, pw, host, dbname)
    #         engine = create_engine(con_url, pool_recycle=3600)
    #         df.to_sql(table_name, engine, if_exists="append", chunksize=20000, index=False, schema=schema_name)
    #         print "** DB import complete..."

    def save(self, table_name, tbl, var_map=None):
        print "** Saving", table_name, "..."
        mode = self._get_config([GLOBAL, MODE], optional=True)
        name = self._get_config([GLOBAL, NAME])
        print mode, "=mode"
        if mode == PREVIEW:
            self._str_save(table_name, tbl, var_map=var_map)
        else:
            output_path = self._output_str(var_map)

            # -- check if output path exists, if not create it
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            # file_name = table_name + ".tsv.bz2"
            file_name = "{}_{}.tsv".format(name, table_name)
            new_file_path = os.path.abspath(os.path.join(output_path, file_name))
            encoding = self._get_config([GLOBAL, ENCODING], optional=True, default="utf-8-sig")
            if not encoding:
                encoding=None
            tbl.to_csv(new_file_path, sep="\t", index=False, encoding=encoding)
            print "** Save complete."
            
            self._import_to_db(tbl, new_file_path, table_name, tbl.columns)
            
