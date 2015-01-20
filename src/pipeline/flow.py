import pandas as pd
import bz2
import os
from database import DB
import itertools

from kungfu import process_operation
import re

from src.pipeline.consts import *
from src.pipeline.exceptions import InvalidSettingException
from src.pipeline._table_manipulation import add_column_lengths, add_summary_rows

from src.plugins import rca_helper
from src.fetcher.fetch import grab_if_needed
from src.plugins.helpers import get_file

from src.pipeline import growth

class Builder(object):
    def __init__(self, config):
        self.config = config
        self.db = DB()

        transformers = self._get_config([GLOBAL, "default_transformations"], optional=True, default={})
        self.converters = self._setup_transfomers(transformers)
        
        self.tables = self._get_config(TABLES)
        self.aggs = self._build_agg(self._get_config([GLOBAL, AGG], optional=True, default={}))
        self.coerce = self._get_config([GLOBAL, TYPE], optional=True, default={})
        for k in self.coerce:
            self.coerce[k] = eval(self.coerce[k])

        self.nan_rules = self._get_config([GLOBAL, NAN], optional=True, default={})
        td_config = self._get_config([GLOBAL, "transformed_depths"], optional=True, default={})
        self.transformed_depths = self._setup_transfomers(td_config)

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

    def _setup_transfomers(self, transformers):
        db_converters = {colname: self.db.make_dict(**settings)
                        for colname, settings in transformers.items() if settings[TYPE] == "DBLOOKUP"}
        return db_converters

    def _build_agg(self, setts):
        return {k: getattr(pd.Series, v) for k,v in setts.items()}

    def _get_setting(self, name, setts, default=None, optional=True):
        if name in setts:
            return setts[name]
        if not optional:
            raise InvalidSettingException("Couldn't find %s in %s" % (name, setts))
        return default

    def _depth_combos(self, df, agg, pk, setts):
        addtl_rows = pd.DataFrame()
        if not "depths" in setts:
            return addtl_rows
        
        my_nesting = []
        my_nesting_cols = []
        if "depths" in setts:
            depths = setts["depths"]
            for depthcol, lengths in depths.items():
                my_nesting.append(lengths)
                my_nesting_cols.append(depthcol)

        # print my_nesting, my_nesting_cols
        for depths in itertools.product(*my_nesting):    
            my_pk = []
            for col_name, l in zip(my_nesting_cols, depths):
                if type(l) in [str, unicode]:
                    transformation = self.transformed_depths[l]
                    my_pk.append( df[col_name].map(transformation) )
                elif l is True:
                    my_pk.append(col_name)
                else:
                    my_pk.append(df[col_name].str.slice(0, l))
            table = df.groupby(my_pk).agg(agg)
            addtl_rows = pd.concat([addtl_rows, table])
        return addtl_rows

    def _computed_columns(self, computed, df, agg=None, pk=[]):
        # print "COMP", computed, pk
        if computed:
            if type(computed) == dict:
                computed = [computed]
            for c in computed:
                for k,v in c.items():
                    df = process_operation(df, k, v, agg, pk)
        return df

    def _compute_lengths(self, should_compute, table, pk, table_conf):
        if should_compute:
            return add_column_lengths(table, pk, table_conf)
        return table

    def _add_summary_rows(self, table, pk, gconf):
        if gconf:
            return add_summary_rows(table, pk, gconf)
        return table

    def run(self):
        raw_input_file = self._get_config([GLOBAL, SOURCE])
        source_setts = self._get_config([GLOBAL, SOURCE_SETTING], optional=True)
        source_regex = self._get_config([GLOBAL, SOURCE_REGEX], optional=True)

        if source_regex:
            print source_regex
            match = re.match(source_regex, raw_input_file)
            if not match:
                raise InvalidSettingException("Fail to match regex", source_regex, raw_input_file)
            parameters = match.groupdict()
            print parameters
            raise Exception("Not fully implemented yet!", match)
        elif not source_setts:
            df = self._to_df(raw_input_file)
            self._run_helper(df)
        else:
            if not "%s" in raw_input_file:
                raise InvalidSettingException("Using a variable input, but missing string formatter") 
            curr = self._get_config([GLOBAL, SOURCE_SETTING, START])
            end = self._get_config([GLOBAL, SOURCE_SETTING, END])
            step = self._get_config([GLOBAL, SOURCE_SETTING, STEP], optional=True, default=1)
            while curr <= end:
                input_file = raw_input_file % (curr)
                df = self._to_df(input_file)

                self._run_helper(df, var=curr)
                curr += step

    def _run_helper(self, df, var=None):
        print "Starting!"

        print "Renaming DF columns..."
        df = self._apply_renames(df)

        print "Running converters..."
        for x, colmap in self.converters.items():
            df[x] = df[x].map(colmap)

        print "Replacing null values..."
        for x,v in self.nan_rules.items():
            df[x] = df[x].fillna(v)

        print "Creating tables..."
        for table_name, setts in self.tables.items():
            mydf = df.copy()
            print "Doing table:", table_name
            print mydf.columns
            pk = setts["pk"]
            agg = self._get_setting(AGG, setts)
            agg = self.aggs if not agg else self._build_agg(agg)
            ''' Do we need to add any computation before aggregating? '''
            table_conf = self._get_setting(TRANSFORM, setts, None)
            gconf = self._get_config([GLOBAL, TRANSFORM], optional=True)

            mydf = self._computed_columns(gconf, mydf, agg, pk)
            mydf = self._computed_columns(table_conf, mydf, agg, pk)

            if "depths" in setts:
                table = self._depth_combos(mydf, agg, pk, setts)
            else:
                if not agg: raise InvalidSettingException("Need to specify agg in settings!")
                table = mydf.groupby(pk).agg(agg)

            gconf = self._get_config([GLOBAL, POST_AGG_TRANSFORM], optional=True)
            table = self._computed_columns(gconf, table, agg, pk)

            gconf = self._get_config([GLOBAL, POST_AGG_PKLENGTHS], optional=True)
            table = self._compute_lengths(gconf, table, pk, setts)

            gconf = self._get_config([GLOBAL, SUMMARY_ROWS], optional=True)
            table = self._add_summary_rows(table, pk, gconf)

            tconf = self._get_setting(RCA, setts, None)
            table = self._calc_rca(table, setts, tconf)
            table = table.reset_index()
            table = self._do_growth(table, table_name, pk, var)

            print table.head(), "final table [head]"
            self.save(table_name, table, var=var)

        return df

    def _apply_renames(self, df):
        rename_map = self._get_config([GLOBAL, RENAME], optional=True, default={})
        return df.rename(columns=rename_map)

    def _calc_rca(self, table, table_setts, gconf):
        if gconf:
            depths = self._get_setting(DEPTHS, table_setts, None, optional=False)
            tmp = self._crossproduct(table, depths)
            return rca_helper.calc_rca(table, depths=tmp, **gconf)
        return table

    def _check_file(self, file_path):
        output_path = self._get_config([GLOBAL, OUTPUT])

        gconf = self._get_config([GLOBAL])
        if FTP_PATHS in gconf or WEB_PATHS in gconf:
            print "Attempting file and (if needed) FTP check", output_path
            grab_if_needed(file_path, gconf)

    def _check_hdf_cache(self, input_file, var):
        output_path = self._get_config([GLOBAL, OUTPUT])

        print "CHECK HDF"
        print input_file
        print output_path
        file_name = os.path.basename(input_file)
        target = os.path.join(output_path, file_name + ".h5")

        if not os.path.exists(output_path):
            # -- make directory if it doesn't exist
            os.makedirs(output_path)

        if not os.path.exists(target):
            return (False, target)
        df = pd.read_hdf(target, HDF_CACHE)
        return (df, target)

    def _to_df(self, input_file, var=None):
        hdf_df, target = self._check_hdf_cache(input_file, var)
        if hdf_df is not False:
            print "Reading from HDF file..."
            return hdf_df

        self._check_file(input_file)
        print "Opening file..."
        input_file = get_file(input_file)

        delim = self._get_config([GLOBAL, SEPERATOR], optional=True, default=";")
        dec = self._get_config([GLOBAL, DECIMAL], optional=True, default=",")
        encoding = self._get_config([GLOBAL, ENCODING], optional=True, default="utf-8-sig")
        df = pd.read_csv(input_file, header=0, sep=delim, encoding=encoding, decimal=dec, converters=self.coerce)
        print "Saving dataframe in HDF file..."
        df.to_hdf(target, HDF_CACHE, append=False)

        return df


    def _do_growth(self, table, table_name, pk, var):
        growth_setts = self._get_config([GLOBAL, SOURCE_SETTING, GROWTH], optional=True, default=None)
        if not growth_setts:
            print "NO growth in settings. Skiping growth calculations..."
            return table
        start = self._get_config([GLOBAL, SOURCE_SETTING, START])
        
        file_name = table_name + ".tsv.bz2"
        for growth_sett in growth_setts:
            if not (YEARS in growth_sett and COLUMNS in growth_sett):
                raise InvalidSettingException("Need to specify years and columns for growth")

            years_ago = growth_sett[YEARS]
            growth_cols = growth_sett[COLUMNS]
            delta_col = self._get_setting(DELTA, growth_setts, None)

            if growth_cols and var >= start + years_ago:

                print "Do one year growth calculation..."
                output_str = self._output_str(var - years_ago)
                growth_path = os.path.abspath(os.path.join(output_str, file_name))
                file_prev = get_file(growth_path)
                tbl_prev = pd.read_csv(file_prev, sep="\t")
                table = growth.do_growth(table, tbl_prev, pk, growth_cols, years_ago=years_ago, delta_col=delta_col)
            
        return table

    def _output_str(self, var=None):
        output_path = self._get_config([GLOBAL, OUTPUT])
        if var:
            output_path = os.path.join(output_path, str(var))
        return output_path

    def save(self, table_name, tbl, var=None):
        print "** Saving", table_name, "..."
        
        output_path = self._output_str(var)

        # -- check if output path exists, if not create it
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        file_name = table_name + ".tsv.bz2"
        new_file_path = os.path.abspath(os.path.join(output_path, file_name))
        encoding = self._get_config([GLOBAL, ENCODING], optional=True, default="utf-8-sig")
        tbl.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep="\t", index=False, encoding=encoding)
        print "** Save complete."
        self._import_to_db(new_file_path) 


    def _import_to_db(self, file_path):
        should_import = self._get_config([GLOBAL, DB_IMPORT], optional=True, default=False)
        if should_import:
            print "Preparing to import to database..."
            _importer(file_path)

    def _crossproduct(self, df, depths):
        ''' Given a dictionary of Column => Nesting Length, generate all possible PK combinations '''
        my_nesting = []
        my_nesting_cols = []
        primary_keys = []
        df = df.reset_index()
        for depthcol, lengths in depths.items():
            my_nesting.append(lengths)
            my_nesting_cols.append(depthcol)

        for depths in itertools.product(*my_nesting):
            my_pk = []
            for col_name, l in zip(my_nesting_cols, depths):
                if type(l) in [str, unicode]:
                    transformation = self.transformed_depths[l]
                    tlen = df[col_name].map(transformation).str.len().max()
                    my_pk.append(df[col_name].str.len() == tlen)
                elif l is True:
                    maxlen = df[col_name].str.len().max()
                    my_pk.append(df[col_name].str.len() == maxlen)
                else:
                    my_pk.append(df[col_name].str.len() == l)
            primary_keys.append(my_pk) 
        return primary_keys