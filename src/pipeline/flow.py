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

from src.pipeline import growth
from src.pipeline.abstract import BaseBuilder

class Builder(BaseBuilder):
    def __init__(self, config):
        self.config = config
        self.db = DB()

        transformers = self._get_config([GLOBAL, "default_transformations"], optional=True, default={})
        self.converters = self._setup_transfomers(transformers)
        
        self.tables = self._get_config(TABLES, optional=True, default={})
        self.aggs = self._build_agg(self._get_config([GLOBAL, AGG], optional=True, default={}))
        self.coerce = self._get_config([GLOBAL, TYPE], optional=True, default={})
        for k in self.coerce:
            self.coerce[k] = eval(self.coerce[k])

        self.nan_rules = self._get_config([GLOBAL, NAN], optional=True, default={})
        td_config = self._get_config([GLOBAL, "transformed_depths"], optional=True, default={})
        self.transformed_depths = self._setup_transfomers(td_config)

    def _setup_transfomers(self, transformers):
        db_converters = {colname: self.db.make_dict(**settings)
                        for colname, settings in transformers.items() if settings[TYPE] == "DBLOOKUP"}
        return db_converters

    def _build_agg(self, setts):
        return {k: getattr(pd.Series, v) for k,v in setts.items()}



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
                    if type(v) == list:
                        for i in v:
                            df = process_operation(df, k, i, agg, pk)
                    else:
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
        raw_input_file = os.path.expandvars(raw_input_file)
        source_setts = self._get_config([GLOBAL, SOURCE_SETTING], optional=True)
        source_vars = self._get_config([GLOBAL, SOURCE_VARS], optional=True)
        concat_mode = self._get_config([GLOBAL, CONCAT_MODE], optional=True, default=False)

        if source_vars:
            s_vars = re.findall("<(\w+)>", raw_input_file)
            ds = {}
            for var in s_vars:
                params = source_vars[var]
                ds[var] = range(params["start"], params["end"] + 1)

            labels = ds.keys()
            combos = itertools.product(*ds.values())
            master_df = pd.DataFrame()
            master_var_map = {}

            for combo in combos:
                var_map = {}
                tmp_file = raw_input_file
                for idx, var in enumerate(labels):
                    val = str(combo[idx])
                    if ZFILL in source_vars[var]:
                        val = val.zfill( source_vars[var][ZFILL] )
                    tmp_file = tmp_file.replace( "<{}>".format(var) , val )
                    var_map[var] = val

                    if concat_mode:
                        if not var in master_var_map:
                            master_var_map[var] = []
                        if not val in master_var_map[var]:
                            master_var_map[var].append(val)


                print tmp_file, combo

                df = self._to_df(tmp_file, var_map=var_map)
                for idx, label in enumerate(labels):
                    df[label] = combo[idx]

                if not concat_mode:
                    self._run_helper(df, var_map=var_map)
                else:
                    print "Joining dataframes..."
                    master_df = pd.concat([master_df, df])

            if concat_mode:
                for key in master_var_map:
                    master_var_map[key] = u'_'.join(master_var_map[key])
                print "Running in CONCAT_MODE..."
                self._run_helper(master_df, var_map=master_var_map)

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

    def _run_helper(self, df, var_map=None):
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
            agg = {k:v for k,v in agg.items()} # deep copy

            ''' Do we need to add any computation before aggregating? '''
            table_conf = self._get_setting(TRANSFORM, setts, None)
            gconf = self._get_config([GLOBAL, TRANSFORM], optional=True)

            mydf = self._computed_columns(gconf, mydf, agg, pk)
            print "Agg=",agg
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
            # TODO: fix growth
            # table = self._do_growth(table, table_name, pk, var_map)

            print table.head(), "final table [head]"
            self.save(table_name, table, var_map=var_map)

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

    # def _do_growth(self, table, table_name, pk, var, var_map):
    #     growth_setts = self._get_config([GLOBAL, SOURCE_SETTING, GROWTH], optional=True, default=None)
    #     if not growth_setts:
    #         print "NO growth in settings. Skiping growth calculations..."
    #         return table
    #     start = self._get_config([GLOBAL, SOURCE_SETTING, START])
        
    #     file_name = table_name + ".tsv.bz2"
    #     for growth_sett in growth_setts:
    #         if not (YEARS in growth_sett and COLUMNS in growth_sett):
    #             raise InvalidSettingException("Need to specify years and columns for growth")

    #         years_ago = growth_sett[YEARS]
    #         growth_cols = growth_sett[COLUMNS]
    #         delta_col = self._get_setting(DELTA, growth_setts, None)

    #         if growth_cols and var >= start + years_ago:

    #             print "Do one year growth calculation..."
    #             output_str = self._output_str(var - years_ago)
    #             growth_path = os.path.abspath(os.path.join(output_str, file_name))
    #             file_prev = get_file(growth_path)
    #             tbl_prev = pd.read_csv(file_prev, sep="\t")
    #             table = growth.do_growth(table, tbl_prev, pk, growth_cols, years_ago=years_ago, delta_col=delta_col)
            
    #     return table


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