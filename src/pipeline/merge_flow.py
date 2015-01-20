from flow import Builder
from consts import *
import pandas as pd
from exceptions import InvalidSettingException

from plugins import secex

class MergeBuilder(Builder):
    def run(self):
        sources = self._get_config([GLOBAL, SOURCE])
        merge_pk = self._get_config([GLOBAL, MERGE_PK])

        suffixes = ["_"+x[LABEL] for x in sources]
        prefixes = [x[LABEL] + "_" for x in sources]

        dataframes = [self._to_df(src[PATH]).groupby(merge_pk).sum() for src in sources]

        merged_df = pd.merge(*dataframes, how='outer', left_index=True, right_index=True, suffixes=suffixes)
        merged_df = self.rename_and_cleanup(merged_df, suffixes, prefixes)
        merged_df = merged_df.reset_index()

        print merged_df.head()

        # print secex.calc_rca(merged_df)

        super(MergeBuilder, self)._run_helper(merged_df)

    def rename_and_cleanup(self, df, suffixes, prefixes):
        cleanup = { }

        merge_vals = self._get_config([GLOBAL, MERGE_VALS], optional=True, default=[])

        if not merge_vals:
            raise InvalidSettingException("Need to specify merge vals!")

        for col in merge_vals:
            for i,suffix in enumerate(suffixes):
                prefix = prefixes[i]
                cleanup[col + suffix] = prefix + "val"

        # print cleanup
        to_drop = set( df.columns ).difference( cleanup.keys() )
        df = df.drop(labels=to_drop, axis=1)

        df = df.rename(columns=cleanup)
        return df