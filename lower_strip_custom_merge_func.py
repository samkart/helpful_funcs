"""
A custom merge function that lowers and strips the keys prior to join
"""

import pandas as pd


def lower_strip_merge_df(left_df: pd.DataFrame,
                         right_df: pd.DataFrame,
                         merge_type: string,
                         on=[], left_on=[], right_on=[]) -> pd.DataFrame:

    assert merge_type in ['left', 'right', 'inner', 'outer', 'cross'], \
        'Only "left", "right", "inner", "outer", "cross" joins accepted'

    assert ((len(on) > 0) and (len(left_on) + len(right_on) == 0)) or \
           ((len(on) == 0) and (len(left_on) > 0) and (len(right_on) > 0)), \
        'merge column configuration not satisfied. use either "on" or "left_on + right_on"'

    if merge_type == 'cross':
        on = []
        left_on = []
        right_on = []

    left_copy_df = left_df.copy()
    right_copy_df = right_df.copy()

    if len(on) != 0:
        new_on = []
        for c in on:
            left_copy_df[c + '_cleanjoin'] = left_copy_df[c].str.lower().str.strip() \
                if left_copy_df[c].dtypes == 'object' else left_copy_df[c]

            right_copy_df[c + '_cleanjoin'] = right_copy_df[c].str.lower().str.strip() \
                if right_copy_df[c].dtypes == 'object' else right_copy_df[c]

            new_on.append(c + '_cleanjoin')

    else:
        new_left_on = []
        for c in left_on:
            left_copy_df[c + '_cleanjoin'] = left_copy_df[c].str.lower().str.strip() \
                if left_copy_df[c].dtypes == 'object' else left_copy_df[c]

            new_left_on.append(c + '_cleanjoin')

        new_right_on = []
        for c in right_on:
            right_copy_df[c + '_cleanjoin'] = right_copy_df[c].str.lower().str.strip() \
                if right_copy_df[c].dtypes == 'object' else right_copy_df[c]

            new_right_on.append(c + '_cleanjoin')

    merged_df = left_copy_df. \
        merge(right_copy_df, how=merge_type,
              on=None if len(on) == 0 else new_on,
              left_on=None if len(left_on) == 0 else new_left_on,
              right_on=None if len(right_on) == 0 else new_right_on,
              suffixes=('_left', '_right')
              )

    merged_df = merged_df. \
        drop(columns=[k for k in merged_df.columns if '_cleanjoin' in k])

    merged_df = merged_df. \
        drop(columns=[k for k in merged_df.columns if k.endswith('_left')] if merge_type == 'right' else
                     [k for k in merged_df.columns if k.endswith('_right')]
             ). \
        rename(columns={k: k[:(len(k) - (5 if k.endswith('_left') else 6))] for k in merged_df.columns
                        if k.endswith('_left') or k.endswith('_right')}
               )

    return merged_df


pd.DataFrame.lower_strip_merge = lower_strip_merge_df
