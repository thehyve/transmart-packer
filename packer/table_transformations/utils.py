import pandas as pd


def filter_rows(df: pd.DataFrame, filter_row_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: dataframe to apply fitering on
    :param filter_row_df: dataframe which index indicative on what rows should stay
    :return: dataframe as df but that has rows with index that appear in filter_row_df dataframe
    """
    df_indx_names = df.index.names
    filter_row_df_indx_names = filter_row_df.index.names
    common_indx_names = list(set(df_indx_names) & set(filter_row_df_indx_names))
    if not common_indx_names:
        raise ValueError('No index columns in common to filter rows.')
    df_common_indx = df.reset_index().set_index(common_indx_names).index
    filter_df_common_indx = filter_row_df.reset_index().set_index(common_indx_names).index
    filtered_df = df[df_common_indx.isin(filter_df_common_indx)]
    return filtered_df
