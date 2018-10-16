import datetime as dt
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)
try:
    from transmart.api.v2.data_structures import ObservationSet
except ImportError as e:
    logging.warning(f'Import errors for {__file__!r}: {str(e)}')

DATE_FORMAT = '%d-%m-%Y'
IDENTIFYING_COLUMN_DICT = {'patient.trial': 'Patient Id',
                           'PMC Diagnosis ID': 'Diagnosis Id',
                           'PMC Biosource ID': 'Biosource Id',
                           'PMC Biomaterial ID': 'Biomaterial Id'}


def from_obs_json_to_pdbb_df(obs_json):
    """
    :param obs_json: json returned by transmart v2/observations call
    :return: data frame that has 4 (patient, diagnosis, biosource, biomaterial) index columns.
    The rest of columns represent concepts (aka variables)
    """

    obs = ObservationSet(obs_json).dataframe

    return from_obs_df_to_pdbb_df(obs)


def from_obs_df_to_pdbb_df(obs):
    if obs.empty:
        logger.warn('Retrieved hypercube is empty! Exporting empty result.')
        return obs
    # order rows by concept_paths:
    # 1)Patient -> 2)Diagnosis -> 3)Biosource -> 4)Biomaterial -> 5)Studies
    obs.sort_values(by=['concept.conceptPath'], inplace=True)
    concept_order = obs['concept.name'].unique().tolist()
    id_column_dict = get_identifying_columns(obs)

    # reformat columns: rename, drop, merge
    logger.info(f'Renaming columns: {id_column_dict}')
    obs.rename(columns=id_column_dict, inplace=True)
    id_columns = list(id_column_dict.values())
    logger.info('Reformatting columns...')
    obs = reformat_columns(obs, id_columns)
    # transform concept rows to column headers
    obs_pivot = concepts_row_to_columns(obs, concept_order)

    # propagate data to lower levels and display only rows that represent the lowest level
    obs_pivot = merge_redundant_rows(obs_pivot, id_columns)
    obs_pivot.reset_index(drop=True, inplace=True)
    obs_pivot = obs_pivot.rename_axis(None)

    # fill NaNs with empty string
    obs_pivot.fillna('', inplace=True)
    obs_pivot.columns.name = None

    # update integer fields - downcasted to float by ffill function
    # (NaN does not have an integer representation)
    obs_pivot = update_integer_fields(obs_pivot)

    # update date fields with format
    # update_date_fields(obs_pivot, obs)

    return obs_pivot


def drop_higher_level(lowest_level_column, data):
    lowest_level = data[data[lowest_level_column].notnull()]
    column_index = data.columns.get_loc(lowest_level_column)
    for index, row in lowest_level.iterrows():
        if column_index == 3:
            conditions = ((data[data.columns[0]] == row[0]) &
                          (data[data.columns[1]] == row[1]) &
                          (data[data.columns[2]] == row[2]) &
                          (data[data.columns[3]].isnull()))
        elif column_index == 2:
            conditions = ((data[data.columns[0]] == row[0]) &
                          (data[data.columns[1]] == row[1]) &
                          (data[data.columns[2]].isnull()))
        elif column_index == 1:
            conditions = ((data[data.columns[0]] == row[0]) &
                          (data[data.columns[1]].isnull()))
        else:
            return
        data.drop(data[conditions].index, inplace=True)


def limit_rows_to_lowest_level(data, id_columns):
    logger.info('Removing redundant rows...')
    lowest_level_col_id = len(id_columns) - 1
    for idx, column in enumerate(reversed(id_columns)):
        if idx == lowest_level_col_id:
            break
        drop_higher_level(column, data)


def merge_redundant_rows(data, id_columns):
    # sort rows by identifying columns, merging of rows strongly depends on sorting
    data.sort_values(id_columns, na_position='first', inplace=True)
    grouped_data = data.groupby(id_columns[0])
    # propagate data to lower levels
    ffill_data = grouped_data.ffill()
    # limit rows to the lowest level
    limit_rows_to_lowest_level(ffill_data, id_columns)
    # drop rows with duplicated identifying columns, keep only the last one
    ffill_data.drop_duplicates(subset=id_columns, keep='last', inplace=True)
    return ffill_data


def update_integer_fields(data):
    for col in data.columns:
        data[col] = data[col].apply(to_int)
    return data


def to_int(x):
    try:
        return int(x)
    except:
        return x


def get_date_string(timestamp, string_format=DATE_FORMAT):
    if pd.notnull(timestamp) and timestamp is not None and timestamp != '':
        return dt.datetime.utcfromtimestamp(float(timestamp) / 1000).strftime(string_format)
    else:
        return timestamp


def update_date_fields(data, obs):
    for column in data.columns:
        if column.upper().startswith('DATE'):
            obs[column] = data[column].apply(get_date_string)


def reformat_columns(obs, id_columns):
    # rename columns and set indexes
    obs.reset_index(inplace=True)
    headers = np.append(id_columns, 'concept.name')

    # prepare 'value' column
    if {'stringValue', 'numericValue'}.issubset(obs.columns):
        # merge stringValue and numericValue into value column
        obs['value'] = obs['stringValue'].fillna(obs['numericValue'])
        obs.drop(['stringValue', 'numericValue'], axis=1, inplace=True)
    elif 'stringValue' in obs:
        obs.rename(columns={"stringValue": "value"}, inplace=True)
    elif 'numericValue' in obs:
        obs.rename(columns={"numericValue": "value"}, inplace=True)
    else:
        obs['value'] = ""
    obs = obs.set_index(list(headers), append=True)[['value']]
    return obs


def concepts_row_to_columns(obs, concept_order):
    # use unstack to move the last level of the index to column names
    obs_pivot = obs.unstack(level=-1)
    # update column names by dropping value level
    obs_pivot.columns = obs_pivot.columns.droplevel(level=0)
    # fix the order of concept columns
    obs_pivot = obs_pivot[concept_order]
    # fix indexes
    obs_pivot.reset_index(inplace=True)
    obs_pivot.drop(obs_pivot.columns[[0]], axis=1, inplace=True)
    return obs_pivot


def get_identifying_columns(obs):
    columns = {}
    for k, v in IDENTIFYING_COLUMN_DICT.items():
        if k in obs:
            columns.update({k: v})
    return columns
