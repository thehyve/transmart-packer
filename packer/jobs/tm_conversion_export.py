import logging
import datetime as dt
import pandas as pd
import numpy as np
import requests

try:
    from transmart.api.v2.data_structures import ObservationSet
except ImportError as e:
    logging.warning(f'Import errors for {__file__!r}: {str(e)}')

from packer.file_handling import FSHandler
from packer.task_status import Status
from ..config import transmart_config
from ..tasks import BaseDataTask, app

logger = logging.getLogger(__name__)
SEP = '\t'
DATE_FORMAT = '%d-%m-%Y'
IDENTIFYING_COLUMN_LIST = ['Patient ID', 'Diagnosis ID', 'Biosource ID', 'Biomaterial ID']


@app.task(bind=True, base=BaseDataTask)
def tm_conversion_export(self: BaseDataTask, constraint):
    """
    Reformat hypercube data into a specific export file.

    :param self: Required for bind to BaseDataTask
    :param constraint: should be in job_parameters.
    """
    handle = f'{transmart_config.get("host")}/v2/observations'
    self.update_status(Status.FETCHING, f'Getting data from observations from {handle!r}')
    r = requests.post(url=handle,
                      json={'type': 'clinical', 'constraint': constraint},
                      headers={
                             'Authorization': self.request.get('Authorization')
                      })

    if r.status_code == 401:
        logging.warning(f'Unauthorized')
        self.update_status(Status.FAILED, 'Unauthorized.')
        return

    self.update_status(Status.RUNNING, 'Observations gotten, transforming.')
    obs = ObservationSet(r.json()).dataframe
    reformatted_obs = reformat_export(obs)

    self.update_status(Status.RUNNING, 'Writing export to disk.')

    with FSHandler(self.task_id).writer as writer:
        reformatted_obs.to_csv(writer, sep='\t', index=False)

    logger.info(f'Stored to disk: {self.task_id}')


def reformat_export(obs):
    if obs.empty:
        print('Retrieved hypercube is empty!')
        return obs

    # order rows by concept_paths:
    # 1)Patient -> 2)Diagnosis -> 3)Biosource -> 4)Biomaterial -> 5)Studies
    obs.sort_values(by=['concept.conceptPath'], inplace=True)
    concept_order = obs['concept.name'].unique().tolist()

    # reformat columns: rename, drop, merge
    obs = reformat_columns(obs)
    # transform concept rows to column headers
    obs_pivot = concepts_row_to_columns(obs, concept_order)

    # propagate data to lower levels and display only rows that represent the lowest level
    obs_pivot = rebuild_rows(obs_pivot)
    obs_pivot.reset_index(drop=True, inplace=True)

    # fill NaNs with empty string
    obs_pivot.fillna('', inplace=True)

    # update date fields with format
    update_date_fields(obs_pivot, obs)
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
                          (data[data.columns[2]].isnull()) &
                          (data[data.columns[3]].isnull()))
        else:
            conditions = ((data[data.columns[0]] == row[0]) &
                          (data[data.columns[1]].isnull()) &
                          (data[data.columns[2]].isnull()) &
                          (data[data.columns[3]].isnull()))
        data.drop(data[conditions].index, inplace=True)


def limit_rows_to_lowest_level(data):
    drop_higher_level('Biomaterial ID', data)
    drop_higher_level('Biosource ID', data)
    drop_higher_level('Diagnosis ID', data)


def rebuild_rows(data):
    # sort rows by identifying columns
    data.sort_values(IDENTIFYING_COLUMN_LIST, na_position='first', inplace=True)
    grouped_data = data.groupby('Patient ID')
    # propagate data to lower levels
    ffill_data = grouped_data.ffill()
    # limit rows to the lowest level
    limit_rows_to_lowest_level(ffill_data)
    return ffill_data


def get_date_string(timestamp, string_format=DATE_FORMAT):
    if pd.notnull(timestamp) and timestamp is not None and timestamp != '':
        return dt.datetime.utcfromtimestamp(float(timestamp) / 1000).strftime(string_format)
    else:
        return timestamp


def update_date_fields(data, obs):
    for column in data.columns:
        if column.upper().startswith('DATE'):
            obs[column] = data[column].apply(get_date_string)


def reformat_columns(obs):
    # rename columns and set indexes
    obs.rename(columns={'patient.trial': 'Patient ID'}, inplace=True)
    obs.reset_index(inplace=True)
    headers = np.append(IDENTIFYING_COLUMN_LIST, 'concept.name')

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
