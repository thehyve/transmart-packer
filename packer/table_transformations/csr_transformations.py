import re
from typing import List, Dict, Any

import pandas
from pandas import DataFrame
import numpy
import logging

logger = logging.getLogger(__name__)
try:
    from transmart.api.v2.data_structures import ObservationSet
except ImportError as e:
    logging.warning(f'Import errors for {__file__!r}: {str(e)}')

DATE_FORMAT = '%Y-%m-%d'
PATIENT_ID_FIELD = 'patient.subjectIds.SUBJ_ID'
ID_COLUMN_MAPPING = {PATIENT_ID_FIELD: 'Patient Id',
                     'Diagnosis': 'Diagnosis Id',
                     'Biosource': 'Biosource Id',
                     'Biomaterial': 'Biomaterial Id',
                     'Study': 'Study Id'}
ID_COLUMNS = ID_COLUMN_MAPPING.values()


def from_obs_json_to_export_csr_df(obs_json: Dict) -> DataFrame:
    """
    :param obs_json: json returned by transmart v2/observations call
    :return: data frame that has 4 (patient, diagnosis, biosource, biomaterial) index columns.
    The rest of columns represent concepts (aka variables)
    """

    df = ObservationSet(obs_json).dataframe
    concept_pat_to_name = _concept_path_to_name(df)

    # Transform sample and study data separately
    sample_df = df
    study_df = None
    if 'Study' in df.columns:
        sample_df = df[df['Study'].isnull()]
        sample_df.drop(columns=['Study'], inplace=True)
        study_df = df[df['Study'].notnull()]
        sample_columns = [c for c in ID_COLUMN_MAPPING.keys() - [PATIENT_ID_FIELD, 'Study'] if c in df.columns]
        study_df.drop(columns=sample_columns, inplace=True)
    sample_df = from_obs_df_to_csr_df(sample_df)
    df = sample_df
    if study_df is not None:
        study_df = from_obs_df_to_csr_df(study_df)
        if sample_df.empty:
            df = study_df
        else:
            # Merge sample and study data, creating a cross-product
            study_df['Study Id'] = study_df.index.get_level_values('Study Id')
            df = sample_df.merge(study_df,
                                 on='Patient Id',
                                 how='outer',
                                 right_index=True
                                 )
        # Create combined index with sample and study identifiers
        id_columns = df.index.names + [column for column in ID_COLUMNS if column in set(df.columns)]
        df.reset_index(inplace=True)
        df.set_index(id_columns, inplace=True)

    df = df.rename(index=str, columns=concept_pat_to_name)
    df = format_columns(df)
    return df


def from_obs_df_to_csr_df(obs: DataFrame) -> DataFrame:
    if obs.empty:
        logger.warning('Hypercube is empty! Returning empty result.')
        return obs
    # Rename the identifier columns
    obs.rename(index=str, columns=ID_COLUMN_MAPPING, inplace=True)
    # Sort data by concept path, compute the list concepts for the column headers
    obs.sort_values(by=['concept.conceptPath'], inplace=True)
    concept_path_col = obs['concept.conceptPath']
    unq_concept_paths_ord = concept_path_col.unique().tolist()
    logger.info('Reformatting columns...')
    id_columns = [column for column in ID_COLUMNS if column in set(obs.columns)]
    obs = _reformat_columns(obs, id_columns)
    # Transform concept rows to column headers
    obs_pivot = _concepts_row_to_columns(obs)
    # Propagate data to lower levels and display only rows that represent the lowest level,
    # e.g., add patient-level data to diagnosis rows and remove the patient-level row
    obs_pivot = _merge_redundant_rows(obs_pivot, id_columns)
    # Set columns order to identifiers first and then concepts
    obs_pivot = obs_pivot[id_columns + unq_concept_paths_ord]
    # Replace NAs and NANs in index columns with empty string
    for id_column in id_columns:
        obs_pivot[id_column] = obs_pivot[id_column].fillna('')
    obs_pivot.set_index(id_columns, inplace=True)
    return obs_pivot


def _concept_path_to_name(df):
    return dict(zip(df['concept.conceptPath'], df['concept.name']))


def format_columns(df):
    """
    :param df: pandas dataframe with various data types of columns
    :return: modified data frame with all columns converted to formatted string
    """
    result_df = DataFrame()
    for col_num, col in enumerate(df.columns):
        # update datetime fields
        if re.match(r'.*\bdate\b.*', col, flags=re.IGNORECASE):
            result_df[col_num] = df.iloc[:, col_num].apply(_to_datetime)
        elif numpy.issubdtype(df.iloc[:, col_num].dtype, numpy.number):
            result_df[col_num] = df.iloc[:, col_num].apply(_num_to_str)
        else:
            result_df[col_num] = df.iloc[:, col_num]
    result_df = result_df.fillna('')
    result_df.columns = df.columns
    return result_df


def _num_to_str(x):
    if pandas.isnull(x):
        return ''
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(x)


def _merge_redundant_rows(data: DataFrame, id_columns: List[str]) -> DataFrame:
    if data.empty:
        return data
    # sort rows by identifying columns, merging of rows strongly depends on sorting
    rows = data.sort_values(id_columns, na_position='last').to_dict('records')
    result_rows = [rows[0]]
    for row in rows[1:]:
        row_copied = False
        for result_row in reversed(result_rows):
            if _is_ancestor_row(row, result_row, id_columns):
                _copy_missing_value_to_descendant_row(row, result_row, id_columns)
                row_copied = True
            else:
                break
        if not row_copied:
            result_rows.append(row)
    return DataFrame(result_rows)


def _is_ancestor_row(ancestor_row_candidate: Dict[str, Any],
                     row: Dict[str, Any],
                     id_columns: List[str]) -> bool:
    """
    Checks if the identifier values of the row equal the identifier values
    of the candidate ancestor row, ignoring missing values in the ancestor.
    E.g., {PatientId: 1, DiagnosisId: None} is an ancestor of {PatientId: 1, DiagnosisId: 3},
      {PatientId: 1, DiagnosisId: None, BiosourceId: None} is an ancestor of
      {PatientId: 1, DiagnosisId: None, BiosourceId: 5}.
    :param ancestor_row_candidate: the row to check if it is an ancestor
    :param row: the row to compare against, which should be more specific but not conflicting with the ancestor
    :param id_columns: the id columns to compare (in order)
    :return: true iff the ancestor_row_candidate is an ancestor of the row.
    """
    for id_column in id_columns:
        if pandas.isnull(ancestor_row_candidate[id_column]):
            continue
        if ancestor_row_candidate[id_column] != row[id_column]:
            return False
    return True


def _copy_missing_value_to_descendant_row(ancestor_row, descendant_row, id_columns: List[str]):
    for column, value in ancestor_row.items():
        if column in id_columns or pandas.isnull(value):
            continue
        if column not in descendant_row or pandas.isnull(descendant_row[column]):
            descendant_row[column] = ancestor_row[column]


def _to_datetime(date_str, string_format=DATE_FORMAT):
    if pandas.notnull(date_str) and date_str is not None and date_str != '':
        try:
            return pandas.to_datetime(date_str).strftime(string_format)
        except:
            return date_str
    else:
        return date_str


def _reformat_columns(obs, id_columns: List[str]):
    # rename columns and set indexes
    obs.reset_index(inplace=True)
    headers = id_columns + ['concept.conceptPath']

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
    obs = obs.set_index(headers, append=True)[['value']]
    return obs


def _concepts_row_to_columns(obs: DataFrame) -> DataFrame:
    # use unstack to move the last level of the index to column names
    obs_pivot = obs.unstack(level=-1)
    # update column names by dropping value level
    obs_pivot.columns = obs_pivot.columns.droplevel(level=0)
    # fix indexes
    obs_pivot.reset_index(inplace=True)
    obs_pivot.drop(obs_pivot.columns[[0]], axis=1, inplace=True)
    return obs_pivot
