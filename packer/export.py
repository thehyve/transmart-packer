import csv
import pandas as pd

from packer.file_handling import FSHandler
from zipfile import ZipFile


def save(export_df: pd.DataFrame, task_id: str, file_name: str, sep: str = '\t'):
    '''
    Writes dataframe including it's index columns to a file
    :param task_id: id of the task that indicates directory to store file to
    :param export_df: dataframe to write to file
    :param file_name: name of the file to export data to
    :param sep: separator in CSV file. Tab by default.
    '''
    with FSHandler(task_id).writer as writer:
        with ZipFile(writer, 'w') as data_zip:
            data_zip.writestr(f'{file_name}.tsv',
                              export_df.reset_index(drop=True).to_csv(encoding='utf-8', sep=sep, index=False,
                                                                      quoting=csv.QUOTE_NONNUMERIC, quotechar='"'))