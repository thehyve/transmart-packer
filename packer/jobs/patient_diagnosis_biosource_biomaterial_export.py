import logging
from zipfile import ZipFile
import requests
from celery.exceptions import Ignore
from ..table_transformations.patient_diagnosis_biosource_biomaterial import from_obs_json_to_pdbb_df

from packer.file_handling import FSHandler
from packer.task_status import Status
from ..config import transmart_config
from ..tasks import BaseDataTask, app

logger = logging.getLogger(__name__)
SEP = '\t'


@app.task(bind=True, base=BaseDataTask)
def patient_diagnosis_biosource_biomaterial_export(self: BaseDataTask, constraint, **custom_name):
    """
    Reformat hypercube data into a specific export file.
    Export table with the following IDs: Patient, Diagnosis, Biosource, Biomaterial

    :param self: Required for bind to BaseDataTask
    :param constraint: should be in job_parameters.
    :param custom_name: optional in job_parameters.
    """
    handle = f'{transmart_config.get("host")}/v2/observations'
    logger.info(f'Getting data from observations from {handle!r} for a job named {custom_name}.')
    self.update_status(Status.FETCHING, f'Getting data from observations from {handle!r}')
    r = requests.post(url=handle,
                      json={'type': 'clinical', 'constraint': constraint},
                      headers={
                          'Authorization': self.request.get('Authorization')
                      })

    if r.status_code == 401:
        logger.error('Export failed. Unauthorized.')
        self.update_status(Status.FAILED, 'Unauthorized.')
        raise Ignore()

    if r.status_code != 200:
        logger.error('Export failed. Error occurred.')
        self.update_status(Status.FAILED, f'Connection error occurred when fetching {handle}. '
                                          f'Response status {r.status_code}')
        raise Ignore()

    self.update_status(Status.RUNNING, 'Observations gotten, transforming.')
    reformatted_obs = from_obs_json_to_pdbb_df(r.json())

    self.update_status(Status.RUNNING, 'Writing export to disk.')
    with FSHandler(self.task_id).writer as writer:
        with ZipFile(writer, 'w') as data_zip:
            data_zip.writestr(f'{custom_name}.tsv', reformatted_obs.to_csv(encoding='utf-8', sep=SEP, index=False))

    logger.info(f'Stored to disk: {self.task_id}')
