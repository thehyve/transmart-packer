import logging

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


@app.task(bind=True, base=BaseDataTask)
def export_maxima(self: BaseDataTask, constraint):
    """
    Example task that does basic additions of two integers.

    :param self: Required for bind to BaseDataTask
    :param constraint: should be in job_parameters.
    """
    handle = f'{transmart_config.get("host")}/v2/observations'
    self.update_status(Status.FETCHING, f'Getting data from observations from {handle!r}')
    r = requests.get(url=handle,
                     params=dict(
                         constraint=constraint,
                         type='clinical'),
                     headers={
                         'Authorization': self.request.get('Authorization')
                     })

    self.update_status(Status.RUNNING, 'Observations gotten, transforming.')
    obs = ObservationSet(r.json()).dataframe

    self.update_status(Status.RUNNING, 'Writing export to disk.')

    with FSHandler(self.task_id).writer as writer:
        obs.to_csv(writer, sep='\t', index=False)

    logger.info(f'Stored to disk: {self.task_id}')
