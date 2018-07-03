import logging
import time
from transmart.api.v2.data_structures import ObservationSet
import requests
import json

from ..config import transmart_config
from ..tasks import BaseDataTask, Status, app

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

    with self.open_writer('observations.tsv') as writer:
        obs.to_csv(writer, sep='\t', index=False)

    logger.info(f'Stored to disk: {self.task_id}')
