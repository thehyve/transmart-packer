import logging
import time

from ..tasks import BaseDataTask, Status, app

logger = logging.getLogger(__name__)


@app.task(bind=True, base=BaseDataTask)
def add(self, x, y):
    """
    Example task that does basic additions of two integers.

    :param self: Required for bind to BaseTask
    :param x: should be in job_parameters.
    :param y: should be in job_parameters.
    """

    for i in range(10, 0, -1):
        msg = f'Task will be ready in {i} seconds'
        self.update_status(Status.RUNNING, msg)
        time.sleep(1)

    value = x * y
    logger.info('Calculated value: {}'.format(value))
    self.task_status.update(result=value)
    logger.info('Stored to redis.')
