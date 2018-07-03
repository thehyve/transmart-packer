import logging
import time

from ..tasks import BaseDataTask, Status, app, FSHandler

logger = logging.getLogger(__name__)


@app.task(bind=True, base=BaseDataTask)
def add(self: BaseDataTask, x: int, y: int) -> None:
    """
    Example task that does basic additions of two integers.

    :param self: first argument is always required for bind to BaseDataTask.
    :param x: should be in job_parameters.
    :param y: should be in job_parameters.
    """

    for i in range(10, 0, -1):
        msg = f'Task will be ready in {i} seconds'
        self.update_status(Status.RUNNING, msg)
        time.sleep(1)

    value = x + y
    logger.info('Calculated value: {}'.format(value))

    with FSHandler(self.task_id).writer as writer:
        writer.write(f'{value}')

    logger.info(f'Stored to disk.')

