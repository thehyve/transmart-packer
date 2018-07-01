import logging
import time
import json

from .celery import app
from .redis_client import redis

logger = logging.getLogger(__name__)


@app.task(name='tasks.add')
def add(x, y, id_, channel):
    task_status = TaskStatusGeneric(id_)
    task_status.update(status='RUNNING')

    for i in range(10, 0, -1):
        msg = f'Task {id_} will be ready in {i} seconds'
        logging.info(msg)
        redis.publish(channel, msg)
        time.sleep(1)

    value = x * y
    logger.info('Calculated value: {}'.format(value))
    redis.set(id_, value)
    logger.info('Stored to redis')
    task_status.update(status='FINISHED')

    redis.publish(channel, "END")


class TaskStatusGeneric:

    def __init__(self, job_id):
        self.job_id = job_id
        self.key = f'job_status:{self.job_id}'

    def get(self):
        return json.loads(redis.get(self.key))

    def update(self, **kwargs):
        obj = self.get()
        obj.update(**kwargs)
        self.create(**obj)

    def create(self, **kwargs):
        kwargs['id'] = self.job_id
        redis.set(self.key, json.dumps(kwargs))
