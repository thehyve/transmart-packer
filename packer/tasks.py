import abc
import json
import logging
import os
from celery import Celery, Task
from celery.utils import cached_property

from .config import redis_config, task_config
from .redis_client import redis

logger = logging.getLogger(__name__)


app = Celery('tasks', backend=redis_config['address'], broker=redis_config['address'])
app.autodiscover_tasks(['packer.jobs'], 'jobs')


class Status:
    REGISTERED = 'REGISTERED'
    FETCHING = 'FETCHING'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


class BaseDataTask(Task, metaclass=abc.ABCMeta):

    def on_success(self, retval, task_id, args, kwargs):
        """Success handler.

        Run by the worker if the task executes successfully.

        Arguments:
            retval (Any): The return value of the task.
            task_id (str): Unique id of the executed task.
            args (Tuple): Original arguments for the executed task.
            kwargs (Dict): Original keyword arguments for the executed task.

        Returns:
            None: The return value of this handler is ignored.
        """
        self.update_status(status=Status.SUCCESS, message='Task finished successfully.')

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Retry handler.

        This is run by the worker when the task is to be retried.

        Arguments:
            exc (Exception): The exception sent to :meth:`retry`.
            task_id (str): Unique id of the retried task.
            args (Tuple): Original arguments for the retried task.
            kwargs (Dict): Original keyword arguments for the retried task.
            einfo (~billiard.einfo.ExceptionInfo): Exception information.

        Returns:
            None: The return value of this handler is ignored.
        """

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Error handler.

        This is run by the worker when the task fails.

        Arguments:
            exc (Exception): The exception raised by the task.
            task_id (str): Unique id of the failed task.
            args (Tuple): Original arguments for the task that failed.
            kwargs (Dict): Original keyword arguments for the task that failed.
            einfo (~billiard.einfo.ExceptionInfo): Exception information.

        Returns:
            None: The return value of this handler is ignored.
        """
        self.update_status(status=Status.FAILED, message=f'Task failed: {exc}')

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        """Handler called after the task returns.

        Arguments:
            status (str): Current task state.
            retval (Any): Task return value/exception.
            task_id (str): Unique id of the task.
            args (Tuple): Original arguments for the task.
            kwargs (Dict): Original keyword arguments for the task.
            einfo (~billiard.einfo.ExceptionInfo): Exception information.

        Returns:
            None: The return value of this handler is ignored.
        """

    def __call__(self, *args, **kwargs):
        self.update_status(status=Status.RUNNING, message=f'Starting task.')
        super().__call__(*args, **kwargs)

    def get_data_dir(self, create=True):
        path = os.path.join(task_config['data_dir'], self.task_id)
        if create:
            os.makedirs(path, exist_ok=True)
        return path

    @property
    def task_id(self):
        return self.request.id

    @property
    def task_status(self):
        return TaskStatusGeneric(self.task_id)

    @property
    def channel(self):
        obj = self.task_status.get()
        return f'channel:{obj.get("user")}'

    def update_status(self, status, message):
        """
        Send status update message through websocket, update job status in Redis.

        :param status: status code.
        :param message: message for client.
        """
        self.task_status.update(status=status, message=message)
        logging.info(f'Status update for {self.task_id}: {message} ({status})')
        redis.publish(
            self.channel,
            {
                'task_id': self.task_id,
                'status': status,
                'message': message
            }
        )


class TaskStatusGeneric:

    def __init__(self, task_id):
        self.task_id = task_id
        self.key = f'job_status:{self.task_id}'

    def get(self):
        return json.loads(redis.get(self.key))

    def update(self, **kwargs):
        obj = self.get()
        obj.update(**kwargs)
        self.create(**obj)

    def create(self, **kwargs):
        kwargs['task_id'] = self.task_id
        redis.set(self.key, json.dumps(kwargs))
