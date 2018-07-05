import json
import abc

from packer.redis_client import redis


class Status:
    REGISTERED = 'REGISTERED'
    CANCELLED = 'CANCELLED'
    FETCHING = 'FETCHING'
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


class TaskStatusABC(metaclass=abc.ABCMeta):

    def __init__(self, task_id):
        self.task_id = task_id
        self.key = f'job_status:{self.task_id}'

    @abc.abstractmethod
    def create(self, **kwargs):
        pass

    @abc.abstractmethod
    def update(self, **kwargs):
        pass

    @abc.abstractmethod
    def get(self):
        pass


class TaskStatus(TaskStatusABC):

    def create(self, **kwargs):
        kwargs['task_id'] = self.task_id
        redis.set(self.key, json.dumps(kwargs))

    def update(self, **kwargs):
        obj = self.get()
        obj.update(**kwargs)
        self.create(**obj)

    def get(self):
        return json.loads(redis.get(self.key))


class TaskStatusAsync(TaskStatusABC):

    def __init__(self, task_id, redis_loop):
        self.redis = redis_loop
        super().__init__(task_id)

    async def create(self, **kwargs):
        kwargs['task_id'] = self.task_id
        await self.redis.set(self.key, json.dumps(kwargs))

    async def update(self, **kwargs):
        obj = await self.get()
        obj.update(**kwargs)
        await self.create(**obj)

    async def get(self):
        status = await self.redis.get(self.key)
        return json.loads(status)

