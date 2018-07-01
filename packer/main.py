import asyncio
import json
import logging
import uuid
import os

import aioredis
import jwt
import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.log import app_log as log
from tornado.options import define

import packer.jobs as jobs
from .config import tornado_config, redis_config, task_config
from .redis_client import redis
from .tasks import TaskStatusGeneric, Status

USER_COOKIE = 'packer-user'  # TODO remove, using session cookie for testing now


class BaseHandler(tornado.web.RequestHandler):

    def get_subject_from_jwt(self):
        if self.request.headers.get("Authorization"):
            token = self.request.headers.get("Authorization")
            token = token.split('Bearer ')[-1]  # strip 'Bearer ' from token so it can be read.
            user_token = jwt.decode(token, verify=False)
            subject = user_token.get('sub')
            log.info(f'Connected: {user_token.get("email")!r}, user id (sub): {subject!r}')
            return subject

        # TODO remove, using session cookie for testing now
        else:
            log.warning('No authorization provided. Using session.')
            return None

    def initialize(self):
        if self.user is None:
            self.set_secure_cookie(USER_COOKIE, str(uuid.uuid4()), expires_days=1)

    @property
    def user(self):
        subject = self.get_subject_from_jwt()
        if subject:
            return subject

        # TODO remove, using session cookie for testing now
        cookie = self.get_secure_cookie(USER_COOKIE)
        if cookie is not None:
            return cookie.decode()


class JobListHandler(BaseHandler):
    """
    Provides a list of all jobs, current and past for current user.
    """
    async def get(self):
        log.info(f'Getting jobs for user: {self.user}')
        jobs = list(redis.smembers(f'jobs:{self.user}'))
        self.write(
            json.dumps({
                'user': self.user,
                'jobs': [TaskStatusGeneric(job).get() for job in jobs]
            }, sort_keys=True, indent=2)
        )
        self.finish()


class CreateJobHandler(BaseHandler):
    """
    Start any available job type.
    """

    arguments = ('job_type', 'job_parameters')

    def create(self, job_type, job_parameters):
        log.info(f'New job request ({job_type}) for user: {self.user}')

        # Find the right job, and check parameters
        task = jobs.registry.get(job_type)

        if task is None:
            raise tornado.web.HTTPError(404, f'Job {job_type!r} not found.')
        log.info(f'Job {job_type!r} found.')

        try:
            job_parameters = json.loads(job_parameters)
        except json.JSONDecodeError:
            raise tornado.web.HTTPError(400, 'Expected json-like job_parameters.')

        task_id = str(uuid.uuid4())  # Job id used for tracking.
        redis.sadd(f'jobs:{self.user}', task_id)

        task_status = TaskStatusGeneric(task_id)
        task_status.create(job_type=job_type,
                           job_parameters=job_parameters,
                           status=Status.REGISTERED,
                           user=self.user)

        try:
            task.apply_async(
                kwargs=job_parameters,
                task_id=task_id
            )
        except Exception as e:
            raise tornado.web.HTTPError(500, str(e))

        self.write(task_status.get())
        self.finish()

    def get(self, *args, **kwargs):
        kwargs = {arg: self.get_argument(arg) for arg in self.arguments}
        self.create(**kwargs)

    def post(self):
        # FIXME In proper parsing of dictionary from request data. Have to request as string.
        kwargs = {arg: self.get_body_argument(arg) for arg in self.arguments}
        self.create(**kwargs)


class JobStatusHandler(BaseHandler):
    """
    Returns status object for single task.
    """

    def get(self, task_id):
        self.write(TaskStatusGeneric(task_id).get())
        self.finish()


class DataHandler(BaseHandler):
    """
    Returns status object for single task.
    """

    async def get(self, task_id):
        task_status = TaskStatusGeneric(task_id).get()

        if task_status['user'] != self.user:
            raise tornado.web.HTTPError(403, 'Forbidden.')

        if task_status['status'] != Status.SUCCESS:
            raise tornado.web.HTTPError(404, f'Wrong task status ({task_status["status"]}).')

        try:
            # Only first file in the directory will be uploaded. Assumption
            # is that every task can only have a single output file.
            path = os.path.join(task_config.get('data_dir'), task_id)
            path = os.path.join(path, os.listdir(path)[0])
        except (FileNotFoundError, IndexError):
            raise tornado.web.HTTPError(404, 'No such resource.')

        # FIXME figure out way to do this async
        with open(path, 'rb') as f:
            while True:
                data = f.read(16384)
                if not data:
                    break
                self.write(data)
        self.finish()


class StatusWebSocket(tornado.websocket.WebSocketHandler):

    @property
    def user(self):
        # FIXME Setup keycloak here.
        return self.get_secure_cookie(USER_COOKIE).decode()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sub = None

    def check_origin(self, origin):
        # FIXME need to limit connections from specific GB
        return True

    async def open(self):
        log.info("WebSocket opened")
        # FIXME creating redis connection for each websocket might not work well forever.
        self.sub = await aioredis.create_redis(
            f'redis://{redis_config.get("host")}',
        )
        channel, = await self.sub.subscribe(f'channel:{self.user}')

        open_msg = f'Listening to channel: {channel.name.decode()!r}'
        log.info(open_msg)
        self.write_message(open_msg)

        async def async_reader(channel):
            while await channel.wait_message():
                msg = await channel.get(encoding='utf-8')
                logging.info(msg)
                self.write_message(msg)

        await async_reader(channel)

    async def on_message(self, message):
        log.info(f"Message received: {message}")

    async def on_close(self):
        log.info("WebSocket closed")
        asyncio.get_event_loop().call_soon(self.sub.close)
        await asyncio.sleep(0)
        await self.sub.wait_closed()


def make_web_app():
    log.info('Starting webapp')
    return tornado.web.Application([
        (r"/jobs", JobListHandler),
        (r"/jobs/create", CreateJobHandler),
        (r"/jobs/status/(.+)", JobStatusHandler),
        (r"/jobs/data/(.+)", DataHandler),
        (r"/jobs/subscribe", StatusWebSocket)
    ], **tornado_config)


if __name__ == "__main__":
    tornado.options.parse_command_line()
    web_app = make_web_app()
    web_app.listen(tornado_config.get('port', 8888))
    tornado.ioloop.IOLoop.current().start()
