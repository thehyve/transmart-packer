import asyncio
import json
import logging
import uuid

import aioredis
import jwt
import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.log import app_log as log
from tornado.options import define

import packer.jobs as jobs
from .config import tornado_config
from .redis_client import redis
from .tasks import TaskStatusGeneric

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
                           user=self.user)

        task.apply_async(
            kwargs=job_parameters,
            task_id=task_id
        )

        self.write(task_status.get())

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

    def get(self, id_):
        self.write(TaskStatusGeneric(id_).get())


class StatusWebSocket(tornado.websocket.WebSocketHandler):

    @property
    def user(self):
        return self.get_secure_cookie(USER_COOKIE).decode()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sub = None

    def check_origin(self, origin):
        return True

    async def open(self):
        log.info("WebSocket opened")
        self.sub = await aioredis.create_redis('redis://localhost')
        channel, = await self.sub.subscribe(f'channel:{self.user}')

        open_msg = f'Listening to channel: {channel.name.decode()!r}'
        log.info(open_msg)
        self.write_message(open_msg)

        async def async_reader(channel):
            while await channel.wait_message():
                msg = await channel.get(encoding='utf-8')
                logging.info(msg)
                self.write_message("message in {}: {}".format(channel.name, msg))

        await async_reader(channel)

    def on_message(self, message):
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
        (r"/jobs/subscribe", StatusWebSocket)
    ], **tornado_config)


if __name__ == "__main__":
    tornado.options.parse_command_line()
    web_app = make_web_app()
    web_app.listen(tornado_config.get('port', 8888))
    tornado.ioloop.IOLoop.current().start()
