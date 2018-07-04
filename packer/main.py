import json
import logging
import uuid
from datetime import datetime

import aioredis
import jwt
import tornado.ioloop
import tornado.web
from tornado.web import HTTPError
import tornado.websocket
from tornado import iostream, gen
from tornado.log import app_log as log
from tornado.options import define

import packer.jobs as jobs
from .config import tornado_config, redis_config
from .redis_client import redis
from .tasks import TaskStatusGeneric, Status, FSHandler

USER_COOKIE = 'packer-user'  # TODO remove, using session cookie for testing now


class BaseHandler(tornado.web.RequestHandler):

    def get_current_user(self):
        """ output of this is accessible in requests as self.current_user """
        if self.request.headers.get("Authorization"):
            token = self.request.headers.get("Authorization")
            token = token.split('Bearer ')[-1]  # strip 'Bearer ' from token so it can be read.
            user_token = jwt.decode(token, verify=False)
            subject = user_token.get('sub')
            log.info(f'Connected: {user_token.get("email")!r}, user id (sub): {subject!r}')
            return subject

        else:
            # TODO remove, using session cookie for testing now
            log.warning('No authorization provided. Using session.')
            cookie = self.get_secure_cookie(USER_COOKIE)
            if cookie is not None:
                return cookie.decode()
            else:
                log.warning('Creating session cookie.')
                self.set_secure_cookie(USER_COOKIE, str(uuid.uuid4()), expires_days=1)
                return self.get_current_user()


class JobListHandler(BaseHandler):
    """
    Provides a list of all jobs, current and past for current user.
    """

    async def get(self):
        log.info(f'Getting jobs for user: {self.current_user}')
        jobs_ = await self.application.redis.smembers(f'jobs:{self.current_user}')
        self.write(
            {'jobs': [TaskStatusGeneric(job).get() for job in list(jobs_)]}
        )
        self.finish()


class CreateJobHandler(BaseHandler):
    """
    Start any available job type.
    """

    arguments = ('job_type', 'job_parameters')

    def create(self, job_type, job_parameters):
        log.info(f'New job request ({job_type}) for user: {self.current_user}')

        # Find the right job, and check parameters
        task = jobs.registry.get(job_type)

        if task is None:
            raise HTTPError(404, f'Job {job_type!r} not found.')
        log.info(f'Job {job_type!r} found.')

        try:
            job_parameters = json.loads(job_parameters)
        except json.JSONDecodeError:
            raise HTTPError(400, 'Expected json-like job_parameters.')

        task_id = str(uuid.uuid4())  # Job id used for tracking.
        redis.sadd(f'jobs:{self.current_user}', task_id)

        task_status = TaskStatusGeneric(task_id)
        task_status.create(
            job_type=job_type,
            job_parameters=job_parameters,
            status=Status.REGISTERED,
            user=self.current_user,
            created_at=str(datetime.utcnow())
        )

        try:
            task.apply_async(
                kwargs=job_parameters,
                task_id=task_id,
                headers=self.request.headers
            )
        except Exception as e:
            raise tornado.web.HTTPError(500, str(e))

        self.write(task_status.get())
        self.finish()

    def get(self):
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

        if task_status['user'] != self.current_user:
            raise HTTPError(403, 'Forbidden.')

        if task_status['status'] != Status.SUCCESS:
            raise HTTPError(404, f'Wrong task status ({task_status["status"]}).')

        file = FSHandler(task_id)

        if not file.exists():
            raise HTTPError(404, 'No such resource.')

        # chunk size to read
        chunk_size = 1024 * 1024 * 1  # 1 MiB

        with file.byte_reader as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                try:
                    self.write(chunk)
                    await self.flush()
                except iostream.StreamClosedError:
                    # this means the client has closed the connection
                    # so break the loop
                    break

                finally:
                    del chunk
                    # pause the coroutine so other handlers can run
                    await gen.sleep(0.00000001)  # 1 nanosecond

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
        channel, = await self.application.redis.subscribe(f'channel:{self.current_user}')

        open_msg = f'Listening to channel: {channel.name.encode()!r}'
        log.info(open_msg)
        self.write_message(open_msg)

        async def async_reader(channel):
            while await channel.wait_message():
                msg = await channel.get()
                logging.info(msg)
                self.write_message(msg)

        await async_reader(channel)

    def on_message(self, message):
        log.info(f"Message received: {message}")

    def on_close(self):
        log.info("WebSocket closed by client.")


class Application(tornado.web.Application):

    def __init__(self, *args, **kwargs):
        self.redis = None
        super().__init__(*args, **kwargs)

    def init_with_loop(self, loop):
        self.redis = loop.run_until_complete(
            aioredis.create_redis(
                (redis_config.get("host"), redis_config.get("port")),
                loop=loop,
                encoding='utf-8'
            )
        )


def make_web_app():
    log.info('Starting webapp')
    return Application([
        (r"/jobs", JobListHandler),
        (r"/jobs/create", CreateJobHandler),
        (r"/jobs/status/(.+)", JobStatusHandler),
        (r"/jobs/data/(.+)", DataHandler),
        (r"/jobs/subscribe", StatusWebSocket)
    ], **tornado_config)


def main():
    tornado.options.parse_command_line()
    app = make_web_app()
    app.listen(tornado_config.get('port', 8888))
    loop = tornado.ioloop.IOLoop.current()
    app.init_with_loop(loop.asyncio_loop)

    loop.start()


if __name__ == "__main__":
    main()
