import json
import logging
import uuid
from datetime import datetime

import jwt
import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado import iostream, gen
from tornado.log import app_log as log
from tornado.options import define
from tornado.web import HTTPError

import packer.jobs as jobs
from packer.file_handling import FSHandler
from packer.task_status import Status, TaskStatusAsync
from .config import tornado_config, app_config
from .redis_client import get_async_redis
from .tasks import app

USER_COOKIE = 'packer-user'  # TODO remove, using session cookie for testing now


def get_current_user(self):
    """ output of this is accessible in requests as self.current_user """
    if self.request.headers.get("Authorization"):
        token = self.request.headers.get("Authorization")
        token = token.split('Bearer ')[-1]  # strip 'Bearer ' from token so it can be read.

        # FIXME add shared secret from keycloak to properly verify users.
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
            fake_id = str(uuid.uuid4())
            self.set_secure_cookie(USER_COOKIE, fake_id, expires_days=1)
            return fake_id


class BaseHandler(tornado.web.RequestHandler):
    get_current_user = get_current_user

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", app_config.get("host"))
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Headers", "authorization, content-type")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    @property
    def user_jobs_key(self):
        return f'jobs:{self.current_user}'

    async def get_task_status(self, task_id):
        """
        Checks whether a task exists and current user has permissions to view.
        Will raise 404 if not, else will return the object to control it.

        :param task_id: uuid
        :return: TaskStatus object.
        """
        is_job = await self.application.redis.sismember(self.user_jobs_key, task_id)
        if not is_job:
            raise HTTPError(404, f'There is no task with id {task_id!r}.')
        else:
            return TaskStatusAsync(task_id, self.application.redis)

    async def options(self, *args):
        # no body
        self.set_status(200)
        self.finish()


class JobListHandler(BaseHandler):
    """
    Provides a list of all jobs, current and past for current user.
    """

    async def get(self):
        log.info(f'Getting jobs for user: {self.current_user}')
        jobs_ = await self.application.redis.smembers(f'jobs:{self.current_user}')
        self.write(
            {'jobs': [await TaskStatusAsync(job, self.application.redis).get() for job in jobs_],
             'available_job_types': [job for job in jobs.registry.keys()]}
        )
        self.finish()


class CreateJobHandler(BaseHandler):
    """
    Start any available job type.
    """

    async def create(self, job_type: str, job_parameters: dict, **kwargs):

        log.info(f'New job request ({job_type}) for user: {self.current_user}')

        if kwargs:
            msg = f'Illegal arguments provided: {", ".join([k for k in kwargs.keys()])}.'
            log.info(msg)
            raise HTTPError(400, msg)

        if not isinstance(job_parameters, dict):
            msg = f'Unexpected argument, "job_parameters" should be in ' \
                  f'dict-like, but got {type(job_parameters)}'
            log.error(msg)
            raise HTTPError(400, msg)

        # Find the right job, and check parameters
        task = jobs.registry.get(job_type)

        if task is None:
            raise HTTPError(404, f'Job {job_type!r} not found.')
        log.info(f'Job {job_type!r} found.')

        task_id = str(uuid.uuid4())  # Job id used for tracking.
        await self.application.redis.sadd(self.user_jobs_key, task_id)

        task_status = await self.get_task_status(task_id)
        await task_status.create(
            job_type=job_type,
            job_parameters=job_parameters,
            status=Status.REGISTERED,
            user=self.current_user,
            created_at=str(datetime.now())
        )

        try:
            task.apply_async(
                kwargs=job_parameters,
                task_id=task_id,
                headers=self.request.headers
            )
        except Exception as e:
            raise tornado.web.HTTPError(400, str(e))

        self.write(await task_status.get())
        self.finish()

    async def get(self):
        kwargs = self.request.arguments
        kwargs['job_type'] = self.get_argument('job_type')
        try:
            kwargs['job_parameters'] = json.loads(self.get_argument('job_parameters'))
        except json.JSONDecodeError:
            raise HTTPError(400, 'Expected valid JSON job parameters.')

        await self.create(**kwargs)

    async def post(self):
        try:
            kwargs = json.loads(self.request.body)
        except json.JSONDecodeError:
            raise HTTPError(400, 'Expected are valid JSON parameters.')

        await self.create(**kwargs)


class JobStatusHandler(BaseHandler):
    """
    Returns status object for single task.
    """

    async def get(self, task_id):
        task_status = await self.get_task_status(task_id)
        self.write(await task_status.get())
        self.finish()


class JobCancelHandler(BaseHandler):
    """
    Returns status object for single task.
    """

    async def get(self, task_id):
        task_status = await self.get_task_status(task_id)
        app.control.revoke(task_id, terminate=True, signal='SIGUSR1')
        logging.info(f'Cancel signal sent to worker for task: {task_id}')
        await task_status.update(
            status=Status.CANCELLED,
            message='Cancelled prior to execution.'
        )
        self.finish()


class DataHandler(BaseHandler):
    """
    Returns status object for single task.
    """

    async def get(self, task_id):
        task_status = await self.get_task_status(task_id)
        task_status = await task_status.get()

        # Should never happen, but let's check to be sure.
        if task_status['user'] != self.current_user:
            raise HTTPError(401, 'Unauthorized.')

        if task_status['status'] != Status.SUCCESS:
            raise HTTPError(403, f'Wrong task status ({task_status["status"]}), '
                                 f'has to be {Status.SUCCESS}.')

        file = FSHandler(task_id)

        if not file.exists():
            raise HTTPError(404, 'Resource not found. Contact administrator.')

        chunk_size = 1024 * 1024 * 1  # 1 MiB chunk size to read

        with file.byte_reader as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                try:
                    self.write(chunk)
                    await self.flush()

                except iostream.StreamClosedError:
                    break  # the client has closed the connection

                finally:
                    del chunk
                    # pause the coroutine so other handlers can run
                    await gen.sleep(0.00000001)  # 1 nanosecond

        self.finish()


class StatusWebSocket(tornado.websocket.WebSocketHandler):
    get_current_user = get_current_user

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sub = None

    def check_origin(self, origin):
        # FIXME need to limit connections from specific GB
        return True

    async def open(self):
        log.info("WebSocket opened")
        self.sub, = await self.application.redis.subscribe(f'channel:{self.current_user}')

        open_msg = f'Listening to channel: {self.sub.name.decode()!r}'
        log.info(open_msg)
        self.write_message(open_msg)

        async def async_reader(channel):
            while await channel.wait_message():
                msg = await channel.get()
                logging.info(msg)
                self.write_message(msg)

        await async_reader(self.sub)

    async def on_message(self, message):
        log.info(f"Message received: {message}")

    async def on_close(self):
        log.info("WebSocket closed by client.")
        await self.sub.unsubscribe()
        log.info("Unsubscribed from channel.")


class Application(tornado.web.Application):

    def __init__(self, *args, **kwargs):
        self.redis = None
        super().__init__(*args, **kwargs)

    def init_with_loop(self, loop):
        self.redis = get_async_redis(loop)


def make_web_app(port, tornado_options):
    """
    Create tornado app and loop.

    :return: (app, loop)
    """
    log.info('Creating web application.')
    web_app = Application([
        (r"/jobs", JobListHandler),
        (r"/jobs/create", CreateJobHandler),
        (r"/jobs/status/(.+)", JobStatusHandler),
        (r"/jobs/data/(.+)", DataHandler),
        (r"/jobs/cancel/(.+)", JobCancelHandler),
        (r"/jobs/subscribe", StatusWebSocket)
    ], **tornado_options)
    web_app.listen(port)
    loop = tornado.ioloop.IOLoop.current()
    web_app.init_with_loop(loop.asyncio_loop)
    return web_app, loop


def main():
    tornado.options.parse_command_line()
    port = tornado_config.get('port', 8888)
    web_app, loop = make_web_app(port, tornado_config)
    log.info(f'Starting at http://localhost:{web_app.settings.get("port")}')
    loop.start()
