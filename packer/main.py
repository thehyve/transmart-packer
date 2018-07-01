import asyncio
import json
import logging
import uuid

import aioredis
import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.log import app_log as log
from tornado.options import define
import jwt

from .config import tornado_config
from .redis_client import redis
from .tasks import add, TaskStatusGeneric

logger = logging.getLogger(__name__)

USER_COOKIE = 'packer-user'


class BaseHandler(tornado.web.RequestHandler):

    def get_subject_from_jwt(self):
        try:
            token = self.request.headers.get("Authorization")
            token = token.split('Bearer ')[-1]  # strip 'Bearer ' from token so it can be read.
            user_token = jwt.decode(token, verify=False)
            subject = user_token.get('sub')
            log.info(f'Connected: {user_token.get("email")!r}, user id (sub): {subject!r}')
            return subject
        except Exception:  # TODO remove, using session cookie for testing now
            logger.warning('No authorization provided. Using session.')
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
    async def get(self):
        log.info(f'Getting jobs for user: {self.user}')
        jobs = list(redis.smembers(f'jobs:{self.user}'))
        self.write(
            json.dumps({
                'user': self.user,
                'jobs': [TaskStatusGeneric(job).get() for job in jobs]
            }, sort_keys=True, indent=2)
        )


class DataJobHandler(BaseHandler):

    def get(self, i1: int, i2: int):
        log.info(f'New job for user: {self.user}')

        id_ = str(uuid.uuid4())
        channel = f'channel:{self.user}'

        redis.sadd(f'jobs:{self.user}', id_)
        TaskStatusGeneric(id_).create(i1=i1, i2=i2, status='REGISTERED')

        async_result = add.delay(int(i1), int(i2), id_, channel)
        logging.info(f'Task ready = {async_result.ready()}')
        self.write(f'Task created with identifier: {id_}')

    def post(self):
        pass


class JobStatusHandler(BaseHandler):

    def get(self, id_):
        result = redis.get(id_)
        self.write(f'Toots: {result}')

    def post(self):
        pass


class EchoWebSocket(tornado.websocket.WebSocketHandler):

    @property
    def user(self):
        return self.get_secure_cookie(USER_COOKIE).decode()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sub = None

    def check_origin(self, origin):
        return True

    async def open(self):
        logger.info("WebSocket opened")
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
        (r"/jobs/([0-9]+)/([0-9]+)", DataJobHandler),
        (r"/jobs/status/(.+)", JobStatusHandler),
        (r"/jobs/subscribe", EchoWebSocket)
    ], **tornado_config)


if __name__ == "__main__":
    tornado.options.parse_command_line()
    web_app = make_web_app()
    web_app.listen(tornado_config.get('port', 8888))
    tornado.ioloop.IOLoop.current().start()
