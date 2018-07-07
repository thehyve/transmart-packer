import json
import os.path
import sys
import time
import uuid

import jwt
import tornado.ioloop
import tornado.websocket
from tornado import httpclient
from tornado.testing import AsyncHTTPTestCase

from packer.main import make_web_app
from packer.task_status import Status
from tests.testing_config import tornado_config

# add application root to sys.path
APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(APP_ROOT, '..'))

app, loop = make_web_app(tornado_config.get('port'), tornado_config)


def get_mock_auth(user_id=None):
    if user_id is None:
        user_id = '1234567890'

    claims = {
        'sub': user_id,
        'name': 'John Doe',
        'iat': 1516239022
    }
    token = jwt.encode(claims, key='secret?')
    return {'Authorization': f'Bearer {token.decode()}'}


class TestHandlerBase(AsyncHTTPTestCase):

    def get_app(self):
        return app

    def get_http_port(self):
        return tornado_config.get('port')

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()


class TestWebAppHandlers(TestHandlerBase):
    post_args = {
        "job_type": "add",
        "job_parameters":
            {
                "x": 4,
                "y": 6,
                "sleep": 2
            }
    }

    def get(self, url):
        return self.fetch(url, method='GET')

    def post(self, url, body):
        return self.fetch(url, method='POST', body=json.dumps(body))

    def mocked_get(self, url):
        """ Make get request to url when authenticated using mocked jwt token. """
        return self.fetch(url, method='GET', headers=get_mock_auth())

    def mocked_post(self, url, body):
        """ Make get request to url when authenticated using mocked jwt token. """
        return self.fetch(url, method='POST', body=json.dumps(body), headers=get_mock_auth())

    def check_create_response(self, body: dict):
        task_id = body.get('task_id')
        user_id = body.get('user')
        status = body.get('status')
        self.assertIsNotNone(task_id)
        self.assertIsNotNone(user_id)
        self.assertIn(status, (Status.REGISTERED, Status.RUNNING))

    def test_create_job_get(self):
        response = self.get('/jobs/create?job_type=add&job_parameters={"x":500,"y":1501,"sleep":0}')
        self.assertEqual(200, response.code)
        self.check_create_response(json.loads(response.body))

    def test_create_job_get_missing_job(self):
        response = self.get('/jobs/create?job_type=not_add&job_parameters={"x":500,"y":1501}')
        self.assertEqual(404, response.code)

    def test_create_job_get_unexpected_argument(self):
        response = self.get('/jobs/create?job_type=add&job_parameters={"x":500,"y":1501}&thirds=nope')
        self.assertEqual(400, response.code)

    def test_create_job_get_invalid_json(self):
        response = self.get("/jobs/create?job_type=add&job_parameters={x:500,y:1501}")
        self.assertEqual(400, response.code)

    def test_create_job_post(self):
        response = self.post('/jobs/create', self.post_args)
        self.assertEqual(200, response.code)
        self.check_create_response(json.loads(response.body))

    def test_create_job_post_missing_job(self):
        args = {
            "job_type": "not_add",
            "job_parameters":
                {
                    "x": 4,
                    "y": 6
                }
        }
        response = self.post('/jobs/create', args)
        self.assertEqual(404, response.code)

    def test_create_job_post_unexpected_argument(self):
        args = {
            "job_type": "not_add",
            "job_parameters":
                {
                    "x": 4,
                    "y": 6
                },
            "thirds": "Nope"
        }
        response = self.post('/jobs/create', args)
        self.assertEqual(400, response.code)

    def test_create_job_post_invalid_json(self):
        args = """
        {
            "job_type": "not_add",
            "job_parameters":
                {
                    'x': 4,
                    'y': 6
                }
        }"""
        response = self.fetch('/jobs/create', method='POST', body=args)
        self.assertEqual(400, response.code)

    def test_create_job_post_missing_job_parameters(self):
        args = {
            "job_type": "add",
            "job_parameters":
                {
                    "y": 6
                }
        }
        response = self.post('/jobs/create', args)
        self.assertEqual(400, response.code)

    def test_create_job_post_additional_job_parameters(self):
        args = {
            "job_type": "add",
            "job_parameters":
                {
                    "x": 4,
                    "y": 6,
                    "z": 4
                }
        }
        response = self.post('/jobs/create', args)
        self.assertEqual(400, response.code)

    def test_status_change(self):
        response = self.mocked_post('/jobs/create', self.post_args)
        body = json.loads(response.body)
        task_id = body.get("task_id")
        time.sleep(1)
        response = self.mocked_get(f'/jobs/status/{task_id}')
        self.assertEqual(200, response.code)
        body = json.loads(response.body)
        self.assertEqual(Status.RUNNING, body.get('status'))
        self.assertEqual(task_id, body.get('task_id'))

    def test_status_wrong_task(self):
        response = self.get('/jobs/status/some-non-existent-uuid')
        self.assertEqual(404, response.code)

    def test_job_cancelling(self):
        response = self.mocked_post('/jobs/create', self.post_args)
        body = json.loads(response.body)
        response = self.mocked_get(f'/jobs/cancel/{body.get("task_id")}')
        self.assertEqual(200, response.code)
        response = self.mocked_get(f'/jobs/status/{body.get("task_id")}')
        body = json.loads(response.body)
        self.assertEqual(Status.CANCELLED, body.get('status'))

    def test_get_job_list(self):
        response = self.get('/jobs')
        self.assertEqual(200, response.code)
        body = json.loads(response.body)
        self.assertIn('add', body.get('available_job_types'))

    def test_job_download(self):
        response = self.mocked_post('/jobs/create', self.post_args)
        task_id = json.loads(response.body).get('task_id')

        response = self.mocked_get(f'/jobs/data/{task_id}')
        self.assertEqual(403, response.code)
        time.sleep(2)

        response = self.mocked_get(f'/jobs/data/{task_id}')
        self.assertEqual(200, response.code)
        calculated_value = int(response.body.decode())
        self.assertEqual(10, calculated_value)

        response = self.get(f'/jobs/data/{task_id}')
        self.assertEqual(404, response.code)

    @tornado.testing.gen_test
    async def test_ws_listen(self):
        user_id = str(uuid.uuid4())
        auth_header = get_mock_auth(user_id)
        port = tornado_config.get("port")
        ws_url = f"ws://localhost:{port}/jobs/subscribe"
        request = httpclient.HTTPRequest(ws_url, headers=auth_header)
        ws_client = await tornado.websocket.websocket_connect(request)

        response = await ws_client.read_message()
        self.assertEqual(f"Listening to channel: 'channel:{user_id}'", response)

        client = httpclient.AsyncHTTPClient()
        r = await client.fetch(
            f"http://localhost:{port}/jobs/create",
            body=json.dumps(self.post_args),
            method='POST',
            headers=auth_header
        )
        task_id = json.loads(r.body).get('task_id')

        while True:
            response = await ws_client.read_message()
            message = json.loads(response)
            self.assertEqual(task_id, message.get("task_id"))
            if message.get('status') == Status.SUCCESS:
                break

