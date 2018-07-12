from packer.tasks import app
from packer.jobs import add
from packer.jobs.example import compute_addition
import unittest
from uuid import uuid4

from packer.task_status import TaskStatus, Status


class TestAddTask(unittest.TestCase):
    #
    # @classmethod
    # def setUpClass(cls):
    #     # task_always_eager is not suited for testing, need to find better
    #     # way to test tasks that go through the task queue.
    #     app.conf.update(task_always_eager=True)

    def setUp(self):
        self.task_id = str(uuid4())

        self.status = TaskStatus(self.task_id)
        self.status.create(status=Status.REGISTERED)

        self.task = add.apply_async(kwargs=dict(x=3, y=5, sleep=0),
                                    task_id=self.task_id)

    def test_task_state(self):
        # task_always_eager prevents passing through the task_id, to the tasks.
        # self.assertEqual(Status.SUCCESS, self.status.get().get('status'))
        pass

    def test_addition(self):
        """ Silly? Yes. """
        self.assertEqual(compute_addition(3, 8), 11)

