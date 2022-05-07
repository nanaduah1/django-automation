
from automation.models import Job


class WorkerTestMixin:
    worker_class = None

    def assertNoError(self):
        self.assertEqual(self.job.status, Job.STATUS_COMPLETED)
        self.assertTrue(self.job.fail_reason is None)

    def setUp(self) -> None:
        super().setUp()
        assert self.worker_class is not None
        self.worker_class.start()
        self.job = Job.objects.get(type_key=self.worker_class.key)
        self.worker = self.worker_class(self.job)

    def test_worker_runs_successfully(self):
        self.worker.execute()
        self.assertNoError()


