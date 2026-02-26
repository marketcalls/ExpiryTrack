"""Tests for JobRepository — create, update, checkpoint."""


class TestJobRepository:
    def test_create_job(self, tmp_db):
        repo = tmp_db.jobs
        job_id = repo.create_job("collection", instrument_key="NSE_FO|NIFTY")
        assert isinstance(job_id, int)
        assert job_id > 0

    def test_update_job_completed(self, tmp_db):
        repo = tmp_db.jobs
        job_id = repo.create_job("collection")
        repo.update_job_status(job_id, "completed")
        # Should not raise

    def test_update_job_failed(self, tmp_db):
        repo = tmp_db.jobs
        job_id = repo.create_job("collection")
        repo.update_job_status(job_id, "failed", error="Something went wrong")
        # Should not raise

    def test_update_job_processing(self, tmp_db):
        repo = tmp_db.jobs
        job_id = repo.create_job("collection")
        repo.update_job_status(job_id, "processing")
        # Should not raise

    def test_save_checkpoint(self, tmp_db):
        repo = tmp_db.jobs
        job_id = repo.create_job("collection")
        repo.save_checkpoint(job_id, {"last_expiry": "2025-01-30", "progress": 50})
        # Should not raise
