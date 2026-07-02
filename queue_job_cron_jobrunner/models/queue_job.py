# Copyright 2022 Camptocamp SA (https://www.camptocamp.com).
# @author Iván Todorovich <ivan.todorovich@camptocamp.com>
# License AGPL-3.0 or later (https://www.gnu.org/licenses/agpl).

import logging
import traceback
from io import StringIO

from psycopg2 import OperationalError

from odoo import api, models, tools
from odoo.service.model import PG_CONCURRENCY_ERRORS_TO_RETRY

from odoo.addons.queue_job.controllers.main import PG_RETRY
from odoo.addons.queue_job.exception import FailedJobError, RetryableJobError
from odoo.addons.queue_job.job import Job
from odoo.addons.base.models.ir_cron import MIN_TIME_PER_JOB

_logger = logging.getLogger(__name__)


class QueueJob(models.Model):
    _inherit = "queue.job"

    @api.model
    def _acquire_one_job(self):
        """Acquire the next job to be run.

        :returns: queue.job record (locked for update)
        """
        # TODO: This method should respect channel priority and capacity,
        #       rather than just fetching them by creation date.
        self.env.flush_all()
        self.env.cr.execute(
            """
            SELECT id
            FROM queue_job
            WHERE state = 'pending'
            AND (eta IS NULL OR eta <= (now() AT TIME ZONE 'UTC'))
            ORDER BY priority, date_created
            LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED
            """
        )
        row = self.env.cr.fetchone()
        return self.browse(row and row[0])

    @api.model
    def _count_pending_jobs(self) -> int:
        """Count pending jobs ready to be processed."""
        self.env.flush_all()
        self.env.cr.execute(
            """
            SELECT COUNT(*)
            FROM queue_job
            WHERE state = 'pending'
            AND (eta IS NULL OR eta <= (now() AT TIME ZONE 'UTC'))
            """
        )
        row = self.env.cr.fetchone()
        if not row:
            return 0
        return row[0]

    def _process(self, commit=False):
        """Process the job"""
        self.ensure_one()
        job = Job._load_from_db_record(self)
        # Set it as started
        job.set_started()
        job.store()
        _logger.debug("%s started", job.uuid)
        # TODO: Commit the state change so that the state can be read from the UI
        #       while the job is processing. However, doing this will release the
        #       lock on the db, so we need to find another way.
        # if commit:
        #     self.env.flush_all()
        #     self.env.cr.commit()

        # Actual processing
        try:
            try:
                with self.env.cr.savepoint():
                    job.perform()
                    job.set_done()
                    job.store()
            except OperationalError as err:
                # Automatically retry the typical transaction serialization errors
                if err.pgcode not in PG_CONCURRENCY_ERRORS_TO_RETRY:
                    raise
                message = tools.ustr(err.pgerror, errors="replace")
                job.postpone(result=message, seconds=PG_RETRY)
                job.set_pending(reset_retry=False)
                job.store()
                _logger.debug("%s OperationalError, postponed", job)

        except RetryableJobError as err:
            # delay the job later, requeue
            job.postpone(result=str(err), seconds=5)
            job.set_pending(reset_retry=False)
            job.store()
            _logger.debug("%s postponed", job)

        except (FailedJobError, Exception):
            with StringIO() as buff:
                traceback.print_exc(file=buff)
                _logger.error(buff.getvalue())
                job.set_failed(exc_info=buff.getvalue())
                job.store()

        if commit:  # pragma: no cover
            self.env.flush_all()
            self.env.cr.commit()  # pylint: disable=invalid-commit

        _logger.debug("%s enqueue depends started", job)
        job.enqueue_waiting()
        _logger.debug("%s enqueue depends done", job)

    @api.model
    def _job_runner(self, commit=True):
        """Short-lived job runner, triggered by async crons"""
        is_cron = bool(self.env.context.get("cron_id"))
        cron_end_time = self.env.context.get("cron_end_time")
        total_done = 0
        if cron_end_time:
            ICP = self.env["ir.config_parameter"].sudo()
            param = ICP.get_param("queue_job_cron_jobrunner.cron_time_limit", default="0")
            try:
                time_limit = float(param)
                extra_time = time_limit - MIN_TIME_PER_JOB if time_limit > MIN_TIME_PER_JOB else 0
                self = self.with_context(cron_end_time=cron_end_time + extra_time)
            except ValueError:
                pass

        time_left = float("inf")

        if is_cron:
            remaining = self._count_pending_jobs()
            time_left = self.env["ir.cron"]._commit_progress(remaining=remaining)
            _logger.debug("Job runner started, remaining=%s, time_left=%s", remaining, time_left)

        job = self._acquire_one_job()
        if not job:
            if is_cron:
                self.env["ir.cron"]._commit_progress(remaining=0)
            return

        while time_left > 0 and job:
            job._process(commit=commit)

            if is_cron:
                remaining = self._count_pending_jobs()
                total_done += 1
                time_left = self.env["ir.cron"]._commit_progress(
                    processed=1,
                    remaining=remaining,
                )
                _logger.debug("Job runner processed one job, total_done=%s, remaining=%s, time_left=%s", total_done, remaining, time_left)
                if time_left <= 0:
                    break

            job = self._acquire_one_job()


    @api.model
    def _cron_trigger(self, at=None):
        """Trigger the cron job runners

        Odoo will prevent concurrent cron jobs from running.
        So, to support parallel execution, we'd need to have (at least) the
        same number of ir.crons records as cron workers.

        All crons should be triggered at the same time.
        """
        crons = self.env["ir.cron"].sudo().search([("queue_job_runner", "=", True)])
        for cron in crons:
            cron._trigger(at=at)

    def _ensure_cron_trigger(self):
        """Create cron triggers for these jobs"""
        records = self.filtered(lambda r: r.state == "pending")
        if not records:
            return
        # Trigger immediate runs
        immediate = any(not rec.eta for rec in records)
        if immediate:
            self._cron_trigger()
        # Trigger delayed eta runs
        delayed_etas = {rec.eta for rec in records if rec.eta}
        if delayed_etas:
            self._cron_trigger(at=list(delayed_etas))

    @api.model_create_multi
    def create(self, vals_list):
        # When jobs are created, also create the cron trigger
        records = super().create(vals_list)
        records._ensure_cron_trigger()
        return records

    def write(self, vals):
        # When a job state or eta changes, make sure a cron trigger is created
        res = super().write(vals)
        if "state" in vals or "eta" in vals:
            self._ensure_cron_trigger()
        return res
