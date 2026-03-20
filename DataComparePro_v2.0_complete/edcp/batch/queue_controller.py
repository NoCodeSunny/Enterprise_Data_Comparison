# -*- coding: utf-8 -*-
"""
edcp.batch.queue_controller
────────────────────────────
QueueController — priority queue with starvation prevention.

Priority tiers (F-PRIORITY-001):
  HIGH   : file < 100 MB  → weight 3
  MEDIUM : file 100-500 MB → weight 2
  LOW    : file > 500 MB  → weight 1

Aging (F-PRIORITY-003):
  Every 30 seconds a job waits in QUEUED state,
  its effective priority score increases by 1,
  preventing low-priority jobs from starving indefinitely.
"""
from __future__ import annotations

import threading
import time
from typing import Generator, List, Optional

from edcp.batch.batch_manager import JobRecord, JobStatus

_PRIORITY_WEIGHT = {"HIGH": 30, "MEDIUM": 20, "LOW": 10}
_AGING_INTERVAL_S = 30   # add 1 to score every N seconds


class QueueController:
    """
    Manages job execution order within one batch.

    drain() is a generator that yields JobRecords in priority order
    with aging applied. Thread-safe for parallel batch execution.
    """

    def __init__(self, jobs: List[JobRecord]) -> None:
        self._jobs   = [j for j in jobs if j.status == JobStatus.QUEUED]
        self._lock   = threading.Lock()
        self._enqueue_times = {j.job_id: time.monotonic() for j in self._jobs}

    def drain(self) -> Generator[JobRecord, None, None]:
        """
        Yield jobs in priority order (highest score first) until queue empty.
        Applies aging so long-waiting jobs get boosted.
        """
        while True:
            with self._lock:
                pending = [j for j in self._jobs if j.status == JobStatus.QUEUED]
                if not pending:
                    break
                # Score = priority_weight + aging_bonus
                now = time.monotonic()
                def score(job: JobRecord) -> int:
                    base  = _PRIORITY_WEIGHT.get(job.priority, 10)
                    wait  = now - self._enqueue_times.get(job.job_id, now)
                    aging = int(wait / _AGING_INTERVAL_S)
                    return base + aging

                pending.sort(key=score, reverse=True)
                next_job = pending[0]
                next_job.status = JobStatus.RUNNING   # claim it

            yield next_job

    def requeue(self, job: JobRecord) -> None:
        """Re-add a job (e.g. for retry)."""
        with self._lock:
            job.status = JobStatus.QUEUED
            self._enqueue_times[job.job_id] = time.monotonic()
            if job not in self._jobs:
                self._jobs.append(job)
