# -*- coding: utf-8 -*-
"""data_compare.jobs – job layer with retry and failure isolation."""
from data_compare.jobs.comparison_job import ComparisonJob, JobResult
__all__ = ["ComparisonJob", "JobResult"]
