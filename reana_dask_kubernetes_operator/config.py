# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2025 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Reana Dask Kubernetes Operator configuration."""

import os

DASK_PODS_FINALIZER_NAME = "dask.kopf.dev/logs"
"""Finalizer name for Dask pods."""

REANA_OPENSEARCH_ENABLED = (
    os.getenv("REANA_OPENSEARCH_ENABLED", "false").lower() == "true"
)
"""OpenSearch enabled flag."""
