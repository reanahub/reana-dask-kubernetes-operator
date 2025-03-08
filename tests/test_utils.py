# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2025 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Dask-Kubernetes-Operator tests."""

import pytest

from reana_dask_kubernetes_operator.watcher import extract_component_name_from_pod_name


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("reana-run-dask-50e66ffb-scheduler-8499f4f8b4-qztft", "scheduler"),
        (
            "reana-run-dask-50e66ffb-scheduler-934faa4ejfnc-qbretrb",
            "scheduler",
        ),
        (
            "reana-run-dask-50e66ffb-worker-8499f4cdsvf-dsvfdsb",
            "worker-8499f4cdsvf-dsvfdsb",
        ),
        (
            "reana-run-dask-50e66ffb-worker-2947i0uelsn-aldkfvs",
            "worker-2947i0uelsn-aldkfvs",
        ),
        ("something-else-entirely", None),
    ],
)
def test_extract_component_name(input_str, expected):
    assert extract_component_name_from_pod_name(input_str) == expected
