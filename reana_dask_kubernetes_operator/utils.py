# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2025 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Dask-Kubernetes-Operator utils."""


def extract_component_name_from_pod_name(pod_name: str) -> str | None:
    """Extracts a component name from the pod name."""

    if "worker" in pod_name:
        return pod_name[pod_name.index("worker") :]
    elif "scheduler" in pod_name:
        return "scheduler"
    else:
        return None
