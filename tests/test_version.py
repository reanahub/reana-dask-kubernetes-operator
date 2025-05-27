# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2025 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Dask-Kubernetes-Operator tests."""


def test_version():
    """Test version import."""
    from reana_dask_kubernetes_operator import __version__

    assert __version__
