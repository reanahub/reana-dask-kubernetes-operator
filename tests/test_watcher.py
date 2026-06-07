# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2026 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Tests for the Dask pod watcher."""

from unittest.mock import Mock, patch

from reana_dask_kubernetes_operator.watcher import _store_pod_logs


def test_store_pod_logs_preserves_newlines():
    """Raw Dask pod log bytes are stored as str with newlines preserved.

    Guards against kubernetes 36.x's str-deserialiser regression that
    turns ``bytes`` payloads into ``"b'...'"`` repr strings with literal
    backslash-n inside; such strings would also break the ``ServiceLog.log``
    JSONB column serialisation.
    """
    pod_logs = b"scheduler started\nworker connected\n"
    service = Mock(id_="service-id", logs=[])
    session = Mock()
    session.query.return_value.filter_by.return_value.first.return_value = service
    service_log = Mock()
    # Pass the k8s client mock via the ``new=`` positional argument so that
    # ``unittest.mock.patch`` does not introspect the original attribute.
    # ``current_k8s_corev1_api_client`` is a werkzeug ``LocalProxy``; any
    # ``getattr``/``hasattr`` on the proxy resolves the underlying object,
    # which in turn calls ``k8s_config.load_incluster_config()`` and fails
    # outside a Kubernetes cluster.
    k8s_client = Mock()
    k8s_client.read_namespaced_pod_log.return_value = Mock(data=pod_logs)

    with patch(
        "reana_dask_kubernetes_operator.watcher.current_k8s_corev1_api_client",
        k8s_client,
    ), patch(
        "reana_dask_kubernetes_operator.watcher.Session", return_value=session
    ), patch(
        "reana_dask_kubernetes_operator.watcher.ServiceLog",
        return_value=service_log,
    ) as service_log_class:
        _store_pod_logs(
            name="reana-run-dask-workflow-scheduler-pod",
            meta={
                "labels": {"reana-run-dask-workflow-uuid": "workflow"},
                "uid": "pod-uid",
            },
            namespace="default",
            logger=Mock(),
        )

    stored_log = service_log_class.call_args.kwargs["log"]
    assert isinstance(stored_log["content"], str)
    assert stored_log["content"] == pod_logs.decode("utf-8")
    assert "b'" not in stored_log["content"]
    assert "\\n" not in stored_log["content"]
    session.add.assert_called_once_with(service_log)
    session.commit.assert_called_once_with()
    # Lock in the kubernetes 36.x workaround: the call MUST pass
    # ``_preload_content=False`` so we get raw bytes from urllib3
    # instead of the broken str-deserialiser output.
    assert k8s_client.read_namespaced_pod_log.called
    for call in k8s_client.read_namespaced_pod_log.call_args_list:
        assert call.kwargs.get("_preload_content") is False
