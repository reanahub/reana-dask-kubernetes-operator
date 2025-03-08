# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2025 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA Workflow Controller Instance."""


import logging
import kopf

from reana_commons.k8s.api_client import current_k8s_corev1_api_client
from reana_commons.utils import get_dask_component_name
from reana_db.database import Session
from reana_db.models import Service, ServiceLog

from reana_dask_kubernetes_operator.config import (
    DASK_PODS_FINALIZER_NAME,
    REANA_OPENSEARCH_ENABLED,
)
from reana_dask_kubernetes_operator.utils import extract_component_name_from_pod_name

logging.info("REANA Dask Kubernetes Operator watcher module loaded.")

if not REANA_OPENSEARCH_ENABLED:

    @kopf.on.create("v1", "pods", labels={"dask.org/component": "scheduler"})
    @kopf.on.create("v1", "pods", labels={"dask.org/component": "worker"})
    def add_finalizer(name, meta, patch, logger, **_):
        """Add a finalizer when a Dask pod is created."""
        try:
            logger.info(f"Adding finalizer to pod {name}")
            finalizers = meta.get("finalizers", [])
            if DASK_PODS_FINALIZER_NAME not in finalizers:
                finalizers.append(DASK_PODS_FINALIZER_NAME)
                patch.metadata["finalizers"] = finalizers
        except Exception as e:
            logger.error(f"Error adding finalizer to pod {name}: {e}")

    @kopf.on.delete("v1", "pods", labels={"dask.org/component": "scheduler"})
    @kopf.on.delete("v1", "pods", labels={"dask.org/component": "worker"})
    def on_pod_delete(name, meta, patch, namespace, logger, **_):
        """Retrieve logs before a Dask-labeled pod is deleted."""
        try:
            logger.info(f"Fetching logs for pod: {name} in namespace: {namespace}")
            pod_logs = current_k8s_corev1_api_client.read_namespaced_pod_log(
                name=name, namespace=namespace
            )
        except Exception as e:
            logger.error(f"Failed to fetch logs from pod {name}: {e}")

        try:
            session = Session()
            workflow_id = meta.get("labels", {}).get("reana-run-dask-workflow-uuid")
            service_name = get_dask_component_name(
                workflow_id, "database_model_service"
            )
            component_name = extract_component_name_from_pod_name(name)

            # Create a new service log entry
            service = session.query(Service).filter_by(name=service_name).first()
            if service and pod_logs:
                service_log = ServiceLog(
                    service_id=service.id_,
                    log={
                        "component": component_name,
                        "content": pod_logs,
                    },
                )
                session.add(service_log)
                session.commit()
                logger.info(f"Successfully stored logs for pod {name}")
            else:
                logger.warning(
                    f"Could not store logs - service {service_name} not found or no logs available"
                )
        except Exception as e:
            logger.error(f"Error writing pod logs to DB {name}: {e}")

        try:
            finalizers = meta.get("finalizers", [])
            if DASK_PODS_FINALIZER_NAME in finalizers:
                logger.info(f"Removing finalizer from {name}")
                patch.metadata["finalizers"] = [
                    f for f in finalizers if f != DASK_PODS_FINALIZER_NAME
                ]

        except Exception as e:
            logger.error(f"Error removing finalizer from {name}: {e}")
