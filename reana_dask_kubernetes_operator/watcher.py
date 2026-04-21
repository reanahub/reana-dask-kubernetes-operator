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
from kubernetes.client.rest import ApiException

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

if not REANA_OPENSEARCH_ENABLED:  # noqa: C901

    def _store_pod_logs(name, meta, namespace, logger):
        """Store Dask pod logs in the database."""
        pod_logs = None
        session = None

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
            pod_uid = meta.get("uid")

            service = session.query(Service).filter_by(name=service_name).first()
            if not service or not pod_logs:
                logger.warning(
                    f"Could not store logs: service {service_name} not found or no logs available"
                )
                return

            # Deduplicate repeated resume or delete events only when the same pod UID is seen again
            if pod_uid and any(
                (stored.log or {}).get("pod_uid") == pod_uid for stored in service.logs
            ):
                logger.info(
                    f"Logs for pod {name} already stored for service {service_name}"
                )
                return

            service_log = ServiceLog(
                service_id=service.id_,
                log={
                    "component": component_name,
                    "pod": name,
                    "pod_uid": pod_uid,
                    "content": pod_logs,
                },
            )
            session.add(service_log)
            session.commit()
            logger.info(f"Successfully stored logs for pod {name}")
        except Exception as e:
            if session:
                session.rollback()
            logger.error(f"Error writing pod logs to DB {name}: {e}")
        finally:
            if session:
                session.close()

    def _remove_finalizer(name, meta, namespace, logger):
        """Remove the custom pod finalizer to let deletion proceed."""
        finalizers = meta.get("finalizers", [])
        if DASK_PODS_FINALIZER_NAME not in finalizers:
            return

        remaining_finalizers = [f for f in finalizers if f != DASK_PODS_FINALIZER_NAME]
        logger.info(f"Removing finalizer from {name}")

        try:
            current_k8s_corev1_api_client.patch_namespaced_pod(
                name=name,
                namespace=namespace,
                body=(
                    [
                        {
                            "op": "replace",
                            "path": "/metadata/finalizers",
                            "value": remaining_finalizers,
                        }
                    ]
                    if remaining_finalizers
                    else [{"op": "remove", "path": "/metadata/finalizers"}]
                ),
            )
        except ApiException as e:
            if e.status == 404:
                logger.info(f"Pod {name} no longer exists while removing finalizer.")
                return
            logger.error(f"Error removing finalizer from {name}: {e}")
            raise kopf.TemporaryError(
                f"Could not remove finalizer from pod {name}.", delay=5
            ) from e
        except Exception as e:
            logger.error(f"Error removing finalizer from {name}: {e}")
            raise kopf.TemporaryError(
                f"Could not remove finalizer from pod {name}.", delay=5
            ) from e

    @kopf.on.create("v1", "pods", labels={"dask.org/component": "scheduler"})
    @kopf.on.resume(
        "v1",
        "pods",
        labels={"dask.org/component": "scheduler"},
        deleted=False,
    )
    @kopf.on.create("v1", "pods", labels={"dask.org/component": "worker"})
    @kopf.on.resume(
        "v1",
        "pods",
        labels={"dask.org/component": "worker"},
        deleted=False,
    )
    def add_finalizer(name, meta, patch, logger, **_):
        """Ensure the custom finalizer is present on Dask pods."""
        try:
            if meta.get("deletionTimestamp"):
                return

            logger.info(f"Adding finalizer to pod {name}")
            finalizers = meta.get("finalizers", [])
            if DASK_PODS_FINALIZER_NAME not in finalizers:
                finalizers.append(DASK_PODS_FINALIZER_NAME)
                patch.metadata["finalizers"] = finalizers
        except Exception as e:
            logger.error(f"Error adding finalizer to pod {name}: {e}")

    @kopf.on.event("v1", "pods", labels={"dask.org/component": "scheduler"})
    @kopf.on.resume(
        "v1",
        "pods",
        labels={"dask.org/component": "scheduler"},
        deleted=True,
    )
    @kopf.on.event("v1", "pods", labels={"dask.org/component": "worker"})
    @kopf.on.resume(
        "v1",
        "pods",
        labels={"dask.org/component": "worker"},
        deleted=True,
    )
    def on_pod_terminating(name, meta, namespace, logger, **_):
        """Retrieve logs when a Dask pod starts terminating."""
        if not meta.get("deletionTimestamp"):
            return

        if DASK_PODS_FINALIZER_NAME not in meta.get("finalizers", []):
            return

        try:
            _store_pod_logs(name=name, meta=meta, namespace=namespace, logger=logger)
        except Exception as e:
            logger.error(f"Unexpected error while storing logs for pod {name}: {e}")
        finally:
            _remove_finalizer(
                name=name,
                meta=meta,
                namespace=namespace,
                logger=logger,
            )
