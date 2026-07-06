"""KubernetesPodOperator factory — one pod per step, running `etl run <pipeline> <step>`.

This is the ONLY Airflow-side code that knows how to launch an ETL step. It reads nothing
from the etl package (keeps Airflow's env ETL-dependency-free); all config comes from the
generated manifest.
"""

from __future__ import annotations

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = "airflow"
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"

_DEFAULT_RESOURCES = {
    "requests": {"cpu": "100m", "memory": "256Mi"},
    "limits": {"cpu": "1", "memory": "1Gi"},
}


def _resources(spec: dict | None) -> k8s.V1ResourceRequirements:
    spec = spec or _DEFAULT_RESOURCES
    return k8s.V1ResourceRequirements(requests=spec.get("requests"), limits=spec.get("limits"))


def etl_step_task(pipeline: str, step: str, image: str, resources: dict | None = None) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=step,
        name=f"{pipeline}-{step}",
        namespace=NAMESPACE,
        image=image,  # immutable tag from the manifest
        image_pull_policy="Always",  # reused tags would otherwise run stale code
        cmds=["python", "-m", "etl"],
        arguments=[
            "run", pipeline, step,
            "--run-id", "{{ run_id }}",
            "--run-ts", "{{ ts_nodash }}",   # unique per DAG run; the control-dir key
            "--logical-date", "{{ ds }}",
        ],
        env_vars={"MINIO_ENDPOINT": MINIO_ENDPOINT},
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="minio-credentials")),
            # SERVING_DB_URI for the publish/sink step (Postgres serving layer for BI)
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="serving-db", optional=True)),
        ],
        # 1-slot pool per pipeline -> serializes ALL runs (scheduled + backfill + manual) so
        # two pods never write the same Delta tables at once (backfill ignores max_active_runs,
        # which previously caused silent concurrent-writer data loss).
        pool=f"etl_{pipeline}",
        container_resources=_resources(resources),
        security_context=k8s.V1SecurityContext(
            run_as_non_root=True,
            run_as_user=50000,
            allow_privilege_escalation=False,
        ),
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
    )
