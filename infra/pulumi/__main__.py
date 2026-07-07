"""Local Airflow-on-Kubernetes ETL platform, provisioned end-to-end with Pulumi.

Replaces the Makefile as the entry point:  pulumi up  /  pulumi destroy.

Layers (Pulumi computes the dependency order automatically):
  1. kind cluster + local registry            -> pulumi_command (no first-class kind provider)
  2. kubeconfig -> Kubernetes provider
  3. etl + superset images                     -> pulumi_docker (build + push to localhost:5001)
  4. MinIO / Dremio / Superset / serving-PG    -> reuse the existing manifests via ConfigGroup
  5. Airflow (KubernetesExecutor + git-sync)   -> Helm release
  6. per-pipeline 1-slot pools                 -> pulumi_command (kubectl/airflow)

App-level BI wiring (Superset chart, Dremio source) stays as the committed scripts
(infra/superset/setup_superset.py) — run them after `pulumi up` (see README).
"""

import os

import pulumi
import pulumi_command as command
import pulumi_docker as docker
import pulumi_kubernetes as k8s
from pulumi_kubernetes.helm.v3 import Release, ReleaseArgs, RepositoryOptsArgs
from pulumi_kubernetes.yaml import ConfigGroup

cfg = pulumi.Config()
CLUSTER = cfg.get("cluster") or "etl"
TAG = cfg.get("imageTag") or "dev"
REGISTRY = "localhost:5001"
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Run command resources through git-bash with our tool dir (kind/helm) on PATH.
BASH = ["bash", "-c"]
PATH_PREFIX = 'export PATH="$HOME/bin:$PATH"; '


def sh(create: str, delete: str = "", **kw):
    return dict(interpreter=BASH, create=PATH_PREFIX + create,
                delete=(PATH_PREFIX + delete) if delete else None, **kw)


# --------------------------------------------------------------- 1. kind + registry
registry = command.local.Command(
    "registry",
    **sh(f"bash {REPO}/infra/registry/setup-registry.sh",
         "docker rm -f kind-registry || true"),
)

cluster = command.local.Command(
    "kind-cluster",
    **sh(f"kind create cluster --name {CLUSTER} --config {REPO}/infra/kind/kind-cluster.yaml || true",
         f"kind delete cluster --name {CLUSTER}"),
    opts=pulumi.ResourceOptions(depends_on=[registry]),
)

wire = command.local.Command(
    "registry-wire",
    **sh(f"bash {REPO}/infra/registry/connect-registry.sh {CLUSTER}"),
    opts=pulumi.ResourceOptions(depends_on=[cluster]),
)

kubeconfig = command.local.Command(
    "kubeconfig",
    **sh(f"kind get kubeconfig --name {CLUSTER}"),
    opts=pulumi.ResourceOptions(depends_on=[cluster]),
)

k8s_provider = k8s.Provider("k8s", kubeconfig=kubeconfig.stdout)
k8s_opts = pulumi.ResourceOptions(provider=k8s_provider, depends_on=[wire])

# --------------------------------------------------------------- 2. images
etl_image = docker.Image(
    "etl-image",
    image_name=f"{REGISTRY}/etl:{TAG}",
    build=docker.DockerBuildArgs(context=f"{REPO}/etl", platform="linux/amd64"),
    registry=docker.RegistryArgs(server=REGISTRY),
    skip_push=False,
    opts=pulumi.ResourceOptions(depends_on=[registry]),
)

superset_image = docker.Image(
    "superset-image",
    image_name=f"{REGISTRY}/superset:{TAG}",
    build=docker.DockerBuildArgs(context=f"{REPO}/infra/superset", platform="linux/amd64"),
    registry=docker.RegistryArgs(server=REGISTRY),
    skip_push=False,
    opts=pulumi.ResourceOptions(depends_on=[registry]),
)

# --------------------------------------------------------------- 3. workloads (reuse manifests)
minio = ConfigGroup(
    "minio",
    files=[f"{REPO}/infra/minio/*.yaml"],
    opts=k8s_opts,
)

serving_pg = ConfigGroup(
    "serving-postgres",
    files=[f"{REPO}/infra/superset/namespace.yaml", f"{REPO}/infra/superset/serving-postgres.yaml"],
    opts=k8s_opts,
)

superset = ConfigGroup(
    "superset",
    files=[f"{REPO}/infra/superset/deployment.yaml", f"{REPO}/infra/superset/service.yaml"],
    opts=pulumi.ResourceOptions(provider=k8s_provider, depends_on=[wire, serving_pg, superset_image]),
)

dremio = ConfigGroup(
    "dremio",
    files=[f"{REPO}/infra/dremio/*.yaml"],
    opts=k8s_opts,
)

# Airflow namespace + secrets (consumed by the KPO/etl pods) before the Helm release.
airflow_pre = ConfigGroup(
    "airflow-pre",
    files=[
        f"{REPO}/infra/helm/namespace.yaml",
        f"{REPO}/infra/helm/minio-credentials.yaml",
        f"{REPO}/infra/helm/serving-db-credentials.yaml",
    ],
    opts=k8s_opts,
)

# --------------------------------------------------------------- 4. Airflow (Helm)
airflow = Release(
    "airflow",
    ReleaseArgs(
        chart="airflow",
        version="1.22.0",
        repository_opts=RepositoryOptsArgs(repo="https://airflow.apache.org"),
        namespace="airflow",
        create_namespace=False,  # created by airflow_pre
        value_yaml_files=[pulumi.FileAsset(f"{REPO}/infra/helm/airflow-values.yaml")],
        timeout=900,
    ),
    opts=pulumi.ResourceOptions(provider=k8s_provider, depends_on=[airflow_pre, etl_image]),
)

# --------------------------------------------------------------- 5. per-pipeline 1-slot pools
pools = command.local.Command(
    "pools",
    **sh(
        "kubectl -n airflow rollout status deploy/airflow-scheduler --timeout=300s; "
        f"for p in $(grep -E '^- id:' {REPO}/dags/pipelines.generated.yaml | awk '{{print $3}}'); do "
        "kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- "
        "airflow pools set etl_$p 1 \"serialize $p writers\"; done"
    ),
    opts=pulumi.ResourceOptions(depends_on=[airflow]),
)

# --------------------------------------------------------------- exports
pulumi.export("cluster", CLUSTER)
pulumi.export("kube_context", f"kind-{CLUSTER}")
pulumi.export("airflow_ui", "kubectl -n airflow port-forward svc/airflow-api-server 8080:8080  # admin/admin")
pulumi.export("superset_ui", "kubectl -n superset port-forward svc/superset 8088:8088  # admin/admin")
pulumi.export("minio_console", "kubectl -n minio port-forward svc/minio 9001:9001  # minioadmin/minioadmin123")
pulumi.export("etl_image", etl_image.image_name)
