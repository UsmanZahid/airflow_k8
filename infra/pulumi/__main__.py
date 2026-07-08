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
# Forward-slash path for Python/Pulumi/docker; MSYS path (/c/...) for git-bash command strings
# (backslashes in a bash -c string get eaten as escapes).
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")).replace("\\", "/")
REPO_BASH = ("/" + REPO[0].lower() + REPO[2:]) if REPO[1:2] == ":" else REPO

# Run command resources through git-bash with our tool dir (kind/helm) on PATH.
BASH = ["bash", "-c"]
PATH_PREFIX = 'export PATH="$HOME/bin:$PATH"; '


def sh(create: str, delete: str = "", **kw):
    return dict(interpreter=BASH, create=PATH_PREFIX + create,
                delete=(PATH_PREFIX + delete) if delete else None, **kw)


# --------------------------------------------------------------- 1. kind + registry
registry = command.local.Command(
    "registry",
    **sh(f"bash {REPO_BASH}/infra/registry/setup-registry.sh",
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
    **sh(f"bash {REPO_BASH}/infra/registry/connect-registry.sh {CLUSTER}"),
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
# Build + push via the docker CLI (command), not pulumi_docker.Image: the latter tars the
# whole build context and chokes ("invalid tar header") on the 600 MB etl/.venv, whereas the
# CLI respects .dockerignore and handles it fine.
etl_image = command.local.Command(
    "etl-image",
    **sh(f"docker build -t {REGISTRY}/etl:{TAG} {REPO_BASH}/etl && docker push {REGISTRY}/etl:{TAG}"),
    opts=pulumi.ResourceOptions(depends_on=[registry]),
)

superset_image = command.local.Command(
    "superset-image",
    **sh(f"docker build -t {REGISTRY}/superset:{TAG} {REPO_BASH}/infra/superset && docker push {REGISTRY}/superset:{TAG}"),
    opts=pulumi.ResourceOptions(depends_on=[registry]),
)

# --------------------------------------------------------------- 3. workloads (reuse manifests)
# ALL namespaces first, as one group everything else depends on. ConfigGroup does not order
# namespace-before-resource on a fresh cluster, so applying namespaced resources in the same
# (or a parallel) group races the namespace -> "namespace not found". This removes that race.
namespaces = ConfigGroup("namespaces", files=[
    f"{REPO}/infra/minio/namespace.yaml",
    f"{REPO}/infra/superset/namespace.yaml",   # superset ns + superset-secret
    f"{REPO}/infra/dremio/namespace.yaml",
    f"{REPO}/infra/helm/namespace.yaml",        # airflow ns
], opts=k8s_opts)


def _after(*deps):
    # wait for the namespaces (+ registry wiring) and this stage's serial predecessor
    return pulumi.ResourceOptions(provider=k8s_provider, depends_on=[wire, namespaces, *deps])


# Deploy SEQUENTIALLY so the heavy pods don't all start at once and spike memory:
# minio -> serving-pg -> superset -> airflow -> dremio (last). namespace.yaml excluded (above).
minio = ConfigGroup("minio", files=[
    f"{REPO}/infra/minio/secret.yaml",
    f"{REPO}/infra/minio/deployment.yaml",
    f"{REPO}/infra/minio/service.yaml",
    f"{REPO}/infra/minio/create-buckets-job.yaml",
], opts=_after())

serving_pg = ConfigGroup("serving-postgres",
    files=[f"{REPO}/infra/superset/serving-postgres.yaml"], opts=_after(minio))

superset = ConfigGroup("superset",
    files=[f"{REPO}/infra/superset/deployment.yaml", f"{REPO}/infra/superset/service.yaml"],
    opts=_after(serving_pg, superset_image))

airflow_pre = ConfigGroup("airflow-pre", files=[
    f"{REPO}/infra/helm/minio-credentials.yaml",
    f"{REPO}/infra/helm/serving-db-credentials.yaml",
], opts=_after(superset))

# --------------------------------------------------------------- 4. Airflow (Helm)
airflow = Release(
    "airflow",
    ReleaseArgs(
        name="airflow",  # pin the release name (else Pulumi auto-suffixes -> airflow-xxxx-scheduler)
        chart="airflow",
        version="1.22.0",
        repository_opts=RepositoryOptsArgs(repo="https://airflow.apache.org"),
        namespace="airflow",
        create_namespace=False,  # created by airflow_pre
        value_yaml_files=[pulumi.FileAsset(f"{REPO}/infra/helm/airflow-values.yaml")],
        timeout=1800,  # Airflow can be slow to fully init on a loaded local machine
    ),
    opts=pulumi.ResourceOptions(provider=k8s_provider, depends_on=[airflow_pre, etl_image]),
)

# Dremio LAST (heaviest / least critical) — only after Airflow is up, so its JVM doesn't
# compete for memory during Airflow's startup.
dremio = ConfigGroup(
    "dremio",
    files=[f"{REPO}/infra/dremio/deployment.yaml", f"{REPO}/infra/dremio/service.yaml"],
    opts=pulumi.ResourceOptions(provider=k8s_provider, depends_on=[wire, namespaces, airflow]),
)

# --------------------------------------------------------------- 5. per-pipeline 1-slot pools
pools = command.local.Command(
    "pools",
    **sh(
        "kubectl -n airflow rollout status deploy/airflow-scheduler --timeout=300s; "
        f"for p in $(grep -E '^- id:' {REPO_BASH}/dags/pipelines.generated.yaml | awk '{{print $3}}'); do "
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
pulumi.export("etl_image", f"{REGISTRY}/etl:{TAG}")
