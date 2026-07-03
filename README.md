# airflow_k8 — Local Airflow-on-Kubernetes ETL Platform

A local, reproducible testing platform where **Apache Airflow** runs as Kubernetes pods
inside a **kind** cluster, orchestrates many ETL pipelines, and spawns **one task pod per
step** via `KubernetesPodOperator`. A shared **MinIO** object store is the data lake; ETL
is written in **Polars + Delta Lake** (`delta-rs`, no Spark) as a **medallion**
(bronze/silver/gold) lakehouse with first-class **late-arriving-data** handling.

See the design/architecture in `docs/PLAN.md` (mirrored from the planning phase).

## Layout

```
infra/    where things run   — kind cluster, local registry, MinIO, Airflow Helm values
dags/     orchestration only  — KPO factory + a single generate_dags.py (manifest-driven)
etl/      business logic      — the framework + one folder per pipeline (own image)
```

Boundaries: `dags/` never imports `etl/`; Airflow's Python env stays ETL-dependency-free
(DAGs render from a generated YAML manifest).

## Quick start

```bash
# 1. Local ETL dev loop (no cluster needed)
cd etl && uv sync && uv run pytest        # unit tests via LocalRunContext

# 2. Stand up the cluster
make cluster-up      # kind + local registry
make minio           # deploy MinIO + create the `lake` bucket
make etl-image       # build + push the etl image (immutable tag)
make airflow         # helm install Airflow (KubernetesExecutor + git-sync)
make manifest        # regenerate dags/pipelines.generated.yaml from Python
make up              # all of the above, in order

make ui              # port-forward Airflow (8080) + MinIO console (9001)
make down            # kind delete cluster
```

## Add a new pipeline

```bash
make new-pipeline name=<name>     # scaffold etl/src/etl/pipelines/<name>/
# implement steps.py + register the Pipeline in pipeline.py
make manifest && make etl-image && git commit -am "add <name>" && git push
# git-sync brings the new DAG in — no Airflow code edited.
```

Credentials and single-node MinIO/Postgres here are **test-only**, not production-safe.
