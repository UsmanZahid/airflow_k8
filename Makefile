# airflow_k8 — developer workflows.
# Note: on Windows use Git Bash (these targets shell out to bash/kubectl/helm/kind/docker).

REGISTRY      ?= localhost:5001
IMAGE         ?= $(REGISTRY)/etl
TAG           ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
CLUSTER       ?= etl
NS_AIRFLOW    ?= airflow
NS_MINIO      ?= minio
CHART_VERSION ?= 1.22.0

.PHONY: help cluster-up registry minio etl-image manifest airflow pools up down ui logs test new-pipeline

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

cluster-up: ## Create kind cluster + local registry
	bash infra/registry/setup-registry.sh
	kind create cluster --name $(CLUSTER) --config infra/kind/kind-cluster.yaml || true
	bash infra/registry/connect-registry.sh $(CLUSTER)

minio: ## Deploy MinIO + create the `lake` bucket
	kubectl apply -f infra/minio/namespace.yaml
	kubectl apply -f infra/minio/secret.yaml
	kubectl apply -f infra/minio/deployment.yaml
	kubectl apply -f infra/minio/service.yaml
	kubectl -n $(NS_MINIO) rollout status deploy/minio --timeout=120s
	kubectl apply -f infra/minio/create-buckets-job.yaml
	kubectl -n $(NS_MINIO) wait --for=condition=complete job/create-buckets --timeout=120s

etl-image: ## Build + push the etl image (immutable tag = git sha)
	docker build -t $(IMAGE):$(TAG) etl
	docker push $(IMAGE):$(TAG)
	@echo "pushed $(IMAGE):$(TAG)"

manifest: ## Regenerate dags/pipelines.generated.yaml from Python (stamped with image tag)
	cd etl && uv run etl export-manifest --image $(IMAGE):$(TAG) --out ../dags/pipelines.generated.yaml
	@echo "wrote dags/pipelines.generated.yaml"

airflow: ## helm install Airflow (KubernetesExecutor + git-sync)
	kubectl apply -f infra/helm/namespace.yaml
	kubectl apply -f infra/helm/minio-credentials.yaml
	helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
	helm repo update >/dev/null
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(NS_AIRFLOW) --version $(CHART_VERSION) \
		-f infra/helm/airflow-values.yaml --timeout 10m

pools: ## Create the 1-slot serialization pool for each pipeline (serializes writers)
	@for p in $$(grep -E '^- id:' dags/pipelines.generated.yaml | awk '{print $$3}'); do \
	  echo "pool etl_$$p"; \
	  kubectl -n $(NS_AIRFLOW) exec deploy/airflow-scheduler -c scheduler -- \
	    airflow pools set etl_$$p 1 "serialize $$p writers"; \
	done

up: cluster-up minio etl-image manifest airflow pools ## Everything, in order

down: ## Delete the kind cluster
	kind delete cluster --name $(CLUSTER)

ui: ## Port-forward Airflow (8080) and MinIO console (9001)
	@echo "Airflow: http://localhost:8080  |  MinIO console: http://localhost:9001"
	kubectl -n $(NS_AIRFLOW) port-forward svc/airflow-api-server 8080:8080 & \
	kubectl -n $(NS_MINIO) port-forward svc/minio 9001:9001

logs: ## Tail the scheduler
	kubectl -n $(NS_AIRFLOW) logs -f deploy/airflow-scheduler

test: ## Run etl unit tests (no cluster; uses LocalRunContext)
	cd etl && uv run pytest -q

new-pipeline: ## Scaffold a new pipeline: make new-pipeline name=foo
	cd etl && uv run etl new-pipeline $(name)
