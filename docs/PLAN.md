# Local Airflow-on-Kubernetes ETL Platform (kind + MinIO + Polars/Delta)

## Context

Goal: a **local, reproducible testing platform** where Apache Airflow runs entirely as
Kubernetes pods inside a `kind` (Kubernetes-in-Docker) cluster, orchestrates **many
(10–15) ETL pipelines** that each have multiple steps, and spawns a task pod per step via
`KubernetesPodOperator` (KPO). A shared **MinIO** object store is the data lake; ETL is
written in **Polars + Delta Lake** (`delta-rs` — no Spark) laid out as a **medallion
(bronze/silver/gold)** lakehouse.

The overriding requirement is a **simple, uniform framework** so that adding and
maintaining pipelines is near-boilerplate-free: a developer adds a folder of Python,
regenerates a manifest + image, commits, and a new DAG appears — with **no Airflow code
changes**. Orchestration (Airflow/DAGs) is kept fully decoupled from business logic (ETL).

The directory is currently empty — a from-scratch build.

### Confirmed decisions
- **Airflow deploy:** official `apache-airflow` Helm chart (one `values.yaml`).
- **Executor:** `KubernetesExecutor` (each Airflow task → its own pod).
- **ETL structure:** separate `etl/` project → its own container image; DAGs only launch it.
- **DAG delivery:** git-sync sidecar (Airflow pulls `dags/` from a git repo).
- **Pipeline contract:** **Python-first + generated manifest** (recommended below).
- **Pod granularity:** **one pod per step** (granular retries/observability; steps hand
  data off through Delta tables on MinIO).
- **Data layout:** **medallion** — `bronze` (raw), `silver` (clean/conformed), `gold`
  (curated) Delta tables on MinIO.
- **Update handling (core requirement):** records (esp. cost rows) **arrive/update late**
  and must **propagate to downstream aggregates**. Strategy: **upsert (MERGE) by business
  key** into persistent Delta tables, **partition by business dimension** (e.g. cost
  period/center — *not* by run), and **recompute only affected partitions** downstream by
  reading all rows for those keys (old + late runs) and atomically replacing just those
  partitions (`replaceWhere`). `run_ts` is an audit column + a per-run control dir, not a
  data partition. See "Update semantics & late-arriving data" below.

### The core design idea (why this is maintainable at 15 pipelines)
A pipeline is declared **once, in Python**, next to its logic. A CLI exports a tiny
**YAML manifest** (`dags/pipelines.generated.yaml`, committed) describing each pipeline's
steps/order/schedule. **One** `dags/generate_dags.py` reads that manifest and renders one
Airflow DAG per pipeline. Consequences:
- Single source of truth = Python; the manifest is a generated artifact that *can't* drift.
- Airflow's env has **zero ETL deps** (only PyYAML) → fast, robust DAG parsing, immune to
  polars/delta upgrades.
- Adding a pipeline touches **no Airflow code** — just a new `pipelines/<name>/` folder.

## Target architecture

```
                 kind cluster (Docker)
  ┌───────────────────────────────────────────────────────────────┐
  │ ns: airflow                                                     │
  │   postgres (metadata DB) [Helm]                                 │
  │   scheduler + git-sync │ apiserver + git-sync │ triggerer       │
  │   dag-processor: runs generate_dags.py -> N DAGs from manifest  │
  │        │ KubernetesExecutor (worker pod per task)               │
  │        ▼                                                        │
  │   [worker pod] ──KPO──▶ [etl:<tag> pod]  cmd: etl run <pl> <st> │
  │                               │ Polars + Delta (delta-rs)       │
  │ ns: minio                     │ S3 API                          │
  │   minio (Deployment+PVC+Svc)  ◀── read/write Delta tables ──────┘
  │   buckets: lake (bronze/silver/gold)                            │
  └───────────────────────────────────────────────────────────────┘
     local registry localhost:5001 -> feeds etl image into kind
```

## Repository layout

```
airflow_k8/
├── README.md
├── Makefile
├── infra/
│   ├── kind/kind-cluster.yaml           # nodes, registry wiring, port maps
│   ├── registry/setup-registry.sh       # local docker registry <-> kind
│   ├── minio/                           # namespace, secret, deployment+PVC, service,
│   │   └── (create-buckets-job.yaml)     # mc job: create `lake` bucket + layer prefixes
│   └── helm/
│       ├── namespace.yaml
│       ├── minio-credentials.yaml        # Secret in airflow ns for KPO/etl pods
│       └── airflow-values.yaml           # KubernetesExecutor, gitSync, extras=PyYAML
├── dags/                                # git-synced (subPath: dags)
│   ├── common/kpo.py                     # etl_step_task() KPO factory
│   ├── generate_dags.py                  # renders ALL DAGs from the manifest
│   └── pipelines.generated.yaml          # committed build artifact (from `make manifest`)
├── etl/                                 # SEPARATE project -> its own image
│   ├── pyproject.toml                    # polars, deltalake, pyyaml, typer, pydantic
│   ├── Dockerfile                        # ENTRYPOINT ["python","-m","etl"]
│   ├── src/etl/
│   │   ├── __main__.py / cli.py          # `etl run|list|export-manifest`
│   │   ├── framework/
│   │   │   ├── step.py                    # Step base class (output Dataset, .read()/.write())
│   │   │   ├── pipeline.py               # Pipeline base + REGISTRY + @register
│   │   │   ├── context.py                # RunContext: read/write/upsert/read_partitions/replace_partitions/affected
│   │   │   ├── storage.py                # MinIO storage_options() for delta-rs
│   │   │   ├── catalog.py                # Dataset (layer/pipeline/table, key, partition_by)
│   │   │   ├── changeset.py              # affected keys/partitions marker I/O under _runs/<run_ts>/ (used by RunContext)
│   │   │   ├── runner.py                  # instantiate + run a single step class
│   │   │   ├── manifest.py               # REGISTRY -> manifest dict/YAML
│   │   │   └── discovery.py              # import all pipelines.* so they register
│   │   └── pipelines/
│   │       ├── _template/                # copyable skeleton for new pipelines
│   │       ├── costs/                     # exemplar: late-arriving upsert + scoped recompute
│   │       │   ├── pipeline.py            # @register Pipeline subclass (steps=[...])
│   │       │   └── steps.py               # Extract(append)/Clean(upsert)/Aggregate(recompute)
│   │       └── inventory/ ...             # (10–15 folders total)
│   └── tests/                            # pure-Polars unit tests per step
└── scripts/bootstrap.sh                 # optional one-shot end-to-end
```

Boundaries: `infra/` = where things run; `dags/` = orchestration only (no ETL imports);
`etl/` = business logic, independently tested/versioned/containerized.

## Framework design (the heart of the maintainability story)

### Class-based Step model — output is defined once, read by class reference

Each step is a **class** that owns the definition of *where its output lives* (a
`Dataset`). Downstream steps read an upstream step's output via
`UpstreamStep.read(ctx)` — never by re-typing the path. The `Dataset`/path is declared in
exactly one place (the producing step's class), so there is no duplicated path string to
keep in sync. This works across separate pods because `.read()` resolves to reading the
Delta table the upstream pod already wrote to MinIO.

```python
# framework/catalog.py — a Dataset is a named handle to a persistent Delta table on MinIO
@dataclass(frozen=True)
class Dataset:
    layer: str                          # bronze | silver | gold
    pipeline: str
    table: str
    key: tuple[str, ...] = ()           # business/merge key -> enables upsert (MERGE)
    partition_by: tuple[str, ...] = ()  # business dimension (e.g. cost_period) for scoped recompute
    @property
    def uri(self) -> str: return f"s3://lake/{self.layer}/{self.pipeline}/{self.table}"

# framework/step.py
class Step(ABC):
    id: ClassVar[str]                              # stable step id (used by CLI + DAG)
    output: ClassVar[Dataset]                      # THE single definition of this step's output
    upstream: ClassVar[tuple[type["Step"], ...]] = ()

    @classmethod
    def read(cls, ctx: "RunContext") -> pl.DataFrame:   # downstream calls Upstream.read(ctx)
        return ctx.read(cls.output)                     # == StepA.read instead of a hardcoded path
    @classmethod
    def affected_partitions(cls, ctx) -> pl.DataFrame:   # DISTINCT(partition_by) changed this run -> aggregate downstream
        return ctx.affected(cls.output, level="partitions")
    @classmethod
    def affected_keys(cls, ctx) -> pl.DataFrame:         # changed key values this run -> 1:1/row-level downstream
        return ctx.affected(cls.output, level="keys")
    def write(self, ctx, df, mode="overwrite"): ctx.write(self.output, df, mode)
    def write_bronze(self, ctx, df): ctx.write_bronze(self.output, df)   # stamps run_ts; idempotent per run
    def upsert(self, ctx, df, source="incremental"): ctx.upsert(self.output, df, source)  # MERGE (+deletes if snapshot)
    @abstractmethod
    def run(self, ctx: "RunContext") -> None: ...

# Convenience bases that own the affected-set plumbing so authors write only business logic
# (kills the repeated read_groups/agg/replace_groups ceremony and enforces invariant #1):
#   class AggregateStep(Step): group_cols=...; agg=...   # author supplies group cols + agg exprs
#   class MapStep(Step):       # 1:1/row-level: reads affected_keys, transforms, upserts by key

# framework/pipeline.py
class Pipeline(ABC):
    id: ClassVar[str]
    schedule: ClassVar[str | None] = None
    steps: ClassVar[tuple[type[Step], ...]]        # order/edges derived from each .upstream

REGISTRY: dict[str, type[Pipeline]] = {}
def register(p): REGISTRY[p.id] = p; return p      # @register on each Pipeline subclass
```
`discovery.py` imports every `etl.pipelines.<name>.pipeline` so all subclasses register.

### Author-facing shape (`pipelines/costs/`) — shows late-arriving update propagation
```python
# steps.py
class ExtractCosts(Step):
    id = "extract"
    output = Dataset("bronze", "costs", "raw")               # append-only arrival log
    def run(self, ctx):
        batch = fetch_source(ctx.logical_date)               # deterministic per logical_date (retry-safe)
        self.write_bronze(ctx, batch)                        # stamps run_ts; idempotent per run (delete-then-append)

class CleanCosts(Step):
    id = "clean"; source = "snapshot"                        # snapshot -> merge applies deletes too
    output = Dataset("silver", "costs", "fact",
                     key=("cost_id",), partition_by=("cost_period", "cost_center"))
    upstream = (ExtractCosts,)
    def run(self, ctx):
        df = ctx.read_new(ExtractCosts.output).pipe(normalize)  # only THIS run's arrivals, not all history
        self.upsert(ctx, df, source=self.source)                # MERGE by cost_id (+ deletes); records affected

class AggregateCosts(Step):
    id = "aggregate"
    output = Dataset("gold", "costs", "cost_by_period",
                     partition_by=("cost_period", "cost_center"))
    group_cols = ("cost_period", "cost_center")              # == silver partition grain here (invariant #1 ok)
    upstream = (CleanCosts,)
    def run(self, ctx):
        groups = CleanCosts.affected_partitions(ctx).select(self.group_cols).unique()
        if groups.is_empty(): return
        facts = ctx.read_groups(CleanCosts.output, groups, self.group_cols)   # ALL rows for those groups
        agg = facts.group_by(*self.group_cols).agg(pl.col("amount").sum())
        ctx.replace_groups(self.output, agg, groups, self.group_cols)  # atomic replace + emit marker for next hop

# pipeline.py
@register
class Costs(Pipeline):
    id = "costs"; schedule = "@daily"
    steps = (ExtractCosts, CleanCosts, AggregateCosts)
```
DAG edges are derived from each step's `upstream` classes (topological) — you never restate
dependencies, and the read path always matches what the producer wrote. A cost row that
arrives three runs late upserts into `silver.costs.fact`, marks its `(cost_period,
cost_center)` as affected, and `AggregateCosts` recomputes **only that period** from *all*
its rows (across every prior run) — correcting the downstream total without a full rescan.

### Shared IO / storage (Polars + delta-rs on MinIO) — used by `RunContext`
```python
# storage.py — one place that configures S3/MinIO for delta-rs.
# Use conditional-put (If-None-Match/ETag) for SAFE concurrent commits on MinIO — NOT
# AWS_S3_ALLOW_UNSAFE_RENAME (which disables the safety check) and NOT a DynamoDB lock.
def storage_options() -> dict[str,str]:
    return {"AWS_ENDPOINT_URL": os.environ["MINIO_ENDPOINT"],   # full URL incl. scheme + :9000 (S3 API)
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
            "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
            "aws_conditional_put": "etag"}   # safe concurrent writes on MinIO, no lock provider
# context.py — runtime passed into every step; resolves Datasets to Delta I/O.
# run_id/run_ts are identical across ALL step pods of one DAG run (from Airflow {{ run_id }}/{{ ts_nodash }}),
# and exposed process-globally via a contextvar (current_run()) — the "shared global to the run".
class RunContext:
    run_id: str; run_ts: str; logical_date: str    # run_ts plumbed from KPO --run-ts {{ ts_nodash }}
    _so = staticmethod(storage_options)
    def read(self, ds): return pl.scan_delta(ds.uri, storage_options=self._so()).collect()
    def read_new(self, ds):                          # run-scoped slice only (avoids re-processing all history)
        return pl.scan_delta(ds.uri, storage_options=self._so()).filter(pl.col("run_ts")==self.run_ts).collect()
    def write(self, ds, df, mode="overwrite"):
        df.write_delta(ds.uri, mode=mode, storage_options=self._so(),
                       delta_write_options={"partition_by": list(ds.partition_by)} if ds.partition_by else None)

    def upsert(self, ds, df, source="incremental"):  # MERGE by ds.key; source in {incremental, snapshot}
        df = df.unique(subset=ds.key, keep="last")   # merge source MUST be unique on key (else delta-rs errors)
        if not _table_exists(ds.uri, self._so()):    # FIRST RUN: merge/pre-image need an existing table
            self.write(ds, df, mode="overwrite"); self._record_affected(ds, df); return
        pre = self._preimage_partitions(ds, df.select(ds.key))   # partitions these keys occupy BEFORE merge
        m = df.write_delta(ds.uri, mode="merge", storage_options=self._so(),
            delta_merge_options={"predicate": " AND ".join(f"t.{k}=s.{k}" for k in ds.key),
                                 "source_alias": "s", "target_alias": "t"}) \
              .when_matched_update_all().when_not_matched_insert_all()
        if source == "snapshot": m = m.when_not_matched_by_source_delete()  # removals from full snapshots
        m.execute()
        self._record_affected(ds, df, pre)   # changed keys + UNION(pre,post,deleted) partitions (invariants #1/#2)

    # read/replace keyed on the GROUP columns the caller passes (default = ds.partition_by).
    # Coarser gold GROUP BY (invariant #1): pass group_cols = gold's grouping (subset of partition_by).
    def read_groups(self, ds, groups, group_cols=None):
        cols = list(group_cols or ds.partition_by)
        return pl.scan_delta(ds.uri, storage_options=self._so()).join(groups.lazy(), on=cols, how="semi").collect()
    def replace_groups(self, ds, df, groups, group_cols=None):
        cols = list(group_cols or ds.partition_by)
        assert not groups.is_empty(), "empty group set: refusing overwrite (would replace whole table)"
        assert df.select(cols).unique().join(groups, on=cols, how="anti").is_empty(), "df has rows outside groups"
        write_deltalake(ds.uri, df.to_arrow(), mode="overwrite",   # create-if-absent: no predicate on first write
                        predicate=None if not _table_exists(ds.uri, self._so()) else _predicate_for(groups, cols),
                        storage_options=self._so())
        self._record_affected(ds, df)        # MULTI-HOP: emit this gold's changed groups for the next hop

    # change channel under the per-run control dir; ALWAYS writes a (possibly empty) typed marker
    def _record_affected(self, ds, batch, pre=None):
        _write_pq(self._uri(ds, "keys"), batch.select(ds.key).unique() if ds.key else _empty())
        if ds.partition_by:
            post = batch.select(ds.partition_by).unique()
            _write_pq(self._uri(ds, "partitions"), pl.concat([p for p in (pre, post) if p is not None]).unique())
    def affected(self, ds, level):               # tolerant: typed-empty frame if no marker was written
        u = self._uri(ds, level)
        return pl.read_parquet(u) if _exists(u) else _empty_like(ds, level)
```
Because each step is its own pod, the handoff medium is the persisted Delta table; the
class API (`Upstream.read` / `.affected_partitions` / `.affected_keys`) is the typed,
drift-free way to name it. Data tables
are **persistent** (they must accumulate for cross-run upsert/recompute); the per-run
timestamp lives as an audit column and a control dir `s3://lake/_runs/<run_ts>/` that carries
the affected-partition set between pods — that is the "run-scoped dir shared globally to the run".
(Exact delta-rs `merge`/`predicate`-overwrite call shapes to be verified against the pinned
`deltalake`/`polars` versions during implementation.)

### CLI (`etl` entrypoint, e.g. Typer)
- `etl run <pipeline> <step> [--run-id ... --logical-date ...]` → discovery → look up the
  `Pipeline` subclass + its `Step` class in REGISTRY → build `RunContext` → instantiate the
  step class and call `step.run(ctx)` (exactly one step class per pod).
- `etl export-manifest` → `manifest.py` walks REGISTRY, emits YAML — each step's `upstream`
  is resolved from its `upstream` classes to a list of step ids (pipelines: id, schedule,
  steps[{id, upstream}]).
- `etl list` → sanity/debug.

### Manifest (contract consumed by Airflow) — `dags/pipelines.generated.yaml`
```yaml
pipelines:
  - id: costs
    schedule: "@daily"
    steps:
      - {id: extract,   upstream: []}
      - {id: clean,     upstream: [extract]}
      - {id: aggregate, upstream: [clean]}
```

### DAG generation (written once) — `dags/generate_dags.py`
```python
manifest = yaml.safe_load(open(HERE/"pipelines.generated.yaml"))
for p in manifest["pipelines"]:
    with DAG(dag_id=p["id"], schedule=p["schedule"], catchup=False,
             max_active_runs=1,            # REQUIRED: serialize writers to shared Delta tables
             params={"full": False}, default_args=p.get("default_args", {}), tags=p.get("tags")) as dag:
        tasks = {s["id"]: etl_step_task(p["id"], s["id"], image=p["image"], resources=s.get("resources"))
                 for s in p["steps"]}
        for s in p["steps"]:
            for up in s["upstream"]:
                tasks[up] >> tasks[s["id"]]
    globals()[p["id"]] = dag
```
Airflow env needs no extra pip packages: **PyYAML already ships with Airflow**, and the
`cncf.kubernetes` provider (KPO) is bundled in the stock (non-slim) `apache/airflow` image.
(The official chart has **no** `extraPipPackages` value — that key belongs to a different
community chart and would be silently ignored.)

### KPO factory — `dags/common/kpo.py`
`etl_step_task(pipeline, step, image, resources)` returns a `KubernetesPodOperator` with:
`image=<from manifest, immutable tag e.g. localhost:5001/etl:<gitsha>>`, `namespace="airflow"`,
`in_cluster=True`, `cmds=["python","-m","etl"]`,
`arguments=["run", pipeline, step, "--run-id","{{ run_id }}","--run-ts","{{ ts_nodash }}","--logical-date","{{ ds }}"]`
(note `--run-ts` — the control-dir key; do NOT derive it from the logical date),
`image_pull_policy="Always"` (reused tag would otherwise run stale code; immutable tags make this safe),
`env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="minio-credentials"))]` (must be
V1EnvFromSource objects, not a bare name), `env_vars={"MINIO_ENDPOINT":"http://minio.minio.svc.cluster.local:9000"}`,
`container_resources=k8s.V1ResourceRequirements(requests/limits cpu+memory)` (gold pods read whole
groups into memory — an unbounded pod can OOM the node),
`security_context=k8s.V1SecurityContext(runAsNonRoot=True, runAsUser=50000, allowPrivilegeEscalation=False,
readOnlyRootFilesystem=True, capabilities=drop:[ALL])` (etl Dockerfile sets `USER 50000`; mount an
emptyDir at /tmp for delta-rs), `get_logs=True`, `on_finish_action="delete_pod"`.

## Update semantics & late-arriving data (core requirement)

The framework treats "a record shows up or changes in a later run, and downstream
aggregates must be corrected" as a first-class concern, not an afterthought.

**Per-layer write semantics**
- **bronze** — `mode="append"`: immutable log of every arrival, stamped `run_ts`/`ingested_at`.
  Repetitions/updates are all kept (audit trail; time-travel over raw).
- **silver (facts)** — `upsert` (Delta `MERGE`) by business `key`. A late or updated row
  **inserts or updates in place** — one current row per key, regardless of which run it
  arrived in. This is what "find all these records from older runs" resolves to: silver
  always holds the full, current per-key state. Prior values remain via Delta **time travel**.
- **gold (aggregates)** — recompute **only affected partitions**: read the affected
  `(dimension…)` set from silver's run, read *all* silver rows for those partitions (old +
  late), aggregate, and `replace_partitions` (predicate/`replaceWhere` overwrite). Untouched
  periods are never recomputed.

**Change-propagation granularity — keys vs partitions (two flavours, one channel)**
A downstream step consumes change at the granularity *it* needs; both are derived from the
same upsert batch and written to the per-run control dir:
- **affected partitions** — `DISTINCT(partition_by)` of the batch. For **aggregate**
  downstream (cost rollups): a changed record forces re-summing its *whole* period, so
  downstream reads all rows of those partitions, recomputes, and `replace_partitions`. Used by `costs`.
- **affected keys** — the changed `key` values of the batch. For **1:1 / row-level**
  downstream (enrichment/mapping, one row in → one row out): downstream semi-joins silver on
  just those keys, transforms them, and upserts by key. No group recompute needed.
`Step.affected_partitions(ctx)` / `Step.affected_keys(ctx)` expose the two; a step picks one.

**Do NOT physically partition by `record_id`.** `record_id` is the MERGE **key**, not a
partition. Partitioning by a primary key is high-cardinality partitioning → one tiny file
+ one `_delta_log` entry *per record* → transaction-log/checkpoint bloat, catastrophic small-file
read cost, and it still can't gather "all rows of period P" for aggregation. Partition by the
**aggregation/query dimension** (moderate cardinality, aligned with rollups + `replaceWhere`);
for fast point access by `record_id`, rely on Delta per-file **statistics (data skipping)** /
clustering, not partitions. Knowing *what changed* comes from the batch, never from the layout.

**How the affected set propagates across pods**
1. `CleanCosts.upsert(...)` records the batch's `DISTINCT(partition_by)` (and/or changed keys) to
   `s3://lake/_runs/<run_ts>/affected/<pipeline>/<table>.parquet`.
2. `AggregateCosts` (a *separate pod*, same `run_ts`) calls `CleanCosts.affected_partitions(ctx)` to read it.
3. Only those partitions are recomputed and atomically replaced in gold.

**Aggregating a changed record — full recompute of the affected partition (NOT incremental math)**
The aggregate step re-derives each affected partition's value from *all* of silver's current
rows for that partition; it never patches the old aggregate. Worked example — `SUM(amount)`
grouped/partitioned by `(cost_period, cost_center)`:
- Run 1: A=100,B=50 in (2026-05,X); C=200 in (2026-06,X) → gold (2026-05,X)=150, (2026-06,X)=200.
- Run 2 batch: A updated 100→130, late D=20 (both in 2026-05,X). silver upsert by `cost_id`:
  A matched→update, D not matched→insert; B,C untouched. Affected = `{(2026-05,X)}` only.
  Recompute reads ALL current silver rows of (2026-05,X) = A(130)+B(50)+D(20) → **200**;
  `replace_partitions` overwrites just that partition. (2026-06,X) is never touched → stays 200.
Why full-recompute: the group total needs the *unchanged* rows too (B's 50), so we read every
row of the partition — summing only changed rows would be wrong. This is correct for any
aggregate (SUM/COUNT/AVG/MIN/MAX/COUNT DISTINCT), handles insert/update/delete uniformly, and
is idempotent (safe for retries). Cost is bounded to affected partitions only.

**Grain — name the two levels correctly (items within a cost):** the MERGE `key` is the
*atomic record* (`item_id`), the `partition_by`/group is the *aggregation grain* (`cost_id`).
Example — gold = `MEAN(amount)` per `cost_id`: iter 1 items a1=10,a2=20 → mean(A)=15; iter 2 a
new item a3=30 for the same cost A → merge-by-`item_id` **inserts** a3 into partition
`cost_id=A`, marks `{cost_id=A}` affected, and gold recomputes A over **all** its current items
{a1,a2,a3} → mean=20. A distinct `item_id` is what distinguishes "new item for existing cost"
(insert) from "correction to a1" (update) from "a2 removed" (delete) — all three just re-mean
the group. Non-additive aggregates (MEAN/MIN/MAX/COUNT DISTINCT) are precisely why we
full-recompute the group rather than patch. (This also re-confirms: partition by the *group*
`cost_id`, never by the record `item_id`, or a cost's items scatter and can't be gathered.)

**Two invariants the framework enforces (else silent drift):**
1. **Partition granularity aligns with the GROUP BY.** Recompute-a-partition == recompute-one-group
   only if silver's `partition_by` matches/maps onto gold's grouping keys. If gold groups coarser
   (e.g. `cost_period` only) the framework widens the affected set to that grouping and recomputes
   all finer members together.
2. **Partition-moving updates mark BOTH old and new partitions.** If an update changes a partition
   column (e.g. period corrected 2026-05→2026-04), the old partition must drop the row and the new
   must gain it. So `upsert` captures each changed key's **pre-image** partition (read from silver
   *before* the merge) and unions it with the post-image partition into the affected set.

**Multi-hop:** a downstream gold-of-gold (e.g. `cost_by_period → cost_by_quarter`) takes the
*changed* partitions of the first gold as its affected set and recomputes only those — the channel
carries forward hop by hop.

This bounds every run's work to *what actually changed*, while guaranteeing downstream
totals reflect late data. Multi-hop chains (gold feeding another gold) carry the affected
set forward the same way. Backfill = re-run with a wider affected set (or a `--full` flag
that treats all partitions as affected).

**Why not the alternatives** (for this workload): plain *append* double-counts in
aggregates and needs latest-per-key reads everywhere; *partition-by-run* re-writes unchanged
rows every run and can't produce a compact current state; full *overwrite* rescans the whole
lake each run and is unsafe with incremental sources. Upsert + affected-partition recompute
is the standard lakehouse answer and is supported by delta-rs without Spark.

**Optional SCD Type 2**: if a table needs queryable history (not just time-travel), a
`SCD2` mixin can implement close-old-row/open-new-row with `valid_from/valid_to/is_current`
— same `key`/`partition_by` contract. Deferred unless a pipeline asks for it.

**Sources are mixed** (full-snapshot vs incremental) → declared per pipeline; the upsert
path is correct for both (snapshot upserts touch more keys, incremental fewer).

## Infra (verified against current tooling, 2026)
- **kind + local registry:** use the **current** official recipe — `registry:3` on
  `127.0.0.1:5001`, `containerdConfigPatches` setting `config_path=/etc/containerd/certs.d`,
  a per-node `hosts.toml` mapping `localhost:5001`→`http://kind-registry:5000`, and
  `docker network connect kind kind-registry`. `setup-registry.sh` must do BOTH the network
  connect and the per-node hosts.toml (loop `kind get nodes`) or pulls fail. Build images with
  an **immutable tag** (`etl:<gitsha>`); `kind load docker-image` is a registry-free fallback.
- **MinIO:** standalone Deployment + PVC + Service (`:9000` S3 API, `:9001` **object-browser**
  console — admin UI was removed from community edition, so create buckets via the `mc` Job, not
  the UI) in `minio` ns. **Pin an explicit RELEASE tag** for `minio/minio` and `minio/mc` (the
  community images are archived/frozen — no `:latest` reliance). Configure the server with
  `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`. The `mc` Job: `mc alias set` → wait `mc ready local`
  → `mc mb --ignore-existing local/lake` (bronze/silver/gold are just key prefixes delta-rs
  creates on write — only `lake` is pre-created); `restartPolicy: Never` + `backoffLimit` so it
  reaches Completed idempotently. Creds in a Secret (test-only), mirrored into `airflow` ns as
  `minio-credentials`. Add pod labels now so a prod NetworkPolicy (airflow→minio:9000) is writable later.
- **Airflow Helm values** (chart 1.22.x / Airflow 3.2.x): `executor: KubernetesExecutor`
  (default is Celery — must set); stock **non-slim** `apache/airflow` image (KPO provider bundled);
  use the `apiServer:` section (Airflow 3 replaced `webserver:`) and the first-class `dagProcessor:`.
  `dags.gitSync.enabled: true` + `repo` + `subPath: dags` + `branch`, `dags.persistence.enabled: false`
  (public GitHub repo simplest; **private HTTPS** → `credentialsSecret` (keys `GIT_SYNC_USERNAME`/
  `GIT_SYNC_PASSWORD`); **SSH deploy key** → `sshKeySecret` (key `gitSshKey`) **+ `sshKnownHosts`**,
  read-only deploy key; optional in-cluster Gitea for fully-local). `postgresql.enabled: true`.
  Keep `rbac.create`/`allowPodLaunching` defaults — the chart's namespaced `pod-launcher-role`
  already lets workers create the KPO pods in-namespace (no ClusterRole, no `multiNamespaceMode`).
  **No `extraPipPackages`** (see above). Airflow's own S3 connection points at the MinIO endpoint
  with `aws_conditional_put=etag`.

## "Add a new pipeline" workflow (the payoff)
1. `make new-pipeline name=<name>` (token-substituting scaffolder — sets the pipeline name once).
2. Implement `steps.py` (a `Step`/`AggregateStep`/`MapStep` subclass per step: set `output`
   Dataset + `upstream`, read upstreams via `Upstream.read`/`.affected_partitions`) and
   register the `Pipeline` subclass in `pipeline.py`. `register()` validates ids/edges/dataset-pipeline.
3. `make manifest` → regenerates `dags/pipelines.generated.yaml` (stamped with the image digest).
4. `make etl-image` → rebuild + push immutable-tagged image.
5. `git commit && git push` → git-sync brings the new DAG; scheduler renders it. CI runs
   `export-manifest` + `git diff --exit-code` so a stale manifest can't ship.
No Airflow code edited. Steps are unit-testable with plain Polars frames via `LocalRunContext`.

## Build order
1. Repo scaffold + `README.md` + `Makefile`.
2. `infra/kind` + `infra/registry`; cluster up.
3. `infra/minio`; deploy + create `lake` bucket; verify console.
4. `etl/` framework + `_template` + the `costs` exemplar (append→upsert→scoped recompute).
   **Smoke-test at pinned versions (via `LocalRunContext`, no cluster):** merge upsert,
   predicate/`replaceWhere` group-replace, time travel, MinIO conditional-put write, AND the
   blocker cases — first-ever run (create-if-absent), snapshot **delete**, **partition-moving**
   update (old+new recompute), coarser gold GROUP BY (invariant #1), empty/missing marker
   no-op, multi-hop marker emission. `pytest` green; build + push immutable-tagged image.
5. `infra/helm/airflow-values.yaml` + secrets; `helm install`.
6. `dags/common/kpo.py` + `dags/generate_dags.py`; `make manifest`; push to git repo.
7. Add 1–2 more pipelines to prove the "add a pipeline" flow end-to-end.

## Makefile targets
`cluster-up`, `minio`, `airflow`, `etl-image`, `manifest`, `new-pipeline name=…`,
`up` (ordered all), `down` (`kind delete cluster`), `ui` (port-forward Airflow+MinIO),
`test` (`pytest` in etl/), `logs`.

## Verification (end-to-end)
- **Cluster/registry/MinIO:** `kubectl get nodes` Ready; push+pull `etl:dev`; MinIO console
  shows the `lake` bucket.
- **Airflow:** pods Running with healthy git-sync sidecars; UI reachable.
- **Manifest→DAGs:** every pipeline in `pipelines.generated.yaml` appears as a DAG in the UI.
- **Per-step pods:** trigger `costs`; `kubectl get pods -n airflow -w` shows a worker
  pod per task, each spawning an `etl` KPO pod, all terminating on success.
- **Medallion/shared storage:** MinIO console shows Delta tables progressing
  `bronze → silver → gold` under `lake/<layer>/costs/…`, proving pods share MinIO.
- **Granular retry:** fail one step, clear it in the UI, confirm only that step's pod reruns.
- **Late-arriving update propagation (the key test):** run `costs` once; note a period's gold
  total. Feed a run whose batch updates one old cost row (and adds a late row for an old
  period). Confirm: silver upserts (one row per `cost_id`, prior value visible via time
  travel), only the affected `(cost_period, cost_center)` partitions recompute (check
  `_runs/<run_ts>/affected/…`), and gold's total for that period is corrected while other
  periods' files are untouched.
- **Delete propagation:** in a snapshot run, drop a record; confirm silver removes it, its
  group is marked affected, and the gold aggregate for that group drops the amount.
- **First run & empty run:** a brand-new pipeline's first run succeeds (create-if-absent); a
  run where nothing changed no-ops (no full-table overwrite, no crash on missing marker).
- **Concurrency guard:** confirm `max_active_runs=1`; a manual trigger during a scheduled run
  queues rather than double-writing; conditional-put commits don't corrupt `_delta_log`.
- **Add-a-pipeline:** run the `make new-pipeline` flow and confirm its DAG shows up after
  `make manifest` + push — no edits to `generate_dags.py`; CI `git diff --exit-code` on the
  generated manifest passes (manifest↔image in sync).
- **Unit tests:** `make test` passes without a cluster (via `LocalRunContext`).
- **Teardown:** `make down` removes everything.

## Hardening (from multi-agent review) — fold into implementation

Blockers/highs are already fixed in the sketches above. This section captures the rest so
nothing is lost, grouped by area. Treat the **blocker/high** items as build-time acceptance
criteria (most have a smoke test in build step 4).

**Correctness (must hold):**
- **Deletes** — silver MERGE must delete, or removed records inflate aggregates forever:
  `when_not_matched_by_source_delete()` for `source="snapshot"`, or a tombstone/`is_deleted`
  path (`when_matched_delete`) for incremental; deleted keys' pre-image partitions join the
  affected set. *(Fixed in `upsert`.)*
- **First run** — MERGE / pre-image read / predicate-overwrite all require an existing table;
  create-if-absent on first write. *(Fixed in `upsert`/`replace_groups`.)*
- **Coarser gold GROUP BY (invariant #1)** — read/replace must key on **gold's** group cols
  (a subset of silver's `partition_by`), else you under-read and undercount. *(Fixed via
  `read_groups`/`replace_groups(group_cols=…)`; add a startup assert `group_cols ⊆ partition_by`.)*
- **Multi-hop** — `replace_groups` must also emit an affected marker so the next hop sees
  changed groups. *(Fixed.)*
- **Run-scoped input** — Clean reads `read_new` (this run's slice via `run_ts`), not the whole
  bronze history, or every partition is always "affected". *(Fixed; bronze partitioned by ingest date.)*
- **Empty/missing marker** — `affected()` returns typed-empty when absent; `replace_groups`
  refuses an empty group set (an empty predicate overwrites the WHOLE table). *(Fixed.)*
- **Merge-source uniqueness** — dedup source on key before MERGE (non-unique source errors). *(Fixed.)*
- **Partition-moving update** — relies on MERGE physically relocating a row across partitions;
  **smoke-test** it; fall back to delete-old+insert-new if it doesn't relocate cleanly.
- **Predicate builder** — centralize + unit-test `_predicate_for` for string/date/NULL/quoting;
  assert the written frame's groups ⊆ predicate (delta-rs fails the write otherwise).

**Concurrency & idempotency:**
- `max_active_runs=1` per DAG (serialize writers) + `aws_conditional_put=etag` (safe MinIO
  commits, no lock provider). *(Fixed.)* Optionally wrap MERGE in retry-on-CommitConflict.
- Bronze idempotent per run (delete-WHERE run_ts then append) so KPO/Airflow retries don't
  duplicate; extract must be deterministic per `logical_date`. `_record_affected` overwrites (not appends).

**Ops (add as maintenance DAG / Make targets — acknowledge now, wire soon):**
- **Retention contract** reconciling time-travel with VACUUM: set `deletedFileRetentionDuration`/
  `logRetentionDuration` ≥ the time-travel horizon the verification relies on; `OPTIMIZE.compact()`
  (+ optional z-order on `record_id`) on a cadence; ensure checkpoints so `_delta_log` is pruned.
- **`_runs/<run_ts>/` cleanup** — final task deletes it after all consumers finish; MinIO
  lifecycle rule on `_runs/` as a backstop for failed runs.
- **Observability** — `RunContext` logs rows in/out + affected counts; optional `Step.validate(df)`
  DQ hook (null/dup key checks post-merge); a `_meta/audit` Delta table; enable OpenLineage.
- **Backfill** — distinguish partition-scope (`--full`/`--partitions from..to`, wired via DAG
  `params`→KPO args) from date-range (`airflow backfill` with `catchup=False`); backfill reuses idempotent bronze.

**Framework ergonomics (do up front — cheap now, expensive to retrofit):**
- **Bind manifest↔image**: `export-manifest` stamps the image tag/digest into the manifest;
  KPO reads `image` per-pipeline from YAML; CI guard `git diff --exit-code` on the generated
  manifest proves it matches current Python (prevents DAG/image split-brain).
- **Richer manifest schema now**: optional Step ClassVars (cpu/mem/retries/timeout) + Pipeline
  (tags/owner/default_args/image) → emitted to YAML → consumed by `generate_dags.py`/KPO, so new
  knobs never touch Airflow code.
- **Registration validation** in `register()`/`export-manifest`: `step.output.pipeline == pipeline.id`,
  unique step ids, `upstream ⊆ steps` (or derive `steps` from the upstream closure), fail loud.
- **`make new-pipeline name=…`** token-substituting scaffolder (not raw `cp -r`) so the pipeline
  name is set once; `AggregateStep`/`MapStep` bases own the recompute plumbing.
- **`LocalRunContext`** test double (delta-rs on `file://` temp dirs or in-memory) shipped in
  `_template/tests` so every step is unit-testable with plain Polars frames, no cluster.
- **Per-pipeline `image` field** in the manifest even while one image is used — leaves the door
  open to split images later with zero Airflow-code change.
- `discovery.py` fail-fast in CI, but collect+report all module import errors in `etl list` dev mode.

**Version pins (etl/pyproject.toml + smoke-test at build step 4):** `deltalake` ≥ 1.x (verified
1.6.x current) and a matching `polars`; prefer `schema_mode` over deprecated `overwrite_schema`;
pass Arrow (`df.to_arrow()`) to `write_deltalake`; **MERGE does not auto-evolve schema** — evolve
the table before merging or overwrite-by-group when columns change (document the policy).

**Confirmed correct by the review (no change needed):** Polars `write_delta(mode="merge",
delta_merge_options=…).when_matched_update_all().when_not_matched_insert_all().execute()`,
`mode="append"/"overwrite"`, `delta_write_options={"partition_by":…}`, `read_delta/scan_delta`
`version=` time travel; delta-rs predicate-overwrite (replaceWhere) does **not** require partition
columns in the predicate; chart RBAC (`pod-launcher-role`) already lets workers launch KPO pods
in-namespace; `on_finish_action="delete_pod"`, `in_cluster=True`, `arguments` templating, and
`apiServer`/`dagProcessor` naming are all correct for chart 1.22 / Airflow 3.2.

## Notes / caveats
- **Concurrency:** on MinIO, `aws_conditional_put=etag` gives safe concurrent commits with **no
  lock provider**; `AWS_S3_ALLOW_UNSAFE_RENAME` is *avoided* (it disables the safety check). A
  DynamoDB-style lock is only needed on classic AWS S3 without conditional-put. `max_active_runs=1`
  is still the primary guard.
- Credentials are **test-only** (root creds, plaintext HTTP). Prod path: bucket-scoped MinIO
  policy (not root), TLS, external secret store, etcd encryption-at-rest, file-mounted creds.
- Chart-managed Postgres + single-node MinIO are fine for local testing, not prod HA. MinIO
  community edition is archived — pin a release; note maintained S3 alternatives (SeaweedFS/Garage) if it matters.
- All image/chart/Python versions pinned for reproducibility.
```
