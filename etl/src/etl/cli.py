"""`etl` CLI — entrypoint for the KPO pods and local dev.

    etl run <pipeline> <step> --run-ts ...   # one step, one pod
    etl export-manifest --image ... --out ...
    etl list
    etl new-pipeline <name>
"""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(add_completion=False, help="Polars/Delta ETL framework CLI")


@app.command()
def run(
    pipeline: str = typer.Argument(...),
    step: str = typer.Argument(...),
    run_id: str = typer.Option("manual", "--run-id"),
    run_ts: str = typer.Option(..., "--run-ts", help="unique per DAG run, e.g. {{ ts_nodash }}"),
    logical_date: str = typer.Option("", "--logical-date"),
) -> None:
    from .framework.context import RunContext
    from .framework.runner import run_step

    ctx = RunContext.from_env(run_id, run_ts, logical_date)
    run_step(pipeline, step, ctx)


@app.command("export-manifest")
def export_manifest_cmd(
    image: str = typer.Option(..., "--image"),
    out: str = typer.Option(..., "--out"),
) -> None:
    from .framework.manifest import export_manifest

    path = export_manifest(image, out)
    typer.echo(f"wrote {path}")


@app.command("list")
def list_pipelines() -> None:
    from .framework.discovery import load_all
    from .framework.pipeline import REGISTRY

    load_all()
    for pid in sorted(REGISTRY):
        p = REGISTRY[pid]
        typer.echo(f"{pid} ({p.schedule}): {[s.id for s in p.steps]}")


@app.command("new-pipeline")
def new_pipeline(name: str = typer.Argument(...)) -> None:
    from .framework.scaffold import scaffold

    pipelines_dir = Path(__file__).resolve().parent / "pipelines"
    dest = scaffold(name, pipelines_dir)
    typer.echo(f"scaffolded {dest}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
