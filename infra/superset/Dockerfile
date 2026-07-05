# Superset + the Postgres driver so it can query the serving-Postgres data source.
# (psycopg2-binary has no SQLAlchemy version constraint, so Superset stays on its pinned 1.4.)
FROM apache/superset:4.1.4

USER root
RUN pip install --no-cache-dir psycopg2-binary
USER superset
