FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install the etl package (hatchling builds from src/).
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Non-root (uid 50000 matches the KPO securityContext / Airflow convention).
RUN useradd --uid 50000 --create-home --shell /usr/sbin/nologin etl
USER 50000

ENTRYPOINT ["python", "-m", "etl"]
