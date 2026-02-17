# elevata – Airflow example image
# Copyright (c) 2026 elevata Ilona Tag
# Licensed under the AGPL-3.0 License.
#
# This Dockerfile is part of the optional Airflow example and is not required
# to run elevata itself.


FROM apache/airflow:3.1.7

USER root

# If you later need OS libs for specific DB drivers, add them here.
# Keep this minimal for the example.
# RUN apt-get update && apt-get install -y --no-install-recommends \
#   build-essential \
#   && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# ---- elevata dependency selection (defaults to base + duckdb) ----
ARG ELEVATA_REQ_BASE=requirements/base.txt
ARG ELEVATA_REQ_BACKEND=requirements/duckdb.txt

# Copy only the requirements folder (fast rebuilds).
COPY requirements/ /tmp/requirements/

RUN pip install --no-cache-dir -U pip && \
  pip install --no-cache-dir \
    -r /tmp/requirements/$(basename ${ELEVATA_REQ_BASE}) \
    -r /tmp/requirements/$(basename ${ELEVATA_REQ_BACKEND})
