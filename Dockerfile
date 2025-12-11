# syntax=docker/dockerfile:1
ARG AIRFLOW_IMAGE=apache/airflow:2.10.2-python3.12
FROM ${AIRFLOW_IMAGE}

# build as root so we can install system deps and cleanup in one layer
USER root

WORKDIR /opt/airflow

# copy requirements early so this layer is cacheable when requirements.txt doesn't change
COPY requirements.txt /tmp/requirements.txt

# Use BuildKit cache for pip; install minimal build deps only for this layer,
# upgrade pip/setuptools/wheel, prefer binary wheels, then purge build deps.
# Note: DOCKER_BUILDKIT=1 must be set when building to enable --mount=type=cache.
RUN --mount=type=cache,target=/root/.cache/pip \
    apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential git gcc gfortran libatlas-base-dev libopenblas-dev \
 && python -m pip install --upgrade pip setuptools wheel \
 && python -m pip install --prefer-binary --no-cache-dir -r /tmp/requirements.txt \
 && apt-get purge -y --auto-remove build-essential gfortran gcc \
 && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

 # -------------------------------------------------------
# Add this block to handle Base64-decoded credentials
# -------------------------------------------------------

# Create directory for credentials
RUN mkdir -p /opt/airflow/secrets \
    && chown airflow:airflow /opt/airflow/secrets

# Accept the Base64 secret at build time
ARG GCP_CREDENTIAL_BASE64=""

# Decode the Base64 into credential.json
# Note the use of bash -lc so the variable resolves
RUN bash -lc 'if [ -n "$GCP_CREDENTIAL_BASE64" ]; then \
      echo "$GCP_CREDENTIAL_BASE64" | base64 -d > /opt/airflow/secrets/credential.json; \
    fi' \
    && chown airflow:airflow /opt/airflow/secrets/credential.json

# switch back to airflow user (as original image expects)
USER airflow
