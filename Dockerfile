# Start from the official Airflow image
FROM apache/airflow:2.10.2

# Switch to root to install system packages
USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential git gcc \
  && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
  && rm /tmp/requirements.txt