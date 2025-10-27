# Start from the official Airflow image
FROM apache/airflow:2.10.2

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential git gcc \
  && rm -rf /var/lib/apt/lists/*
  
RUN pip install --no-cache-dir "apache-airflow-providers-celery>=3.3.0"