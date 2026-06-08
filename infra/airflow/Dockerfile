FROM apache/airflow:2.8.1-python3.10

USER root

# Install dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install python dependencies
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-docker
