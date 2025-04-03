FROM apache/airflow:2.10.5

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Remove the COPY dags line as we're mounting the dags directory in docker-compose
# COPY dags/ $AIRFLOW_HOME/dags/

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow