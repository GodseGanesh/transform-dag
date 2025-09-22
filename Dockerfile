
# Use the official Airflow image as the base
FROM apache/airflow:2.9.3-python3.9

# Set working directory
WORKDIR /opt/airflow

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
RUN mkdir -p /opt/airflow/logs/isin_profile_transform && chown -R airflow: /opt/airflow/logs
USER airflow

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire transform_dags directory and .env file
COPY transform_dags/ dags/
COPY .env .

# Set environment variables to load .env file
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/dags/utils:/opt/airflow/dags/scripts:/opt/airflow/dags/config:/opt/airflow/dags/mappings
