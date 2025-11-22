FROM apache/airflow:3.1.3

USER root

# ------------------------------------------------------------------------------
# 1. System Dependencies & MS SQL Drivers
# ------------------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gnupg2 curl unixodbc-dev ca-certificates && \
    # Download and de-armor the Microsoft signing key
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    # Add the repository
    curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    # Install the driver and tools (ACCEPT_EULA is required)
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18 && \
    # Cleanup to keep image size down
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch to airflow user for Python package installation
USER airflow

# ------------------------------------------------------------------------------
# 2. Python Dependencies
# ------------------------------------------------------------------------------
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# Install pip packages with constraints matching the current Python version
# We dynamically grab the python version to prevent constraint mismatch errors
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.3/constraints-$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2).txt" \
    -r /tmp/requirements.txt

# ------------------------------------------------------------------------------
# 3. Project Files & Configuration
# ------------------------------------------------------------------------------
# Copy your DAGs, Scripts, and SQL
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./python_scripts /opt/airflow/python_scripts
COPY --chown=airflow:root ./sql /opt/airflow/sql

# CRITICAL: Add /opt/airflow to PYTHONPATH. 
# This allows code in 'dags' to do: importing of python modules"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Final user switch
USER airflow