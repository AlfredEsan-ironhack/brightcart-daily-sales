FROM apache/airflow:3.3.2

# pandas pinned to the version Airflow 3.3.2 is tested with (same as your venv).
# Installing apache-airflow at its own version alongside stops pip from changing Airflow.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "pandas==3.0.5"