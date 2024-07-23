FROM apache/airflow:latest-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

ENV POETRY_VERSION=1.8.2
ENV POETRY_HOME=/usr/local
ENV POETRY_VIRTUALENVS_CREATE=false

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends && \
    apt-get install -y postgresql-client

RUN mkdir -p ${AIRFLOW_HOME} && \
    mkdir -p /opt/airflow/logs && \
    chown -R airflow: ${AIRFLOW_HOME} && \
    chown -R airflow: /opt/airflow/logs

RUN curl -sSL https://install.python-poetry.org | python3 - --version=$POETRY_VERSION

USER airflow

# DEPENDENCIES
COPY ../../poetry.lock pyproject.toml /opt/airflow/

RUN poetry install --no-root

# COPY DAGs AND SRC FILES
COPY dags /opt/airflow/dags/
COPY ../.. /opt/airflow/src/
RUN ls -la /opt/airflow

#RUN echo "[core]" > ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "airflow_home = ${AIRFLOW_HOME}" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "executor = LocalExecutor" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "[webserver]" > ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "base_url = http://localhost:8080" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "web_server_host = 0.0.0.0" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "web_server_port = 8080" >> ${AIRFLOW_HOME}/airflow.cfg && \
#    echo "secret_key = super_secret_key" >> ${AIRFLOW_HOME}/airflow.cfg

ENTRYPOINT ["/entrypoint"]