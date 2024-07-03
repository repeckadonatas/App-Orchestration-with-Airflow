FROM apache/airflow:latest-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

USER root

RUN mkdir -p ${AIRFLOW_HOME} && \
    chown -R airflow: ${AIRFLOW_HOME}

USER airflow

COPY ../.. /opt/airflow
RUN ls -la /opt/airflow

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

RUN echo "[core]" > ${AIRFLOW_HOME}/airflow.cfg && \
    echo "airflow_home = ${AIRFLOW_HOME}" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "executor = LocalExecutor" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "[webserver]" > ${AIRFLOW_HOME}/airflow.cfg && \
    echo "base_url = http://localhost:8080" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "web_server_host = 0.0.0.0" >> ${AIRFLOW_HOME}/airflow.cfg && \
    echo "web_server_port = 8080" >> ${AIRFLOW_HOME}/airflow.cfg

ENTRYPOINT ["/entrypoint"]