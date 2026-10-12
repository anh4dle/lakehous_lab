# # Stage 1: Pull official Spark image
# FROM apache/spark:3.5.4-python3 AS spark-base

# # Stage 2: Build Airflow Image
# FROM apache/airflow:3.2.2-python3.10

# USER root

# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#         openjdk-17-jre-headless \
#         procps \
#         ca-certificates \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# # Set JAVA_HOME dynamically
# RUN JAVA_BIN="$(readlink -f "$(command -v java)")" \
#     && JAVA_DIR="$(dirname "$(dirname "${JAVA_BIN}")")" \
#     && ln -s "${JAVA_DIR}" /opt/java

# ENV JAVA_HOME=/opt/java

# # Copy compiled Spark distribution directly from Stage 1 (NO CURL NEEDED)
# COPY --from=spark-base /opt/spark /opt/spark

# ENV SPARK_HOME=/opt/spark
# ENV PATH="${SPARK_HOME}/bin:${PATH}"

# USER airflow

# # Install PySpark provider
# RUN pip install --no-cache-dir apache-airflow-providers-apache-spark "minio>=7.2.20" pyarrow delta-spark==3.2.0 pandera pydantic

# Stage 1: Pull official Spark image
FROM apache/spark:3.5.4-python3 AS spark-base

# Stage 2: Build Airflow Image
FROM apache/airflow:3.2.2-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME dynamically
RUN JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && JAVA_DIR="$(dirname "$(dirname "${JAVA_BIN}")")" \
    && ln -s "${JAVA_DIR}" /opt/java

ENV JAVA_HOME=/opt/java

# Copy compiled Spark distribution directly from Stage 1 (NO CURL NEEDED)
COPY --from=spark-base /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# --- Added Requirements Setup ---
# Copy requirements.txt with proper ownership for the airflow user
COPY --chown=airflow:root requirements.txt /requirements.txt

USER airflow

# Upgrade pip and install all dependencies from the requirements file
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir apache-airflow-providers-apache-spark -r /requirements.txt
