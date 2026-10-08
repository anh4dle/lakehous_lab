# FROM apache/airflow:3.2.2-python3.10

# USER root

# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#         openjdk-17-jre-headless \
#         curl \
#         procps \
#         ca-certificates \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# # Find the actual Java installation regardless of CPU architecture
# RUN JAVA_BIN="$(readlink -f "$(command -v java)")" \
#     && JAVA_DIR="$(dirname "$(dirname "${JAVA_BIN}")")" \
#     && ln -s "${JAVA_DIR}" /opt/java

# ENV JAVA_HOME=/opt/java

# ENV SPARK_VERSION=3.5.1
# ENV HADOOP_VERSION=3

# RUN curl -fSL \
#     "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
#     -o /tmp/spark.tgz \
#     && tar -xzf /tmp/spark.tgz -C /opt \
#     && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark \
#     && rm /tmp/spark.tgz

# ENV SPARK_HOME=/opt/spark
# ENV PATH="${SPARK_HOME}/bin:${PATH}"

# USER airflow

# RUN pip install --no-cache-dir \
#     "apache-airflow==${AIRFLOW_VERSION}" \
#     apache-airflow-providers-apache-spark

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

USER airflow

# Install PySpark provider
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark