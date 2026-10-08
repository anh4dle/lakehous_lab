FROM apache/spark:3.5.4-scala2.12-java17-python3-ubuntu
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && pip3 install --no-cache-dir pandas pyarrow delta-spark==3.2.0 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
USER spark