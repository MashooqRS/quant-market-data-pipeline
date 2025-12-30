FROM apache/airflow:2.8.1-python3.10

USER root

# Java for Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install python deps (pyspark etc.)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
