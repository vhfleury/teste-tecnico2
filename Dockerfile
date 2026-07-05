ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Delta Lake jars are baked into pyspark's jars/ at build time so gold-layer
# sessions never download dependencies at runtime (no Ivy resolution per task).
ARG DELTA_VERSION=3.3.2
ARG DELTA_MAVEN_URL=https://repo1.maven.org/maven2/io/delta

RUN PYSPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fSL "${DELTA_MAVEN_URL}/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
        -o "${PYSPARK_JARS}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    && curl -fSL "${DELTA_MAVEN_URL}/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
        -o "${PYSPARK_JARS}/delta-storage-${DELTA_VERSION}.jar"

# Apache Sedona jars (geospatial analytics layer) are baked in the same way:
# the shaded Spark bundle plus the geotools wrapper required by ST_* functions.
ARG SEDONA_VERSION=1.9.0
ARG GEOTOOLS_WRAPPER_VERSION=1.9.0-33.5
ARG MAVEN_CENTRAL_URL=https://repo1.maven.org/maven2

RUN PYSPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fSL "${MAVEN_CENTRAL_URL}/org/apache/sedona/sedona-spark-shaded-3.5_2.12/${SEDONA_VERSION}/sedona-spark-shaded-3.5_2.12-${SEDONA_VERSION}.jar" \
        -o "${PYSPARK_JARS}/sedona-spark-shaded-3.5_2.12-${SEDONA_VERSION}.jar" \
    && curl -fSL "${MAVEN_CENTRAL_URL}/org/datasyslab/geotools-wrapper/${GEOTOOLS_WRAPPER_VERSION}/geotools-wrapper-${GEOTOOLS_WRAPPER_VERSION}.jar" \
        -o "${PYSPARK_JARS}/geotools-wrapper-${GEOTOOLS_WRAPPER_VERSION}.jar"
