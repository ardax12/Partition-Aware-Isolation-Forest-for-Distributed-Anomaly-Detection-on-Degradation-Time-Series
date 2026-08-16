# Spark cluster image matching the paper's stack: Java 21 + Python 3.12 + Spark 4.1.2.
# eclipse-temurin:21-jre-noble is Ubuntu 24.04 based, which ships Python 3.12.
FROM eclipse-temurin:21-jre-noble

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip procps curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

# Spark ships inside the pip 'pyspark' package; expose it as SPARK_HOME.
RUN ln -s "$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
ENV PYSPARK_PYTHON=python3

WORKDIR /work
