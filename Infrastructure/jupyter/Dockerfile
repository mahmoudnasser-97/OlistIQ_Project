# =============================================================================
# jupyter/Dockerfile
#
# Base  : jupyter/pyspark-notebook:spark-3.5.0
#         Ships: Python 3.11 · PySpark 3.5.0 · OpenJDK 17 · JupyterLab 4
# Adds  : Delta Lake, S3A/MinIO, PostgreSQL JARs + all project Python deps
#         spark-defaults.conf pre-wired to the cluster in docker-compose
#
# Access: http://localhost:8888  (token: olist)
# =============================================================================

FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# -----------------------------------------------------------------------------
# 1.  OS packages
# -----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        wget \
        netcat-openbsd \
        postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# 2.  Python packages
# -----------------------------------------------------------------------------
RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    kafka-python==2.0.2 \
    boto3==1.34.0 \
    s3fs==2023.12.0 \
    psycopg2-binary==2.9.9 \
    SQLAlchemy==2.0.23 \
    pandas==2.0.3 \
    pyarrow==14.0.2 \
    matplotlib==3.8.2 \
    seaborn==0.13.0 \
    plotly==5.17.0 \
    redis==5.0.1 \
    great-expectations==0.18.8 \
    jupyterlab-git==0.50.0 \
    ipywidgets==8.1.1

# -----------------------------------------------------------------------------
# 3.  Spark JARs (same set as the Spark image — must stay in sync)
# -----------------------------------------------------------------------------
ENV DELTA_VERSION=3.0.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.540
ENV PG_JDBC_VERSION=42.6.0
ENV KAFKA_SPARK_VERSION=3.5.0
ENV SCALA_VERSION=2.12
ENV MAVEN=https://repo1.maven.org/maven2

RUN JARS=$(python3 -c \
      "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
    && wget -q -P "$JARS" \
      "${MAVEN}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
    && wget -q -P "$JARS" \
      "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && wget -q -P "$JARS" \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && wget -q -P "$JARS" \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && wget -q -P "$JARS" \
      "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar" \
    && wget -q -P "$JARS" \
      "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar" \
    && echo "JARs installed:" \
    && ls "$JARS" | grep -E "delta|hadoop-aws|aws-java|postgresql|kafka"

# -----------------------------------------------------------------------------
# 4.  spark-defaults.conf — connects to the cluster, pre-sets S3A and Delta
# -----------------------------------------------------------------------------
RUN CONF=$(python3 -c \
      "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__),'conf'))") \
    && mkdir -p "$CONF" \
    && cat > "$CONF/spark-defaults.conf" << 'SPARKEOF'
spark.master                                    spark://spark-master:7077

spark.sql.extensions                            io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog                 org.apache.spark.sql.delta.catalog.DeltaCatalog

spark.hadoop.fs.s3a.endpoint                    http://minio:9000
spark.hadoop.fs.s3a.access.key                  minioadmin
spark.hadoop.fs.s3a.secret.key                  minioadmin
spark.hadoop.fs.s3a.path.style.access           true
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider    org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.multipart.size              104857600
spark.hadoop.fs.s3a.fast.upload                 true

spark.sql.shuffle.partitions                    8
spark.default.parallelism                       8
spark.driver.memory                             1g
spark.executor.memory                           1500m
spark.databricks.delta.schema.autoMerge.enabled true
SPARKEOF

# -----------------------------------------------------------------------------
# 5.  JupyterLab config
# -----------------------------------------------------------------------------
RUN mkdir -p /home/jovyan/.jupyter \
    && cat > /home/jovyan/.jupyter/jupyter_lab_config.py << 'JLABEOF'
c.ServerApp.token        = 'olist'
c.ServerApp.password     = ''
c.ServerApp.open_browser = False
c.ServerApp.ip           = '0.0.0.0'
c.ServerApp.port         = 8888
c.ServerApp.root_dir     = '/home/jovyan/work'
c.ServerApp.allow_origin = '*'
JLABEOF

# -----------------------------------------------------------------------------
# 6.  Directories and helper files
# -----------------------------------------------------------------------------
RUN mkdir -p /home/jovyan/work \
             /home/jovyan/data \
             /home/jovyan/spark_jobs \
    && chown -R jovyan:users \
        /home/jovyan/work \
        /home/jovyan/data \
        /home/jovyan/spark_jobs

COPY spark_session.py /home/jovyan/work/spark_session.py

USER jovyan

EXPOSE 8888
CMD ["start-notebook.sh", "--NotebookApp.token=olist"]
