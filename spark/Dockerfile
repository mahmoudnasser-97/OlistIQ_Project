# =============================================================================
# spark/Dockerfile
#
# Base  : apache/spark:3.5.1-python3   (official Apache image — bitnami/spark
#         was removed from Docker Hub free tier in September 2025)
#
# Key differences from bitnami/spark:
#   SPARK_HOME  = /opt/spark            (not /opt/bitnami/spark)
#   default user= spark (uid 185)       (not 1001)
#   no SPARK_MODE env-var entrypoint    → we set CMD explicitly in compose
#   Python 3    included in this tag
#
# Used by: spark-master, spark-worker, spark-streaming in docker-compose.yml
# =============================================================================

FROM apache/spark:3.5.1-python3

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONUNBUFFERED=1

USER root

# -----------------------------------------------------------------------------
# 1.  OS packages
# -----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        wget \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# 2.  Python packages
# -----------------------------------------------------------------------------
RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    kafka-python==2.0.2 \
    redis==5.0.1 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.0.3 \
    pyarrow==14.0.2

# -----------------------------------------------------------------------------
# 3.  Spark JARs
#
#     JAR                                   Purpose
#     ─────────────────────────────────────────────────────────────────────
#     delta-spark_2.12-3.0.0.jar           Delta Lake read/write engine
#     delta-storage-3.0.0.jar              Delta storage abstraction
#     hadoop-aws-3.3.4.jar                 S3A → MinIO connector
#     aws-java-sdk-bundle-1.12.540.jar     AWS credentials + S3 client
#     postgresql-42.6.0.jar               JDBC driver (Gold → PostgreSQL)
#     spark-sql-kafka-0-10_2.12-3.5.1.jar Kafka source for Structured Streaming
#     kafka-clients-3.4.0.jar             Kafka client (Streaming dependency)
#     spark-token-provider-kafka…jar      Token provider (Streaming dependency)
# -----------------------------------------------------------------------------
ENV DELTA_VERSION=3.0.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.540
ENV PG_JDBC_VERSION=42.6.0
ENV KAFKA_SPARK_VERSION=3.5.1
ENV SCALA_VERSION=2.12
ENV MAVEN=https://repo1.maven.org/maven2
ENV JARS=${SPARK_HOME}/jars

RUN wget -q -P ${JARS} \
      "${MAVEN}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
    && wget -q -P ${JARS} \
      "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && wget -q -P ${JARS} \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && wget -q -P ${JARS} \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && wget -q -P ${JARS} \
      "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar" \
    && wget -q -P ${JARS} \
      "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar" \
    && wget -q -P ${JARS} \
      "${MAVEN}/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar" \
    && wget -q -P ${JARS} \
      "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${KAFKA_SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${KAFKA_SPARK_VERSION}.jar" \
    && echo "✓ JARs:" && ls ${JARS} | grep -E "delta|hadoop-aws|aws-java|postgresql|kafka"

# -----------------------------------------------------------------------------
# 4.  spark-defaults.conf
# -----------------------------------------------------------------------------
RUN mkdir -p ${SPARK_HOME}/conf \
    && cat > ${SPARK_HOME}/conf/spark-defaults.conf << 'EOF'
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
spark.hadoop.fs.s3a.fast.upload.buffer          bytebuffer

spark.sql.shuffle.partitions                    8
spark.default.parallelism                       8
spark.databricks.delta.schema.autoMerge.enabled true
EOF

# -----------------------------------------------------------------------------
# 5.  Work directory and permissions
# -----------------------------------------------------------------------------
RUN mkdir -p /opt/spark_jobs /opt/data /opt/spark/logs \
    && chown -R spark:spark /opt/spark/logs /opt/spark_jobs /opt/data

USER spark
