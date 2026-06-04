# =============================================================================
# kafka-connect/Dockerfile
#
# Base  : confluentinc/cp-kafka-connect:7.6.0
# Adds  : confluent S3 sink connector (writes Kafka topics → MinIO as Parquet)
#
# The S3 sink connector is installed at build time using confluent-hub.
# This avoids the slow startup install pattern and makes the image reproducible.
# =============================================================================

FROM confluentinc/cp-kafka-connect:7.6.0

# Install the S3 sink connector (supports MinIO via path-style S3A endpoint)
RUN confluent-hub install --no-prompt \
    confluentinc/kafka-connect-s3:10.5.7

# The connector plugin lands in /usr/share/confluent-hub-components
# which is already on CONNECT_PLUGIN_PATH in docker-compose.yml

# No USER change needed — cp-kafka-connect runs as appuser already
