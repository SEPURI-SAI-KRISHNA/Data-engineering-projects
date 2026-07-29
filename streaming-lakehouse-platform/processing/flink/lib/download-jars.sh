#!/usr/bin/env bash
# Fetches the Flink connector jars needed by the streaming job.
# They are too big to keep in git, so run this once after cloning.
set -e

cd "$(dirname "$0")"

MAVEN=https://repo1.maven.org/maven2

jars=(
  "$MAVEN/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
  "$MAVEN/org/apache/flink/flink-sql-connector-kafka/3.0.0-1.17/flink-sql-connector-kafka-3.0.0-1.17.jar"
  "$MAVEN/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar"
  "$MAVEN/org/apache/iceberg/iceberg-flink-runtime-1.17/1.4.3/iceberg-flink-runtime-1.17-1.4.3.jar"
)

for url in "${jars[@]}"; do
  file=$(basename "$url")
  if [ -f "$file" ]; then
    echo "already have $file"
  else
    echo "downloading $file"
    curl -fLO "$url"
  fi
done
