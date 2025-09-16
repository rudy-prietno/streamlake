#!/bin/bash
set -e

REGION_NAME="ap-southeast-3"

echo "[INFO] Fetching bootstrap_servers from AmazonMSK_/prod/mskbootstrap..."
BOOTSTRAP_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "AmazonMSK_/prod/mskbootstrap" \
  --region "$REGION_NAME" \
  --query SecretString \
  --output text)

BOOTSTRAP_SERVERS=$(echo "$BOOTSTRAP_SECRET" | jq -r '.bootstrap_servers')
echo "[INFO] Bootstrap servers: $BOOTSTRAP_SERVERS"

echo "[INFO] Fetching schema-user credentials from AmazonMSK_/prod/dataengineer..."
JAAS_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "AmazonMSK_/prod/dataengineer" \
  --region "$REGION_NAME" \
  --query SecretString \
  --output text)

USERNAME=$(echo "$JAAS_SECRET" | jq -r '.username')
PASSWORD=$(echo "$JAAS_SECRET" | jq -r '.password')

SCHEMA_JAAS="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${USERNAME}\" password=\"${PASSWORD}\";"

# Export environment variables so they can be used by Schema Registry
export SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="$BOOTSTRAP_SERVERS"
export SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG="$SCHEMA_JAAS"

echo "[INFO] SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG constructed successfully."

# Start the Schema Registry
exec "$@"
