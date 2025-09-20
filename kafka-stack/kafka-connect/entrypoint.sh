#!/usr/bin/env bash
set -euo pipefail

REGION_NAME="${REGION_NAME:-ap-southeast-3}"
PROVIDER_DIR="/usr/share/confluent-hub-components/confluentinc-csid-secrets-provider-aws-1.0.44/lib"

# 0) Provider jars visible to JVMs
export KAFKA_CLASSPATH="${PROVIDER_DIR}/*:/etc/kafka-connect/jars/*${KAFKA_CLASSPATH:+:$KAFKA_CLASSPATH}"

# 1) Fetch endpoints (MSK + SR)
echo "[INFO] Fetching MSK bootstrap & Schema Registry from Secrets Manager ..."
BOOTSTRAP_SECRET="$(aws secretsmanager get-secret-value \
  --secret-id 'AmazonMSK_/prod/mskbootstrap' \
  --region "$REGION_NAME" --query SecretString --output text)"

BOOTSTRAP_SERVERS="$(jq -r '.bootstrap_servers // empty' <<<"$BOOTSTRAP_SECRET")"
SCHEMA_REGISTRY="$(jq -r '.schema_registry // empty'    <<<"$BOOTSTRAP_SECRET")"
echo "[INFO] BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-<empty>}"
echo "[INFO] SCHEMA_REGISTRY=${SCHEMA_REGISTRY:-<empty>}"

# 2) Fetch SCRAM creds
echo "[INFO] Fetching Connect SCRAM credentials ..."
JAAS_SECRET="$(aws secretsmanager get-secret-value \
  --secret-id 'AmazonMSK_/prod/dataengineer' \
  --region "$REGION_NAME" --query SecretString --output text)"
USERNAME="$(jq -r '.username // empty' <<<"$JAAS_SECRET")"
PASSWORD="$(jq -r '.password // empty' <<<"$JAAS_SECRET")"

# 3) JAAS for producers/consumers
mkdir -p /etc/kafka
cat >/etc/kafka/kafka_client_jaas.conf <<EOF
KafkaClient {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="${USERNAME}"
  password="${PASSWORD}";
};
EOF
export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/kafka_client_jaas.conf"

# 4) (optional) quick TCP probe — non-fatal
if [[ -n "${BOOTSTRAP_SERVERS:-}" ]]; then
  echo "[INFO] Probing broker TCP reachability ..."
  IFS=',' read -r -a bs_arr <<<"$BOOTSTRAP_SERVERS"
  for hp in "${bs_arr[@]}"; do
    host="${hp%%:*}"; port="${hp##*:}"
    if [[ -n "$host" && "$host" != "$port" && "$port" =~ ^[0-9]+$ ]]; then
      if (exec 3<>/dev/tcp/"$host"/"$port") 2>/dev/null; then
        echo "[INFO] TCP OK: $host:$port"; exec 3<&- 3>&-; break
      else
        echo "[WARN] TCP fail: $host:$port"
      fi
    fi
  done
fi

# 5) Build worker properties (bypass kafka-ready)
cat >/tmp/worker.properties <<EOF
# ---- Core ----
bootstrap.servers=${BOOTSTRAP_SERVERS}
group.id=connect-cluster

# REST
rest.port=8083
rest.advertised.host.name=kafka-connect
rest.advertised.port=8083

# Storage topics (must be already or enable auto-create by Connect based on policy cluster)
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status

# Converters (Avro) + Schema Registry
key.converter=io.confluent.connect.avro.AvroConverter
value.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=${SCHEMA_REGISTRY}
value.converter.schema.registry.url=${SCHEMA_REGISTRY}

# Internal converters (tanpa schema)
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

# Security (SCRAM) untuk worker, producer, consumer
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${USERNAME}" password="${PASSWORD}";

producer.security.protocol=SASL_SSL
producer.sasl.mechanism=SCRAM-SHA-512
producer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${USERNAME}" password="${PASSWORD}";

consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=SCRAM-SHA-512
consumer.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${USERNAME}" password="${PASSWORD}";

# Plugin dirs (scan)
plugin.path=/usr/share/java,/etc/kafka-connect/jars,/usr/share/confluent-hub-components/confluentinc-csid-secrets-provider-aws-1.0.44

# ---- AWS Secrets Manager ConfigProvider ----
config.providers=secrets
config.providers.secrets.class=io.confluent.csid.config.provider.aws.SecretsManagerConfigProvider
config.providers.secrets.param.use.json=true
config.providers.secrets.param.aws.region=${REGION_NAME}
config.providers.secrets.param.polling.interval.seconds=300
EOF

echo "[INFO] Worker properties written to /tmp/worker.properties"
echo "[INFO] Starting Kafka Connect (connect-distributed /tmp/worker.properties) ..."
exec /usr/bin/connect-distributed /tmp/worker.properties
