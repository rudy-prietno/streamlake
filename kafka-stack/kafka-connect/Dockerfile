FROM confluentinc/cp-kafka-connect:7.6.0

USER root

# Install Java 11 (with JDK), tools, and AWS CLI
RUN yum update -y && \
    yum install -y curl unzip jq java-11-openjdk-devel && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    yum clean all && \
    rm -rf /var/cache/yum /tmp/*

# Copy plugin ZIP (make sure it's in Docker build context)
COPY confluentinc-csid-secrets-provider-aws-1.0.44.zip /tmp/

# Extract plugin
RUN unzip /tmp/confluentinc-csid-secrets-provider-aws-1.0.44.zip -d /usr/share/confluent-hub-components/ && \
    rm /tmp/confluentinc-csid-secrets-provider-aws-1.0.44.zip

# Debug: List all files to find the correct JAR path
RUN find /usr/share/confluent-hub-components/ -type f

# Run JAR class check after plugin has been confirmed
RUN jar tf /usr/share/confluent-hub-components/confluentinc-csid-secrets-provider-aws-1.0.44/lib/csid-secrets-provider-aws-1.0.44.jar | grep 'SecretsManagerConfigProvider.class' || (echo "Class not found!" && exit 1)

# Set plugin path
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/etc/kafka-connect/jars,/usr/share/confluent-hub-components/confluentinc-csid-secrets-provider-aws-1.0.44"

USER appuser
