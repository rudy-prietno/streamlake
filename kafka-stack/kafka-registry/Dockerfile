FROM confluentinc/cp-schema-registry:7.2.15

USER root

# Install dependencies using yum
RUN yum update -y && \
    yum install -y curl unzip jq && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    yum clean all && \
    rm -rf /var/cache/yum /tmp/*

USER appuser
