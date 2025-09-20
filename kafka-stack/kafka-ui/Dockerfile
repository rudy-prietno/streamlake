
FROM provectuslabs/kafka-ui:latest

USER root

# Install dependencies (Python 3 + pip required for AWS CLI v2 in Alpine)
RUN apk add --no-cache \
    curl \
    unzip \
    jq \
    python3 \
    py3-pip \
    groff \
    less \
    bash

# Install AWS CLI v1
RUN apk add --no-cache jq curl python3 py3-pip && \
    pip install awscli

RUN rm -f /app/lib/commons-logging-*.jar
