# Streamlake
End-to-end data pipeline framework built on AWS, integrating **Kafka Connect**, **Kafka Registry**,  **[Kafka Broker] MSK**, **Kinesis Firehose**, **S3 Data Lake**, **Athena**, and **micro-batching jobs**.  
Designed for scalable streaming ingestion, cost-efficient storage, and near real-time analytics.

## 🔄 Architecture Workflow

```mermaid
flowchart LR
  %% ===== Left block =====
  DB[RDS PostgreSQL]
  NGINX[Nginx]

  subgraph KAFKA_STACK[Kafka Stack]
    KC1["Kafka Connect 01"]
    KC2["Kafka Connect 02"]
    REG["Kafka Registry"]
    MSK["MSK (broker)"]
  end

  KUI["Kafka UI"]

  %% ===== Middle / Right block =====
  KFH["Kinesis Firehose"]
  S3["S3"]
  EXT["External Table"]
  ATH["Athena"]
  META["Metabase"]
  MB["Micro Batch"]
  ICE["Iceberg Table"]

  %% ===== Flows =====
  DB --> NGINX
  NGINX --> KC1
  NGINX --> KC2
  NGINX --> REG
  KC1 --> MSK
  KC2 --> MSK

  %% Kafka UI points to the whole stack
  KUI --> KAFKA_STACK

  %% Stream to Firehose and S3
  MSK -- "2 seconds" --> KFH
  KFH -- "5 minutes" --> S3

  %% S3 -> Athena via External Table
  S3 --> EXT --> ATH
  ATH --> META

  %% Micro-batch path to Iceberg
  S3 --> MB
  MB -- "15-20 minutes" --> ICE
  ICE --> ATH

  %% Optional style for the stack area
  style KAFKA_STACK fill:#fffbe6,stroke:#888,stroke-width:1px,stroke-dasharray: 5 5
```
