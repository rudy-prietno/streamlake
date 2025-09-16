# Streamlake
End-to-end data pipeline framework built on AWS, integrating **Kafka Connect**, **Kafka Registry**,  **[Kafka Broker] MSK**, **Kinesis Firehose**, **S3 Data Lake**, **Athena**, and **micro-batching jobs**.  
Designed for scalable streaming ingestion, cost-efficient storage, and near real-time analytics.

## 🔄 Architecture Workflow

```mermaid
flowchart LR
  %% ===== Left block =====
  DB[DB Prod]
  Nginx[Nginx]

  subgraph Kafka_Stack[Kafka Stack]
    KC1[Kafka Connect [01]]
    KC2[Kafka Connect [02]]
    REG[Kafka Registry]
    MSK[MSK (broker)]
  end

  KUI[Kafka UI]

  %% ===== Middle / Right block =====
  KFH[Kinesis Firehose]
  S3[S3]
  EXT[External Table]
  ATH[DB Athena]
  META[Metabase]
  MB[Micro Batch]
  ICE[Iceberg Table]

  %% ===== Flows =====
  DB --> Nginx
  Nginx --> KC1
  Nginx --> KC2
  Nginx --> REG
  KC1 --> MSK
  KC2 --> MSK

  %% Kafka UI access
  KUI --> KC1
  KUI --> KC2
  KUI --> REG

  %% Stream to Firehose (latency note)
  MSK -- 2 seconds --> KFH

  %% Firehose to S3 (buffer/flush)
  KFH -- 5 minutes --> S3

  %% S3 to Athena via External Table
  S3 --> EXT --> ATH

  %% Metabase queries Athena
  META --> ATH

  %% Micro-batch path to Iceberg
  S3 --> MB
  MB -- 15-20 minutes --> ICE
  ICE --> ATH

```
