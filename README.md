# streamlake
End-to-end data pipeline framework built on AWS, integrating **Kafka Connect on MSK**, **Kinesis Firehose**, **S3 Data Lake**, **Athena**, and **micro-batching jobs**.  
Designed for scalable streaming ingestion, cost-efficient storage, and near real-time analytics.

## 🔄 Architecture Workflow

flowchart TD
  %% Source
  subgraph Source
    A[RDS PostgreSQL]
  end

  %% Kafka Connect
  subgraph Kafka_Connect[Kafka Connect]
    B[CDC via Kafka Connect Plugin Debezium]
  end

  %% MSK
  subgraph MSK
    C[MSK - Kafka Topics]
    Cnote[[JSON Schemas]]
  end

  %% Firehose
  subgraph Firehose[Kinesis Firehose]
    D[Dynamic Partitioning + Transform]
  end

  %% S3 Data Lake
  subgraph S3[S3 Data Lake]
    E[bronze/ (raw)]
    F[silver/ (cleansed & deduped)]
    G[gold/ (curated marts)]
  end

  %% Jobs
  subgraph Jobs
    H[Micro-Batching Jobs (Python)]
    I[Batching Jobs (Python)]
  end

  %% Athena
  subgraph Athena
    J[Athena]
  end

  A --> B --> C --> D --> E
  C --> Cnote
  E --> H --> F
  F --> I --> G
  G --> J

