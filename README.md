# streamlake
End-to-end data pipeline framework built on AWS, integrating **Kafka Connect on MSK**, **Kinesis Firehose**, **S3 Data Lake**, **Athena**, and **micro-batching jobs**.  
Designed for scalable streaming ingestion, cost-efficient storage, and near real-time analytics.

## 🔄 Architecture Workflow

```mermaid
flowchart TD
    subgraph Source
        A[RDS PostgreSQL]
    end

    subgraph KafkaConnect[Kafka Connect]
        B[Debezium CDC Plugin]
    end

    subgraph MSK
        C[MSK - Kafka Topics]
        noteC[(JSON Schemas)]
    end

    subgraph Firehose[Kinesis Firehose]
        D[Dynamic Partitioning + Transform]
    end

    subgraph S3[S3 Data Lake]
        E[bronze/ (raw)]
        F[silver/ (cleansed & deduped)]
        G[gold/ (curated marts)]
    end

    subgraph Jobs
        H[Micro-Batching Jobs (Python)]
        I[Batching Jobs (Python)]
    end

    subgraph Athena
        J[Athena Queries & Views]
    end

    A --> B --> C --> D --> E
    E --> H --> F
    F --> I --> G
    G --> J
