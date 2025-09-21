# MSK Clusters
```mermaid
flowchart TB
    subgraph KRaftControllers [KRaft Controllers]
        C1(["Controller 1"])
        C2(["Controller 2 - L"])
        C3(["Controller 3"])
    end

    subgraph Brokers [Brokers]
        B1(["Broker 1"])
        B2(["Broker 2"])
        B3(["Broker 3"])
        B4(["Broker 4"])
    end

    C1 --- C2 --- C3
    C2 --> B1
    C2 --> B2
    C2 --> B3
    C2 --> B4

    classDef leader fill=#00BFFF,stroke=#333,stroke-width=2px,color=white;
    class C2 leader;
```
