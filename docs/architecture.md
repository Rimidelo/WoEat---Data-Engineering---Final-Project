# WoEat Data Platform – Architecture Overview

The following Mermaid diagram illustrates the complete flow of data—from event generation through ingestion, processing layers, orchestration, and finally analytics & monitoring.

```mermaid
%%{init: {"flowchart": {"layout": "dagre", "useMaxWidth": true}} }%%
flowchart TD
  subgraph "Source Data"
    A1["Batch generator<br/>(generate_5000_orders.py)"]
    A2["Kafka topics<br/>orders-topic<br/>order-items-topic"]
  end

  subgraph Ingestion
    B1["Spark job<br/>create_supporting_data.py"]
    B2["Spark streaming<br/>kafka_stream_ingestion.py"]
  end

  subgraph "Bronze Layer"
    C1["bronze.bronze_orders"]
    C2["bronze.bronze_order_items"]
    C3["bronze_* reference tables"]
  end

  subgraph "Silver Layer"
    D1["silver.silver_orders"]
    D2["silver.silver_order_items"]
    D3["silver_* reference tables"]
  end

  subgraph "Gold Layer"
    E1["gold.fact_orders"]
    E2["gold.dim_restaurant"]
    E3["gold.dim_driver"]
    E4["gold.dim_menu_item"]
    E5["gold.dim_date"]
  end

  subgraph Orchestration
    F1["Airflow DAG<br/>woeat_etl_pipeline"]
  end

  subgraph "BI & Monitoring"
    G1["HTML Dashboard<br/>(woeat_dashboard.html)"]
    G2["Data quality check<br/>verify_orders.py"]
  end

  A1 --> C1 & C2
  A2 --> B2
  B2 --> C1 & C2
  B1 --> C3
  C1 --> D1
  C2 --> D2
  C3 --> D3
  D1 --> E1
  D3 --> E2 & E3
  D2 --> E1
  E1 --> G1 & G2
  F1 -.-> B1 & B2 & D1 & E1
```

*Airflow orchestrates every stage (dotted arrows).* Facts and dimensions in the Gold layer feed the dashboard and verification scripts that monitor data quality and key business metrics. 