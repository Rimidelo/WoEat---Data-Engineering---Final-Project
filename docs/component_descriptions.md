# Component Descriptions

---

## 1. Orchestration Component `(/orchestration)`

* **Purpose** – Manages scheduling and coordination of batch and streaming ETL workflows.
* **Technology** – Apache Airflow 2.7.1
* **Key Features**
  * Batch ETL orchestration via **`woeat_etl_dag.py`**
  * Continuous micro-batch logic embedded in the same DAG 
  * Task dependencies and data-quality gates between layers
  * Automatic retries, alerting, and log retention
* **Key Files**
  * `orchestration/dags/`

---

## 2. Streaming Component `(/streaming)`

* **Purpose** – Simulates and ingests real-time event data using Kafka.
* **Technology** – Apache Kafka with Python producers
* **Key Features**
  * `orders_producer.py` – streams order and order-item events
* **Key Files**
  * `streaming/orders_producer.py`

---

## 3. Processing Component `(/processing)`

* **Purpose** – Handles data ingestion, transformation, and enrichment across Bronze, Silver, and Gold layers.
* **Technology** – Apache Spark 3.5.0 (PySpark)
* **Key Features**
  * `kafka_stream_ingestion.py` – writes raw data to Bronze
  * `silver_processing.py` – cleans & validates Silver tables
  * `gold_processing.py` – builds dimensional models in Gold
  * `late_data_detector.py` – detects and integrates late-arriving events
  * Implements SCD-2 logic for slowly changing dimensions
* **Key Files** – all Python modules inside `processing/`

---

## 4. Storage & Catalog Components

* **MinIO (S3-compatible object storage)** – Stores Parquet files for Bronze, Silver, and Gold tables; accessed via the S3 API by Spark and Iceberg.
* **Apache Iceberg** – Manages table metadata, schema evolution, partitioning, and time-travel capabilities. Integrated with Spark for reads/writes.

Both services run inside Docker containers and are referenced by the Spark and Airflow stacks via environment variables. 