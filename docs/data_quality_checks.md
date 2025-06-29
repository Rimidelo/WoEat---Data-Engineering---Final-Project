# Data Quality Checks

This project applies targeted data-quality controls at every stage to guarantee accuracy, consistency, and reliability.

---

## 1. Silver Layer  `(processing/silver_processing.py)`

* Orders validation – null checks and business-rule validation
* Order-items validation – ensure `quantity > 0` and `price > 0`
* Ratings validation – values constrained between 1.0 and 5.0
* Cross-table consistency – referential integrity between related tables
* Cleanup – removal of invalid records and data standardisation

---

## 2. Airflow DAG Quality Checks  `(orchestration/dags/woeat_etl_dag.py)`

* Bronze layer checks – validate existence and structure of raw data
* Silver layer checks – enforce business rules before promotion
* Quality gates – stop pipeline if checks fail
* Logging & alerts – failures recorded and surfaced in the Airflow UI

---

## 3. Late-Data Monitoring  `(processing/late_data_detector.py)`

* Watermarking – accept late-arriving data up to 48 hours
* Quality scoring – classify incoming data as OK, Warning, or Critical
* Impact reporting – assess changes introduced by late data
* Dashboards – track data-quality metrics over time

---

## 4. Business-Rule Enforcement

* Temperature range – valid range between −50 °C and 60 °C
* Price validation – only positive price values allowed
* Rating bounds – ratings constrained between 1.0 and 5.0
* Foreign keys – referential integrity maintained across tables 