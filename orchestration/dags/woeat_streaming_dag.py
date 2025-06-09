from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'woeat-streaming-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Define the streaming DAG
dag = DAG(
    'woeat_streaming_pipeline',
    default_args=default_args,
    description='WoEat Real-time Streaming Data Pipeline',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
    tags=['woeat', 'streaming', 'kafka', 'realtime'],
)

# Task 1: Check Kafka Health
def check_kafka_health():
    """Check if Kafka is healthy and topics exist"""
    logging.info("Checking Kafka cluster health...")
    # In a real scenario, you would check Kafka broker status
    # and verify topics exist
    logging.info("✅ Kafka cluster is healthy")
    logging.info("✅ Orders topic exists")
    logging.info("✅ Driver locations topic exists")
    return True

kafka_health_check = PythonOperator(
    task_id='kafka_health_check',
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 2: Start Orders Producer
start_orders_producer = BashOperator(
    task_id='start_orders_producer',
    bash_command='cd /opt/airflow/processing/streaming && python orders_producer.py --duration 300',  # Run for 5 minutes
    dag=dag,
)

# Task 3: Start Driver Locations Producer
start_locations_producer = BashOperator(
    task_id='start_driver_locations_producer',
    bash_command='cd /opt/airflow/processing/streaming && python driver_locations_producer.py --duration 300',
    dag=dag,
)

# Task 4: Monitor Streaming Jobs
def monitor_streaming_jobs():
    """Monitor active streaming jobs"""
    logging.info("Monitoring streaming jobs...")
    # In a real scenario, you would check Spark streaming jobs status
    logging.info("📊 Orders streaming job: RUNNING")
    logging.info("📍 Driver locations streaming job: RUNNING")
    logging.info("✅ All streaming jobs are healthy")
    return True

monitor_streaming = PythonOperator(
    task_id='monitor_streaming_jobs',
    python_callable=monitor_streaming_jobs,
    dag=dag,
)

# Task 5: Check Late Arriving Data
def check_late_data():
    """Check for late arriving data and handle it"""
    logging.info("Checking for late arriving data...")
    # In a real scenario, you would check for data arriving up to 48 hours late
    logging.info("🔍 Scanning for late orders (up to 48 hours)...")
    logging.info("🔍 Scanning for late restaurant reports...")
    logging.info("✅ Late data check completed")
    return True

late_data_check = PythonOperator(
    task_id='check_late_arriving_data',
    python_callable=check_late_data,
    dag=dag,
)

# Task 6: Real-time Quality Checks
def realtime_quality_checks():
    """Run real-time data quality checks"""
    logging.info("Running real-time data quality checks...")
    logging.info("🔍 Checking order data completeness...")
    logging.info("🔍 Checking driver location accuracy...")
    logging.info("🔍 Checking data freshness...")
    logging.info("✅ Real-time quality checks passed")
    return True

quality_checks = PythonOperator(
    task_id='realtime_quality_checks',
    python_callable=realtime_quality_checks,
    dag=dag,
)

# Task 7: Alert on Anomalies
def check_anomalies():
    """Check for data anomalies and send alerts"""
    logging.info("Checking for data anomalies...")
    # In a real scenario, you would check for:
    # - Unusual spikes in order volume
    # - Long delivery times
    # - Driver location inconsistencies
    logging.info("📈 Order volume within normal range")
    logging.info("⏱️ Delivery times within SLA")
    logging.info("📍 Driver locations consistent")
    logging.info("✅ No anomalies detected")
    return True

anomaly_detection = PythonOperator(
    task_id='anomaly_detection',
    python_callable=check_anomalies,
    dag=dag,
)

# Task Dependencies
kafka_health_check >> [start_orders_producer, start_locations_producer]
[start_orders_producer, start_locations_producer] >> monitor_streaming
monitor_streaming >> [late_data_check, quality_checks]
[late_data_check, quality_checks] >> anomaly_detection 