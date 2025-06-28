from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'woeat-data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'woeat_etl_pipeline',
    default_args=default_args,
    description='WoEat ETL Pipeline - Micro-batch processing every 15min (auto-stops when Kafka empty)',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
    tags=['woeat', 'streaming', 'kafka', 'etl', 'clean'],
)

# Task 1: Start Pipeline
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Task 2: Health Checks - Kafka connectivity
def check_kafka_health():
    """Check if Kafka topics are healthy and receiving data"""
    logging.info("Checking Kafka topic health...")
    
    import subprocess
    try:
        # Check if orders topics exist
        result = subprocess.run([
            'docker', 'exec', 'kafka', 
            'kafka-topics', '--bootstrap-server', 'localhost:9092', '--list'
        ], capture_output=True, text=True)
        
        topics = result.stdout.strip().split('\n')
        required_topics = ['orders-topic', 'order-items-topic']
        
        for topic in required_topics:
            if topic in topics:
                logging.info(f"Topic {topic} exists")
            else:
                logging.error(f"Topic {topic} not found")
                return False
            
        logging.info("Kafka health check passed")
        return True
            
    except Exception as e:
        logging.error(f"Kafka health check failed: {e}")
        return False

kafka_health_check = PythonOperator(
    task_id='kafka_health_check',
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 3: Bronze Processing - Stream ingestion from Kafka (auto-stops when empty)
bronze_stream_ingestion = BashOperator(
    task_id='bronze_stream_ingestion',
    bash_command='''
    echo "🥉 Starting Kafka stream ingestion to Bronze layer..."
    echo "Will automatically stop when no new data for 60 seconds..."
    if ! docker exec spark-iceberg python /home/iceberg/processing/kafka_stream_ingestion.py; then
        echo "❌ Bronze streaming ingestion failed!"
        exit 1
    fi
    echo "✅ Bronze streaming completed - Kafka topics processed"
    ''',
    dag=dag,
)

# Task 4: Silver Processing - Data cleaning and quality validation
silver_processing = BashOperator(
    task_id='silver_processing',
    bash_command='''
    echo "🥈 Starting Silver layer processing..."
    docker exec spark-iceberg python /home/iceberg/processing/silver_processing.py
    echo "Silver processing completed"
    ''',
    dag=dag,
)

# Task 5: Gold Processing - Star schema creation
gold_processing = BashOperator(
    task_id='gold_processing', 
    bash_command='''
    echo "🥇 Starting Gold layer processing..."
    docker exec spark-iceberg python /home/iceberg/processing/gold_processing.py
    echo "Gold processing completed"
    ''',
    dag=dag,
)

# Task 6: Late Data Detection - Automatic detection and reprocessing
late_data_detection = BashOperator(
    task_id='late_data_detection',
    bash_command='''
    echo "Checking for late-arriving data..."
    docker exec spark-iceberg python /home/iceberg/processing/late_data_detector.py --mode detect
    echo "Late data detection completed"
    ''',
    dag=dag,
)

# Task 7: Business Reports - Generate analytics
def generate_business_reports():
    """Generate business analytics and reports"""
    logging.info("Generating business reports...")
    
    import subprocess
    try:
        # Generate dashboard
        result = subprocess.run([
            'docker', 'exec', 'spark-iceberg', 
            'python', '/home/iceberg/project/create_data_dashboard.py'
        ], capture_output=True, text=True)
        
        logging.info("Dashboard generated")
        
        # Log some basic metrics
        metrics_result = subprocess.run([
            'docker', 'exec', 'spark-iceberg', 'spark-sql', '-e',
            '''SELECT 
                 'Orders' as metric, COUNT(*) as count 
               FROM gold.fact_orders
               UNION ALL
               SELECT 
                 'Revenue' as metric, ROUND(SUM(total_amount), 2) as count
               FROM gold.fact_orders'''
        ], capture_output=True, text=True)
        
        logging.info(f"Business metrics: {metrics_result.stdout}")
        logging.info("Business reports generated")
        return True
        
    except Exception as e:
        logging.error(f"Business reports failed: {e}")
        return False

business_reports = PythonOperator(
    task_id='business_reports',
    python_callable=generate_business_reports,
    dag=dag,
)

# Task 8: Data Quality Monitoring
def monitor_data_quality():
    """Monitor overall data quality across all layers"""
    logging.info("Monitoring data quality...")
    
    import subprocess
    try:
        # Check Bronze layer
        bronze_result = subprocess.run([
            'docker', 'exec', 'spark-iceberg', 'spark-sql', '-e',
            'SELECT COUNT(*) as bronze_orders FROM bronze.bronze_orders'
        ], capture_output=True, text=True)
        
        # Check Silver layer
        silver_result = subprocess.run([
            'docker', 'exec', 'spark-iceberg', 'spark-sql', '-e',
            'SELECT COUNT(*) as silver_orders FROM silver.silver_orders'
        ], capture_output=True, text=True)
        
        # Check Gold layer
        gold_result = subprocess.run([
            'docker', 'exec', 'spark-iceberg', 'spark-sql', '-e',
            'SELECT COUNT(*) as gold_orders FROM gold.fact_orders'
        ], capture_output=True, text=True)
        
        logging.info(f"Bronze orders: {bronze_result.stdout}")
        logging.info(f"Silver orders: {silver_result.stdout}")
        logging.info(f"Gold orders: {gold_result.stdout}")
        logging.info("Data quality monitoring completed")
        return True
        
    except Exception as e:
        logging.error(f"Data quality monitoring failed: {e}")
        return False

data_quality_monitoring = PythonOperator(
    task_id='data_quality_monitoring',
    python_callable=monitor_data_quality,
    dag=dag,
)

# Task 9: Pipeline Completion
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies - Clean linear flow
start_pipeline >> kafka_health_check >> bronze_stream_ingestion >> silver_processing >> gold_processing >> late_data_detection >> business_reports >> data_quality_monitoring >> end_pipeline 