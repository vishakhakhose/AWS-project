from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_submit_dag',
    default_args=default_args,
    schedule_interval='@once',
)

# Define the SparkSubmitOperator to run the PySpark job
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/opt/airflow/dags/process_data_in_pyspark.py',  # Path to your PySpark script
    conn_id='spark_conn',  # Airflow connection ID for the Spark cluster
    dag=dag,
    # java_home='/usr/lib/jvm/java-8-openjdk-amd64',
)

spark_submit_task
