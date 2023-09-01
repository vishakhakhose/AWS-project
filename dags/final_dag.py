from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from get_data_from_mongo import get_data_from_mongo
from send_data_to_mysql import send_data_to_mysql
# from process_data_in_pyspark import process_data_in_pyspark
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 18),
    'email': ['test@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with  DAG("final_dag_processing", 
          default_args=default_args,
          schedule_interval='@once'):
    
    get_data = PythonOperator(task_id = "get_data_from_pymongo", 
                            python_callable=get_data_from_mongo
                            )

    check_file = FileSensor(task_id = "check_if_file_is_present",
                            filepath = "advertising1.csv",
                            fs_conn_id = "file_conn",
                            poke_interval=5, 
                            timeout=20, 
                            )

    send_data = PythonOperator(task_id = 'send_data_to_mysql',
                                python_callable= send_data_to_mysql,
                                trigger_rule='one_success'
                                )

    process_data = SparkSubmitOperator(task_id="process_data_in_pyspark", 
                                        application='/opt/airflow/dags/process_data_in_pyspark.py',  # Path to your PySpark script
                                        conn_id='spark_conn',
                                        jars = "/opt/jars/mysql-connector-java-8.0.30.jar",  # Airflow connection ID for the Spark cluster
                                        )


get_data >> check_file >> send_data >> process_data