from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from get_data_from_mongo import get_data_from_mongo
from send_data_to_mysql import send_data_to_mysql
from datetime import timedelta, date, datetime
import awoc

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
def success_fn(**kwargs):
    print(kwargs)
    print("success function")
    kwargs['ti'].xcom_push(key= 'output', value='success')
    print(kwargs['ti'].xcom_push(key= 'output', value='success'))

def failure_fn(**kwargs):
    print(kwargs)
    print("failure function")
    kwargs['ti'].xcom_push(key= 'output', value='failure')
    print(kwargs['ti'].xcom_push(key= 'output', value='failure'))

with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    # schedule_interval=timedelta(days=1)
    ) as dag:

    get_data_from_mongo = PythonOperator( task_id='get_data_from_mongo',
                                        python_callable = get_data_from_mongo,
                                        trigger_rule='one_success')
    # Define the SQL query to be executed

    is_file_present = FileSensor(task_id = 'is_file_present', 
                                 filepath = 'advertising1.csv', 
                                 fs_conn_id = 'file_conn', 
                                 poke_interval=5, 
                                 timeout=20, 
                                 )

    send_data_to_mysql = PythonOperator(task_id = 'send_data_to_mysql',
                                        python_callable= send_data_to_mysql,
                                        trigger_rule='one_success')
    

    def branch():
        import pymysql
        import awoc
        details = awoc.AWOC()
        mysql_host = "mysql"
        mysql_port = 3306
        mysql_user = "root"
        mysql_password = "password"
        mysql_database = "mysql"
        mysql_table = "advertising"

        # # Create a connection to MySQL
        connection = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        cursor = connection.cursor()
        cursor.execute(f"SELECT distinct country FROM {mysql_table}")
        coun = list(cursor.fetchall())
        print(coun)
        countries = [i[0] for i in coun]

        # countries = [row['country'] for row in ]
        print('++', countries)

        for i in countries:
            try:
                continent = details.get_country_data(i)['Continent Name']
                continent = continent.replace(' ', '_').lower()
                # cursor.execute(f"create table {i} as select * from {mysql_table} where country = '{i}'")
                print(i, continent)
            except:
                print(f"country {i} doesnot exist!")
                pass
            return continent + "_task"
            
    branch_task = BranchPythonOperator(task_id = 'branch', 
                                  python_callable=branch,
                                    provide_context=True,
                                    trigger_rule = 'all_done')


    # Define a success task (e.g., an operator that will run if the SQL query result is non-zero)
    # success_task = MySqlOperator(
    #     task_id='success_task',
    #     mysql_conn_id='mysql_conn',
    #     sql="SELECT * FROM advertising WHERE Country = 'Egypt' and City = 'North Michael';",
    #     trigger_rule='all_success'
    # )

    # Define a failure task (e.g., an operator that will run if the SQL query result is zero)
    # failure_task = MySqlOperator(
    #     task_id='failure_task',
    #     mysql_conn_id='mysql_conn',
    #     sql="SELECT * FROM advertising WHERE Country = 'Bangladesh';",
    #     trigger_rule='one_failed'
    # )

# Define the workflow
import re
get_data_from_mongo >> is_file_present >> send_data_to_mysql >> branch_task
details = awoc.AWOC()
for task in details.get_continents_list():
    task = task.replace(' ', '_').lower()
    branch_task.set_downstream(DummyOperator(task_id= f'{task}_task', dag=dag))
    
#     
# sql_sensor_task >> success_task
# sql_sensor_task >> failure_task

