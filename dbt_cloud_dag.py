from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from datetime import timedelta,datetime
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dbt_cloud_conn_id':'dbt-cloud'
}
dag = DAG('dbt_cloud_dag',
        default_args=default_args,
        description='DAG to trigger dbt Cloud jobs',
        start_date=datetime(2023, 12, 3),
        schedule_interval='@daily',
        catchup=False
        )
dbt_cloud_task = DbtCloudRunJobOperator(
    task_id = "dbt_cloud_task",
    account_id='222318',
    job_id='467953',
    dag = dag
)
task_dbt_check = DbtCloudJobRunSensor(
    task_id="task_dbt_check", dbt_cloud_conn_id='dbt-cloud', run_id=dbt_cloud_task.output, timeout=20, 
    dag=dag, 
)
 
          