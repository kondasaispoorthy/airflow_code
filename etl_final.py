# Importing the required Modules
import time
import subprocess
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
# Define your AWS Glue client
glue_client = boto3.client(
    'glue',
    region_name='eu-north-1',
    aws_access_key_id='AKIA4N7RVQEQUUAHHWTT',
    aws_secret_access_key='2Ten/Rb/7hEOQnsBjnufc0WdjYikmF6vSP2O8Kya'
)
 
# Function to run Glue jobs
def run_glue_job(job_name):
    try:
        job_run = glue_client.start_job_run(JobName=job_name)
        print(f"{job_name} job run successfully")
    except Exception as e:
        print(f"Error: {e}")

# Function to transfer data from redshift to s3
def run_script(job_name):
        path = f"/home/spoorthy/airflow_demo/airflow/dags/{job_name}.py"
        subprocess.run(["python", path])
# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 3),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'dbt_cloud_conn_id':'dbt-cloud'   
}

# Creating DAG'S
dag = DAG(
    'etl_final',
    default_args=default_args,
    description='DAG to run AWS Glue jobs',
    schedule_interval='@daily',  # Set your preferred schedule
)
# List of s3 jobs to run
jobs=['offices','customers','employees','orderdetails','orders','productlines','products','payments']

# List of Glue jobs to run
job = ['sampleglue', 's3_to_stage_customers', 's3_to_stage_employees', 's3_to_stage_orderdetails', 's3_to_stage_orders', 's3_to_stage_productlines', 's3_to_stage_products', 's3_to_stage_payments']

run_task = PythonOperator(
        task_id="run_batch_control_start",
        python_callable=run_script,
        op_args=['batch_control_log_start'],  # Pass job_name as an argument to the Python function
        dag=dag,
    )

# Define a PythonOperator s3 jobs
for job_name in jobs:
    task_id = f'run_{job_name}_script'
    run_task = PythonOperator(
        task_id=task_id,
        python_callable=run_script,
        op_args=[job_name],  # Pass job_name as an argument to the Python function
        dag=dag,
        depends_on_past=True,
    )
    #print(f'run_{job_name}_scipt')

# Define a PythonOperator for glue jobs
for job_name in job:
 
    task_id = f'run_{job_name}_gluejob'
    run_glue_job_task = PythonOperator(
        task_id=task_id,
        python_callable=run_glue_job,
        op_args=[job_name],  # Pass job_name as an argument to the Python function
        dag=dag,
        depends_on_past=True,
    )
dbt_cloud_task = DbtCloudRunJobOperator(
    task_id = "trigger_dbt_cloud_job_run",
    account_id='222318',
    job_id='467953',
    dag = dag,
    depends_on_past=True,
)
run_task = PythonOperator(
        task_id="run_batch_control_end",
        python_callable=run_script,
        op_args=['batch_control_log_end'],  # Pass job_name as an argument to the Python function
        dag=dag,
        depends_on_past=True,
    )
