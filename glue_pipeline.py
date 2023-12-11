from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3

# Define your AWS Glue client
glue_client = boto3.client(
    'glue',
    region_name='eu-north-1',
    aws_access_key_id='AKIA4N7RVQEQUUAHHWTT',
    aws_secret_access_key='2Ten/Rb/7hEOQnsBjnufc0WdjYikmF6vSP2O8Kya'
)
#file = open('/home/spoorthy/airflow_demo/airflow/dags/file_log.txt','a')

# Function to run Glue jobs
def run_glue_job(job_name):
    try:
        job_run = glue_client.start_job_run(JobName=job_name)
        status_detail = glue_client.get_job_runs(JobName=f"{job_name}")
        last_run = status_detail["JobRuns"][0]
        with open('/home/spoorthy/airflow_demo/airflow/dags/file_log.txt','a') as file:
            file.write(f''' '{last_run["JobName"]}',
                                '{last_run["Id"]}',
                                '{last_run["StartedOn"]}',
                                '{last_run["JobRunState"]}',
                                '{last_run["ExecutionTime"]}' ''')
    except Exception as e:
        print(f"Error: {e}")

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'glue_pipeline',
    default_args=default_args,
    description='DAG to run AWS Glue jobs',
    schedule_interval='@daily',  # Set your preferred schedule
)

# List of Glue jobs to run
jobs = ['sampleglue', 's3_to_stage_customers', 's3_to_stage_employees', 's3_to_stage_orderdetails', 's3_to_stage_orders', 's3_to_stage_productlines', 's3_to_stage_products', 's3_to_stage_payments']

# Define a PythonOperator for each job
for job_name in jobs:
    task_id = f'run_{job_name}_job'
    run_glue_job_task = PythonOperator(
        task_id=task_id,
        python_callable=run_glue_job,
        op_args=[job_name],  # Pass job_name as an argument to the Python function
        dag=dag,
    )

