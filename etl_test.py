import subprocess
from airflow.decorators import task
from datetime import datetime
from airflow import DAG

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="etl_test", start_date=datetime(2023, 12, 3), schedule_interval="@daily") as dag:
    @task()
    def run_customers():
        path = "/home/spoorthy/airflow_demo/airflow/dags/customers.py"
        subprocess.run(["python", path])
    @task()
    def run_offices():
        path = "/home/spoorthy/airflow_demo/airflow/dags/offices.py"
        subprocess.run(["python", path])
    @task()
    def run_employees():
        path = "/home/spoorthy/airflow_demo/airflow/dags/employees.py"
        subprocess.run(["python", path])
    @task()
    def run_payments():
        path = "/home/spoorthy/airflow_demo/airflow/dags/payments.py"
        subprocess.run(["python", path])
    @task()
    def run_orderdetails():
        path = "/home/spoorthy/airflow_demo/airflow/dags/orderdetails.py"
        subprocess.run(["python", path])
    @task()
    def run_products():
        path = "/home/spoorthy/airflow_demo/airflow/dags/products.py"
        subprocess.run(["python", path])
    @task()
    def run_orders():
        path = "/home/spoorthy/airflow_demo/airflow/dags/orders.py"
        subprocess.run(["python", path])
    @task()
    def run_productlines():
        path = "/home/spoorthy/airflow_demo/airflow/dags/productlines.py"
        subprocess.run(["python", path])


    run_customers() >> run_offices() >> run_employees() >> run_payments() >> run_orderdetails() >> run_products() >> run_orders() >> run_productlines()