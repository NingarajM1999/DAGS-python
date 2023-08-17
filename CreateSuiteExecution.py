from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class CreateSuiteExecution:
    def __init__(self):
        self.default_args = {
            'owner': 'airflow',
            'retries': 5,
            'retry_delay': timedelta(minutes=2)
        }

    def create_dag(self, dag_id, description, start_date, schedule_interval):
        dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args,
            description=description,
            start_date=start_date,
            schedule_interval=schedule_interval
        )

        def get_token():
            email = 'shubham.i@fireflink.com'
            password = 'Password@123'
            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)

            signin_payload = {
                'emailId': email,
                'password': password
            }
            response = session.post("https://app.fireflink.com:8201/optimize/v1/public/user/signin", json=signin_payload)
            print(response.content)
            print("Status Code:", response.status_code)
            response.raise_for_status()
            token = response.json()["responseObject"]["access_token"]
            return token

        def first_function_execute(**kwargs):
            token = get_token()
            kwargs['ti'].xcom_push(key='auth_token', value=token)
            return token

        def second_function_execute(**kwargs):
            ti = kwargs['ti']
            token = ti.xcom_pull(task_ids='sign_in_task', key='auth_token')
            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)

            session.headers
            headers = {'Content-Type': 'application/json',
                       'Authorization': 'Bearer {}'.format(token)
                       }
            print(headers)
            try:
                print("Before API call")
                response = session.post("https://app.fireflink.com:8209/optimize/execution/suite/Suite1001", headers=headers)
                response.raise_for_status()
                data = response.json()
                print("After API call. Response:", data)
            except requests.exceptions.RequestException as e:
                print("Error:", e)

            return "execution suite"

        with dag:
            first_function_execute_task = PythonOperator(
                task_id="sign_in_task",
                python_callable=first_function_execute,
                provide_context=True
            )
            second_function_execute_task = PythonOperator(
                task_id="execution_suite",
                python_callable=second_function_execute,
                provide_context=True
            )
            first_function_execute_task >> second_function_execute_task

        return dag

    def create_dags_with_tasks(self):
        dags = []

        # Create DAG 1 - Schedule every day
        dashBoard = self.create_dag(
            dag_id='CreateSuiteExecution',
            description='This is Execution dag',
            start_date=datetime(2023, 8, 16),
            schedule_interval=timedelta(days=1)
        )
        dags.append(dashBoard)

        return dags


createSuiteExecution = CreateSuiteExecution()

dags_with_tasks = createSuiteExecution.create_dags_with_tasks()

dag = dags_with_tasks[0]
task = dag.get_task("sign_in_task")
task1 = dag.get_task("execution_suite")

def read_properties(file_path):
    properties = {}
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Ignore empty lines and comments
                key, value = line.split('=')
                properties[key.strip()] = value.strip()
    return properties

file_path = 'config.properties'
properties = read_properties(file_path)

# Accessing properties
print(properties['database.host'])
print(properties['database.port'])
print(properties['database.username'])
print(properties['database.password'])





