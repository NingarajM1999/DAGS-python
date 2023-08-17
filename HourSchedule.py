from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from  SignIn1 import  SignIn1
from requests.adapters import HTTPAdapter
from urllib3 import Retry

class HourSchedule:
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
        def first_function_execute(**kwargs):
            signIn1 = SignIn1()
            token = signIn1.get_token1()
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
                response = session.post("http://30.30.30.118:8216/optimize/v1/scheduler/scheduledExecutionDashboardStorageTask", headers=headers)
                response.raise_for_status()
                data = response.json()
                print("After API call. Response:", data)
            except requests.exceptions.RequestException as e:
                print("Error:", e)

            return "dashboard"
        def third_function_execute(**kwargs):
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
                response = session.post("http://30.30.30.118:8216/optimize/v1/scheduler/scheduledVideoAndScreenshotsStorageTask", headers=headers)
                response.raise_for_status()
                data = response.json()
                print("After API call. Response:", data)
            except requests.exceptions.RequestException as e:
                print("Error:", e)

            return "dashboard"
        with dag:
            first_function_execute_task = PythonOperator(
                task_id="sign_in_task",
                python_callable=first_function_execute,
                provide_context=True
            )
            second_function_execute_task = PythonOperator(
                task_id="scheduledExecutionDashboardStorageTask",
                python_callable=second_function_execute,
                provide_context=True
            )
            third_function_execute_task = PythonOperator(
                task_id="scheduledVideoAndScreenshotsStorageTask",
                python_callable=third_function_execute,
                provide_context=True
            )
            first_function_execute_task>>[second_function_execute_task,third_function_execute_task]
        return dag
    def create_dags_with_tasks(self):
        dags = []

        # Create DAG 1 - Schedule every 1 hour
        dashBoard = self.create_dag(
            dag_id='Schedule_hourly1',
            description='This is DAG running for one hour',
            start_date=datetime(2023, 8, 11),
            schedule_interval=timedelta(hours=1)
        )
        dags.append(dashBoard)

        return dags
hourSchedule=HourSchedule()
dags_with_tasks=hourSchedule.create_dags_with_tasks()
dag = dags_with_tasks[0]
task = dag.get_task("sign_in_task")
task1 = dag.get_task("scheduledExecutionDashboardStorageTask")
task2=dag.get_task("scheduledVideoAndScreenshotsStorageTask")

