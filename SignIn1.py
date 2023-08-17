from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

class SignIn1:

    def __init__(self):
        self.default_args = {
            'owner': 'airflow',
            'retries': 5,
            'retry_delay': timedelta(minutes=2)
        }
    def get_token1(self):
        email = 'rakesh.h@fireflink.com'
        password = 'Password@123'
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)

        signin_payload = {
            'emailId': email,
            'password': password
        }
        response = session.post("http://10.10.10.30:8201/optimize/v1/public/user/signin", json=signin_payload)
        response.raise_for_status()
        token = response.json()["responseObject"]["access_token"]
        return token


