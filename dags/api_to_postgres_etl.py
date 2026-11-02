import json
import pendulum

import requests

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


API_ENDPOINT = "https://jsonplaceholder.typicode.com/"
POSTGRES_CONN_ID = "my_prod_database"


def fetch_data_from_api(endpoint: str) -> list:
    """
    Fetch data from the API endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.

    Returns:
        list: A list of dictionaries containing the fetched data.
    """
    response = requests.get(endpoint)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def transform_data(**context) -> list:
    fetched_data = context['ti'].xcom_pull(task_ids='fetch_data_from_api')
    transformed_data = []
    for item in fetched_data:
        for key, value in list(item.items()):
            if key == "email":
                item[key] = value.lower()
            if key == "address" or key == "company":
                del item[key]
        transformed_data.append(item)
    print(transformed_data)
    return transformed_data

def load_data_to_postgres(**context) -> None:
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_data")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    # create table if not exists
    pg_create_table_query = """CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT,
        email TEXT,
        phone TEXT,
        website TEXT)"""
    pg_cursor.execute(pg_create_table_query)
    pg_conn.commit()

    pg_cursor.executemany("INSERT INTO users (id, name, username, email, phone, website) " \
        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO " \
    "UPDATE SET name = EXCLUDED.name, username = EXCLUDED.username, email = EXCLUDED.email, " \
        "phone = EXCLUDED.phone, website = EXCLUDED.website", [[item["id"], item["name"], item["username"], 
            item["email"], item["phone"], item["website"]] for item in transformed_data])
    pg_conn.commit()

DAG_ID = "api_to_postgres_etl"
START_DATE = pendulum.datetime(2026, 11, 2, tz="UTC")
SCHEDULE = "@daily"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(dag_id=DAG_ID, default_args=default_args, start_date=START_DATE, schedule=SCHEDULE, catchup=False) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_data_from_api",
        python_callable=fetch_data_from_api,
        op_kwargs={"endpoint": f"{API_ENDPOINT}users"}
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_data_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_to_postgres,
    )

    fetch_data_task >> transform_data_task >> load_data_task