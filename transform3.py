from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from datetime import datetime, timedelta
from bson import ObjectId

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 9),
    'retries': 1,
}

dag = DAG(
    'mongodb_to_gsheet',
    default_args=default_args,
    schedule_interval=None
)

def objectid_and_timestamp_to_str(doc):
    if isinstance(doc, dict):
        return {key: (
            str(value) if isinstance(value, ObjectId) 
            else value.to_pydatetime() if isinstance(value, pd.Timestamp) 
            else value
        ) for key, value in doc.items()}
    return doc

def extract_mongo_data(**kwargs):
    mongo_hook = MongoHook(mongo_conn_id="mongo_atlas")
    client = mongo_hook.get_conn()
    db = client['sample_mflix']
    collection = db['movies']
    data = list(collection.find())
    mongo_data_cleaned = [objectid_and_timestamp_to_str(doc) for doc in data]
    kwargs['ti'].xcom_push(key='movies',value=mongo_data_cleaned)

def transform_data(**kwargs):
    movies = kwargs['ti'].xcom_pull(key='movies')
    df = pd.json_normalize(movies)
    # df.drop(columns=['_id'],inplace=True)
    kwargs['ti'].xcom_push(key='transformed_movies', value=df.to_dict(orient='records'))

def save_to_csv(**kwargs):
    transformed_movies = kwargs['ti'].xcom_pull(key='transformed_movies')
    df = pd.DataFrame(transformed_movies)
    df.to_csv('/opt/bitnami/airflow/movies.csv',index=False)

with dag:
    extract_task = PythonOperator(
        task_id='extract_mongo_data',
        python_callable=extract_mongo_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        depends_on_past=True,
        provide_context=True,
    )

    save_to_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        depends_on_past=True,
        provide_context=True,
    )
    
    extract_task  >> transform_task >> save_to_csv_task