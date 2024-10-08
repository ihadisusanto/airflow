#Load data from postgres to mongodb database
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, date

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 11),
    'retries': 1,
}

dag = DAG(
    'postgres_to_mongo',
    default_args=default_args,
    schedule_interval=None
)

def extract_data_postgres(**kwargs):
    #Connect using postgres hook
    pg_hook = PostgresHook(postgres_conn_id='local-postgres')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute("SELECT * FROM public.dim_pertemuan_kuliah;")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description] #get the column names
    data = [dict(zip(columns,row)) for row in rows] # Convert rows to dictionary format

    for item in data:
        if isinstance(item['tanggal'], date):
            item['tanggal'] = datetime.combine(item['tanggal'], datetime.min.time()) #Since mongo DB doesn't support date format, we change it to datetime
    cursor.close()
    pg_conn.close()
    kwargs['ti'].xcom_push(key='data_pertemuan_kuliah',value=data)


def load_to_mongo(**kwargs):
    # Connect using mongo hook
    mongo_hook = MongoHook(mongo_conn_id='mongo_atlas')
    client = mongo_hook.get_conn()
    data = kwargs['ti'].xcom_pull(key='data_pertemuan_kuliah')
    db = client['sample_mflix'] #get the database name
    collection = db['pertemuan_kuliah']
    if data :
        collection.insert_many(data) #insert if not found, update if found

with dag:
    extract_data_task = PythonOperator(
        task_id='extract_data_postgres',
        python_callable=extract_data_postgres,
        dag=dag
    )

    load_to_mongo_task = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
        dag=dag
    )

    extract_data_task >> load_to_mongo_task