#Make google sheet, Load Data to Google sheet, and updated data

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
from googleapiclient.discovery import build

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 9),
    'retries': 1,
}

dag = DAG(
    'postgres_to_gsheet',
    default_args=default_args,
    schedule_interval=None
)

def create_new_sheet():
    sheet_hook = GSheetsHook(gcp_conn_id='google_cloud_default')
    credentials = sheet_hook.get_credentials()
    service = build('sheets', 'v4', credentials=credentials)

    spreadsheet_id = '1IwXEaoUR5OdXc4z2lyBjr0xUYrLDZHb6uDYibgXsSto'

    # Request to create a new sheet
    request_body = {
        "requests": [{
            "addSheet": {
                "properties": {
                    "title": "Peringkat-KS",  # Set the name for the new sheet
                    "gridProperties": {
                        "rowCount": 1000,  # Default number of rows
                        "columnCount": 26  # Default number of columns (A-Z)
                    }
                }
            }
        }]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()

extract_query = """
SELECT 
	bio.nim,
	bio.nama,
	bio.kabupaten_asal,
	bio.provinsi_asal,
	pa.keputusan,
	pa.ipk
FROM public.fact_perkuliahan_akademik as pa
INNER JOIN public.dim_biodata_mahasiswa as bio ON pa.nim = bio.nim
INNER JOIN public.dim_prodi as prodi ON pa.id_prodi = prodi.id_prodi
WHERE 
	bio.nim NOT IN ('222011294','221910851','222011788','222011255','222011407','222011732') AND
	TRIM(prodi.kode_prodi) = '49502' 
	AND pa.id_semester = '20231' 
	AND pa.kelas ILIKE ANY (ARRAY['4SI%','4SD%'])
	AND pa.keputusan = 'Lanjut Ke Semester Genap'
ORDER BY pa.ipk DESC
"""

def extract_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='local-postgres')
    conn = postgres_hook.get_conn()
    df = pd.read_sql(extract_query,conn)
    kwargs['ti'].xcom_push(key='data', value=df)

def load_data_to_sheet(**kwargs):
    df = kwargs['ti'].xcom_pull(key='data')
    sheet_hook = GSheetsHook(gcp_conn_id='google_cloud_default')

    #Specify the spreadsheet ID and range
    sheet_id = '1IwXEaoUR5OdXc4z2lyBjr0xUYrLDZHb6uDYibgXsSto'
    sheet_range = 'Peringkat-KS!A1'

    values = [df.columns.tolist()] + df.values.tolist()

    sheet_hook.update_values(
        spreadsheet_id=sheet_id,
        range=sheet_range,
        values=values,
        value_input_option='RAW'  # Use raw values for the data
    )



with dag:
    create_sheet_task = PythonOperator(
        task_id='create_sheet',
        python_callable=create_new_sheet
    )
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_sheet,
        provide_context=True
    )

    create_sheet_task >> extract_data_task >> load_data_task