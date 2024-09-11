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

    service.spreadsheet().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()


with dag:
    create_sheet_task = PythonOperator(
        task_id='create_sheet',
        python_callable=create_new_sheet
    )

    create_sheet_task