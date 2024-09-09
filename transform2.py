from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 9),
    'retries': 1,
}

dag = DAG(
    'mssql_to_gsheet_etl',
    default_args=default_args,
    schedule_interval=None
)

query_transformation = """
    SELECT 
        EnglishCountryRegionName as english_country_region_name,
        StateProvinceName as state_province_name,
        City as city,
        SUM(Reseller.UnitPrice) as unit_price,
        SUM(Reseller.ProductStandardCost) as product_standard_cost,
        MIN(Reseller.TotalProductCost) as total_product_cost,
        MAX(Reseller.SalesAmount) as sales_amount,
        AVG(Reseller.TaxAmt) as tax_amt
    FROM DimGeography
    INNER JOIN FactResellerSales AS Reseller ON DimGeography.SalesTerritoryKey = Reseller.SalesTerritoryKey
    GROUP BY EnglishCountryRegionName, StateProvinceName, City
"""

def extract_from_mssql():
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_local')
    mssql_conn = mssql_hook.get_conn()
    df = pd.read_sql_query(query_transformation, mssql_conn)
    return df

def load_data_to_gsheet():
    sheet_hook = GSheetsHook(gcp_conn_id='google_cloud_default')
    df = extract_from_mssql()
    #Specify the spreadsheet ID and range
    sheet_id = '1IwXEaoUR5OdXc4z2lyBjr0xUYrLDZHb6uDYibgXsSto'
    sheet_range = 'Sheet2!A1'


    #Update the data include the column name
    values = [df.columns.tolist()] + df.values.tolist()

    #Update the spreadsheet with data
    sheet_hook.update_values(
        sheet_id,
        sheet_range,
        values,
        value_input_option='RAW'
    )

with dag:
    load_task = PythonOperator(
        task_id='load_data_to_gsheet',
        python_callable=load_data_to_gsheet,
    )
    load_task