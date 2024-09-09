from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

import pandas as pd

#DEFINE THE DAG

dag = DAG(
    'ETL_Transform_1',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 9, 9),
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    schedule_interval=None
)

create_destination_table = """
CREATE TABLE IF NOT EXISTS AggregationTransformation(
    EnglishCountryRegionName varchar(50),
    StateProvinceName varchar(50),
    City varchar(50),
    UnitPrice money,
    ProductStandardCost money,
    TotalProductCost money,
    SalesAmount money,
    TaxAmt money
)
"""

query_transformation = """
    SELECT 
        EnglishCountryRegionName,
        StateProvinceName,
        City,
        SUM(Reseller.UnitPrice) as UnitPrice,
        SUM(Reseller.ProductStandardCost) as ProductStandardCost,
        MIN(Reseller.TotalProductCost) as TotalProductCost,
        MAX(Reseller.SalesAmount) as SalesAmount,
        AVG(Reseller.TaxAmt) as TaxAmt
    FROM DimGeography
    INNER JOIN FactResellerSales AS Reseller ON DimGeography.SalesTerritoryKey = Reseller.SalesTerritoryKey
    GROUP BY EnglishCountryRegionName, StateProvinceName, City
"""

create_table_task = PostgresOperator(
    task_id = 'create_table_task',
    postgres_conn_id = 'vercel-postgres',
    sql = create_destination_table,
    autocommit = True,
    dag = dag
)

def etl_data ():
    #Connect to datasource MS SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='sql_server_local')
    mssql_conn = mssql_hook.get_conn()
    vercel_hook = PostgresHook(postgres_conn_id='vercel-postgres')
    vercel_conn = vercel_hook.get_conn()

    #Query data from source MS SQL Server
    df = pd.read_sql_query(query_transformation, mssql_conn)
    
    #transform data
    df['EnglishCountryRegionName'] = df['EnglishCountryRegionName'].astype(str)
    df['StateProvinceName'] = df['StateProvinceName'].astype(str)
    df['City'] = df['City'].astype(str)
    df['UnitPrice'] = df['UnitPrice'].astype(float)
    df['ProductStandardCost'] = df['ProductStandardCost'].astype(float)
    df['TotalProductCost'] = df['TotalProductCost'].astype(float)
    df['SalesAmount'] = df['SalesAmount'].astype(float)
    df['TaxAmt'] = df['TaxAmt'].astype(float)
    
    #Write transformed data to destination PostgreSQL table
    # df.to_csv('/opt/bitnami/airflow/dags/transform1.csv',index=False)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO AggregationTransformation (%s) VALUES %%s" % (cols)
    cursor = vercel_conn.cursor()
    cursor.executemany(query, tuples)
    vercel_conn.commit()
    cursor.close()
    # df.to_sql('AggregationTransformation', con=vercel_conn, index=False, if_exists='append')

with dag:
    etl_task = PythonOperator(
        task_id = 'etl_task',
        python_callable = etl_data,
        dag = dag
    )

    #Set the dependencies
    create_table_task >> etl_task