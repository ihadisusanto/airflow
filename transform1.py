from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from psycopg2 import sql
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
CREATE TABLE IF NOT EXISTS aggregation_transformation(
    english_country_region_name varchar(50),
    state_province_name varchar(50),
    city varchar(50),
    unit_price money,
    product_standard_cost money,
    total_product_cost money,
    sales_amount money,
    tax_amt money
)
"""

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

    #Write transformed data to destination PostgreSQL table
    # df.to_csv('/opt/bitnami/airflow/dags/transform1.csv',index=False)
    # Convert DataFrame to tuples
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = df.columns.tolist()
    
    # Dynamically construct SQL query using psycopg2's sql module
    query = sql.SQL("INSERT INTO aggregation_transformation ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, cols)),
        sql.SQL(', ').join(sql.Placeholder() * len(cols))
    )
    
    # Execute the query
    cursor = vercel_conn.cursor()
    cursor.executemany(query.as_string(vercel_conn), tuples)
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