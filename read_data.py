from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

import pandas as pd

#DEFINE THE DAG
dag = DAG(
    'ETL_RIWAYAT_PENDIDIKAN',
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 5, 5),
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    schedule_interval=None
)

create_table_jenis_keluar = """
CREATE TABLE IF NOT EXISTS core.dim_jenis_keluar_mahasiswa(
    id_jenis_keluar INT NOT NULL,
    jenis_keluar VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(id_jenis_keluar)
)
"""

create_table_core_riwayat_pendidikan = """
CREATE TABLE IF NOT EXISTS core.fact_riwayat_pendidikan(
    id_mahasiswa TEXT,
    nim VARCHAR(12) NOT NULL,
    id_jenis_daftar INT,
    id_periode_masuk VARCHAR(10),
    tanggal_daftar DATE,
    id_jenis_keluar VARCHAR(10),
    id_prodi TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(id_mahasiswa)
)
"""

query_core_jenis_keluar = """
    SELECT 
        jenis.id_jenis_keluar,
        jenis.jenis_keluar
    FROM jenis_keluar as jenis
"""

query_core_riwayat_pendidikan = """
    SELECT
        rw.id_mahasiswa,
        rw.nim,
        rw.id_jenis_daftar,
        rw.id_periode_masuk,
        rw.tanggal_daftar,
        rw.id_jenis_keluar,
        rw.id_prodi
    FROM riwayat_pendidikan_mahasiswa as rw
    WHERE rw.id_periode_masuk > '20181' 
"""

create_table_core_riwayat_pendidikan_task = PostgresOperator(
    task_id = 'create_table_core_riwayat_pendidikan_task',
    postgres_conn_id = 'dw-sipadu',
    sql = create_table_core_riwayat_pendidikan,
    autocommit = True,
    dag=dag
)

create_table_core_jenis_keluar_task = PostgresOperator(
    task_id = 'create_table_core_jenis_keluar_task',
    postgres_conn_id = 'dw-sipadu',
    sql = create_table_jenis_keluar,
    autocommit = True,
    dag=dag
)

def etl_core_jenis_keluar():
    hook_extract = PostgresHook(postgres_conn_id='sipadu-perkuliahan')
    conn_extract = hook_extract.get_conn()
    hook_load = PostgresHook(postgres_conn_id='dw-sipadu')
    conn_load = hook_load.get_conn()

    query = "SELECT DISTINCT updated_at FROM core.dim_jenis_keluar_mahasiswa ORDER BY updated_at DESC"
    df_updated = pd.read_sql(query,conn_load)

    if(len(df_updated)==0):
        get_delta_load_query = query_core_jenis_keluar
    elif(len(df_updated)>0):
        get_delta_load_query = query_core_jenis_keluar + " WHERE jenis.updated_at > '{}' ORDER BY id_jenis_keluar".format(df_updated['updated_at'][0])
    
    df_jenis_keluar = pd.read_sql(get_delta_load_query,conn_extract)

    #Transform Data
    df_jenis_keluar['id_jenis_keluar'] = df_jenis_keluar['id_jenis_keluar'].astype(int)
    df_jenis_keluar['jenis_keluar'] = df_jenis_keluar['jenis_keluar'].astype(str)

    #Load Data to Postgres
    tuples = [tuple(x) for x in df_jenis_keluar.to_numpy()]
    cols = ','.join(list(df_jenis_keluar.columns))
    query  = """INSERT INTO %s (%s,updated_at) VALUES (%%s,%%s,CURRENT_TIMESTAMP)
                ON CONFLICT (id_jenis_keluar)
                DO UPDATE SET
                jenis_keluar = EXCLUDED.jenis_keluar,
                updated_at = CURRENT_TIMESTAMP
            """ % ('core.dim_jenis_keluar_mahasiswa',cols)
    cursor = conn_load.cursor()
    cursor.executemany(query, tuples)

    conn_load.commit()
    cursor.close()

def etl_core_riwayat_pendidikan ():
    hook_extract = PostgresHook(postgres_conn_id='sipadu-perkuliahan')
    conn_extract = hook_extract.get_conn()
    hook_load = PostgresHook(postgres_conn_id='dw-sipadu')
    conn_load = hook_load.get_conn()

    query = "SELECT DISTINCT updated_at FROM core.fact_riwayat_pendidikan ORDER BY updated_at DESC"
    df_updated = pd.read_sql(query,conn_load)

    
    if(len(df_updated)==0):
        get_delta_load_query = query_core_riwayat_pendidikan
    elif(len(df_updated)>0):
        get_delta_load_query = query_core_riwayat_pendidikan + " AND rw.updated_at > '{}'".format(df_updated['updated_at'][0])
    
    df_rw = pd.read_sql(get_delta_load_query,conn_extract)

    #Transform Data
    df_rw['id_mahasiswa'] = df_rw['id_mahasiswa'].astype(str)
    df_rw['nim'] = df_rw['nim'].astype(str)
    df_rw['id_jenis_daftar'] = df_rw['id_jenis_daftar'].astype(int)
    df_rw['id_periode_masuk'] = df_rw['id_periode_masuk'].astype(str)
    df_rw['tanggal_daftar'] = pd.to_datetime(df_rw['tanggal_daftar'],  format='%Y-%m-%d')
    df_rw['id_jenis_keluar'] = df_rw['id_jenis_keluar'].astype(str)
    df_rw['id_prodi'] = df_rw['id_prodi'].astype(str)

    #Load Data to Postgres
    tuples = [tuple(x) for x in df_rw.to_numpy()]
    cols = ','.join(list(df_rw.columns))
    query  = """INSERT INTO %s (%s,updated_at) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,CURRENT_TIMESTAMP)
                ON CONFLICT (id_mahasiswa)
                DO UPDATE SET
                nim = EXCLUDED.nim,
                id_jenis_daftar = EXCLUDED.id_jenis_daftar,
                id_periode_masuk = EXCLUDED.id_periode_masuk,
                tanggal_daftar = EXCLUDED.tanggal_daftar,
                id_jenis_keluar = EXCLUDED.id_jenis_keluar,
                id_prodi = EXCLUDED.id_prodi,
                updated_at = CURRENT_TIMESTAMP
            """ % ('core.fact_riwayat_pendidikan',cols)
    cursor = conn_load.cursor()
    cursor.executemany(query, tuples)

    #add timestamps to database
    query_ts = "INSERT INTO core.timestamp (etl_tools) VALUES ('ETL Riwayat Pendidikan Mahasiswa')"
    cursor.execute(query_ts)

    conn_load.commit()
    cursor.close()

def core_riwayat_pendidikan_load_null():
    #Postgres connection
    dw_postgres_hook = PostgresHook(postgres_conn_id='dw-sipadu')
    conn_dw_postgres_hook = dw_postgres_hook.get_conn()
    column = ['id_jenis_keluar']
    for col in column:
        query = "UPDATE core.fact_riwayat_pendidikan SET {} = NULL WHERE {} = 'None'".format(col,col)
        cursor = conn_dw_postgres_hook.cursor()
        cursor.execute(query)
        conn_dw_postgres_hook.commit()
        cursor.close()

with dag:
    etl_core_jenisKeluar_task = PythonOperator(
        task_id = 'etl_core_ipkm_task',
        python_callable = etl_core_jenis_keluar
    )

    etl_core_riwayat_pendidikan_task = PythonOperator(
        task_id = 'etl_core_riwayatPendidikan_task',
        python_callable = etl_core_riwayat_pendidikan
    )

    etl_core_load_null = PythonOperator(
        task_id = 'etl_core_riwayatPendidikan_load_null',
        python_callable = core_riwayat_pendidikan_load_null
    )

    #Set up dependencies
    create_table_core_jenis_keluar_task >> create_table_core_riwayat_pendidikan_task >> etl_core_jenisKeluar_task >> etl_core_riwayat_pendidikan_task >> etl_core_load_null
