from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from snowflake.connector import connect
from snowflake_connection import snowflake_conn


def smokers_region_statistics():
    conn = connect(**snowflake_conn)
    cursor = conn.cursor()
    
    # Drop the max_ratings table if it already exists
    cursor.execute("DROP TABLE IF EXISTS smokers_region_statistics")
    
    # Create the max_ratings table
    cursor.execute("""
        CREATE TABLE smokers_region_statistics (
            region STRING,
            smoker_percent FLOAT     
            
        )
    """)
    # conn.commit
    cursor.close()
    conn.close()


def region_obesity_stat():
    conn = connect(**snowflake_conn)
    cursor = conn.cursor()
    
    # Drop the max_ratings table if it already exists
    cursor.execute("DROP TABLE IF EXISTS region_obesity")
    
    # Create the max_ratings table
    cursor.execute("""
        CREATE TABLE region_obesity (
            region STRING,
            sex string,
            avg_obesity float     
            
        )
    """)
    # conn.commit
    cursor.close()
    conn.close()