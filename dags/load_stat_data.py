import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from snowflake_connection import snowflake_conn
from snowflake.connector import connect

def execute_sql_query(query):
    connection = connect(**snowflake_conn)
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
    connection.close()

def load_smokers_statistics():
    # statistics about smokers depending on the region
    query = """
        INSERT INTO smokers_region_statistics
        SELECT 
            region,
            SUM(iff(smoker='yes', 1, 0))/(SUM(iff(smoker='yes', 1, 0)) + SUM(iff(smoker='no', 1, 0))) * 100 as smoker_percent
        FROM structured_data
        GROUP BY region;
    """
    execute_sql_query(query)

def load_region_obesity_stat():
    # statistics about obesity depending on the region
    query = """
        INSERT INTO region_obesity
        SELECT region, sex, AVG(bmi)
        FROM structured_data
        WHERE bmi > 30
        GROUP BY 1, 2
        ORDER BY region;
    """
    execute_sql_query(query)