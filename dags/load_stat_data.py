import json
from datetime import datetime
from airflow import DAG
from snowflake_connection import snowflake_conn  # Import the Snowflake connection function
from airflow.operators.python_operator import PythonOperator
from snowflake.connector import connect


def load_smokers_statistics():
    connection = connect(**snowflake_conn)
    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE smokers_region_statistics;")

    # statistics about smokers depending on the region
    cursor.execute("""
        INSERT INTO smokers_region_statistics
        select 
            region,
            sum(iff(smoker='yes',1,0))/(sum(iff(smoker='yes',1,0))+sum(iff(smoker='no',1,0))) *100 as smoker_percent
        from structured_data
        group by region;""")
    cursor.close()
    connection.close()