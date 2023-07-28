from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from snowflake.connector import connect, Error
from snowflake_connection import snowflake_conn

def create_structured_table():
    try:
        # Connect to Snowflake
        conn = connect(**snowflake_conn)
        cursor = conn.cursor()

        # Drop the structured_data table if it already exists
        cursor.execute("DROP TABLE IF EXISTS structured_data")

        # Create the structured_data table
        cursor.execute("""
            CREATE TABLE structured_data (
                age INT,
                sex STRING,
                bmi FLOAT,
                children INT,
                smoker STRING,
                region STRING,
                charges FLOAT
            )
        """)

        # Commit the changes
        conn.commit()
        print("Structured table 'structured_data' successfully created.")

    except Error as e:
        print("Error: ", e)

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()