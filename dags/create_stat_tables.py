from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from snowflake.connector import connect, Error
from snowflake_connection import snowflake_conn

def create_table(table_name, table_columns):
    try:
        # Connect to Snowflake
        conn = connect(**snowflake_conn)
        cursor = conn.cursor()

        # Drop the table if it already exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create the table
        create_table_query = f"""
            CREATE TABLE {table_name} (
                {table_columns}
            )
        """
        cursor.execute(create_table_query)

        # Commit the changes
        conn.commit()
        print(f"Table '{table_name}' successfully created.")

    except Error as e:
        print("Error: ", e)

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()