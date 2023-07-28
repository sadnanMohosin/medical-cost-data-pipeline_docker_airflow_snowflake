import csv
import json 
import os
from snowflake.connector import connect 
from snowflake_connection import snowflake_conn

# Snowflake connection parameters
snowflake_conn = connect(**snowflake_conn)

def create_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unstructured_data (
            data VARIANT
        )
    """)

def truncate_table(cursor):
    cursor.execute("TRUNCATE TABLE unstructured_data")

def insert_data_from_csv(cursor, csv_file_path):
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Convert the row to JSON format
            variant_data = json.dumps(row)
            
            # Insert the data into the table
            cursor.execute("INSERT INTO unstructured_data (data) SELECT PARSE_JSON(%s)", (variant_data,))

def load_dataset_to_snowflake():
    # File path to the CSV file
    csv_file_path = os.path.join(os.getcwd(), 'dags', 'insurance.csv')

    try:
        # Create a Snowflake connection
        conn = snowflake.connector.connect(**snowflake_conn)

        # Create a Snowflake cursor
        cursor = conn.cursor()

        # Create the table if it does not exist
        create_table_if_not_exists(cursor)

        # Truncate the table to start fresh
        truncate_table(cursor)

        # Insert data from CSV into Snowflake table
        insert_data_from_csv(cursor, csv_file_path)

        # Commit the changes
        conn.commit()
        
        print("Data successfully loaded to Snowflake.")

    except Exception as e:
        print("Error: ", e)

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_dataset_to_snowflake()
