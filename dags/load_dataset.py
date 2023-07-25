import csv
import json 
import os
from snowflake.connector import connect
from snowflake_connection import snowflake_conn


def load_dataset_to_snowflake():
    # File path to the CSV file
    csv_file_path = os.path.join(os.getcwd(), 'dags', 'insurance.csv')

    # Create a Snowflake connection
    conn = connect(**snowflake_conn)

    # Create a Snowflake cursor
    cursor = conn.cursor()

    # Create the table with a single column of type VARIANT
    cursor.execute("CREATE TABLE IF NOT EXISTS unstructured_data (data VARIANT)")
    cursor.execute("TRUNCATE TABLE unstructured_data")
    # Read the CSV file and insert data into the table
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            # Convert the row to JSON format
            variant_data = json.dumps(row)
            
            
            # Insert the data into the table
            cursor.execute("INSERT INTO unstructured_data (data) SELECT PARSE_JSON(%s)", (variant_data,))

    # Close the cursor and connection
    cursor.close()
    conn.close()