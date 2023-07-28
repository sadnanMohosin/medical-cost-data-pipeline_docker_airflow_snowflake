import json
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from snowflake.connector import connect, Error
from snowflake_connection import snowflake_conn

def load_data_from_unstructured_column():
    try:
        # Connect to Snowflake
        connection = connect(**snowflake_conn)
        cursor = connection.cursor()

        # Extract data from the unstructured_data table
        select_query = "SELECT data FROM unstructured_data"
        cursor.execute(select_query)
        results = cursor.fetchall()

        # Transform and load data into the structured_data table
        insert_query = """
            INSERT INTO structured_data (
                age,
                sex,              
                bmi,
                children,
                smoker,
                region,
                charges
            )
            VALUES (
                %(age)s,
                %(sex)s,
                %(bmi)s,
                %(children)s,
                %(smoker)s,
                %(region)s,
                %(charges)s
            )
        """

        for row in results:
            data_string = row[0]
            data = json.loads(data_string)  # Parse the data string as JSON

            # Prepare values for the insert query
            values = {
                'age': data.get('age', None),
                'sex': data.get('sex', None),
                'bmi': data.get('bmi', None),
                'children': data.get('children', None),
                'smoker': data.get('smoker', None),
                'region': data.get('region', None),
                'charges': data.get('charges', None)
            }

            # Handle empty or missing numeric values
            for key in ['age', 'sex', 'bmi', 'children', 'smoker', 'region', 'charges']:
                if values[key] == '':
                    values[key] = None

            cursor.execute(insert_query, values)

        # Commit the changes
        connection.commit()
        print("Data successfully loaded to Snowflake.")

    except Error as e:
        print("Error: ", e)

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()