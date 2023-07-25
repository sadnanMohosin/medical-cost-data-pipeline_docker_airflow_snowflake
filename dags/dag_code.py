from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from load_dataset import load_dataset_to_snowflake
from create_structured_table import create_structured_table
from load_data import load_data_from_unstructured_column
from create_stat_tables import smokers_region_statistics
from load_stat_data import load_smokers_statistics
from create_stat_tables import region_obesity_stat
from load_stat_data import load_region_obesity_stat

default_args = {
    'owner': 'sadnan',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 23),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG('medical_cost_pipeline',default_args=default_args,schedule_interval=None) as dag:

    load_dataset_task = PythonOperator(
        task_id = 'load_dataset_to_snowflake',
        python_callable = load_dataset_to_snowflake
    )

    create__structured_table_task = PythonOperator(
        task_id = 'create_structured_table',
        python_callable = create_structured_table
    )

    load_data_to_sturctured_table = PythonOperator(
        task_id = 'structured_table',
        python_callable = load_data_from_unstructured_column
    )
    create_region_stat_table = PythonOperator(
        task_id = 'region_stat',
        python_callable = smokers_region_statistics
    )
    load_region_stat = PythonOperator(
        task_id = 'load_region_stat',
        python_callable = load_smokers_statistics
    )
    
    region_obesity_stat_table = PythonOperator(
        task_id = 'region_obesity',
        python_callable = region_obesity_stat
    )
    load_region_obesity_stats = PythonOperator(
        task_id = 'load_region_obesity',
        python_callable = load_region_obesity_stat
    )

load_dataset_task >> create__structured_table_task >> load_data_to_sturctured_table >> create_region_stat_table >> load_region_stat >> region_obesity_stat_table >> load_region_obesity_stats