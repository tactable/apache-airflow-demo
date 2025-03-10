from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import pandas as pd
import os

# Define file paths (inside the Docker container)
DATA_DIR = "/opt/airflow/data"  # Ensure this is correctly mapped
CSV_FILE_1 = os.path.join(DATA_DIR, "file1.csv")
CSV_FILE_2 = os.path.join(DATA_DIR, "file2.csv")
OUTPUT_JSON = os.path.join(DATA_DIR, "merged.json")

# Read CSV 1
def read_csv_1(**kwargs):
    df1 = pd.read_csv(CSV_FILE_1)
    kwargs['ti'].xcom_push(key='df1', value=df1.to_json())  # Store as JSON string
    print(f"CSV 1 read successfully: {CSV_FILE_1}")
    raise Exception("Something went wrong!")

# Read CSV 2
def read_csv_2(**kwargs):
    df2 = pd.read_csv(CSV_FILE_2)
    kwargs['ti'].xcom_push(key='df2', value=df2.to_json())  # Store as JSON string

# Merge CSVs on 'id', use name from either file
def merge_csvs(**kwargs):
    ti = kwargs['ti']
    
    # Retrieve from XCom and convert JSON back to DataFrame
    df1 = pd.read_json(ti.xcom_pull(key='df1'))
    df2 = pd.read_json(ti.xcom_pull(key='df2'))
    
    # Merge on 'id' column (inner join)
    merged_df = pd.merge(df1, df2, on='id', how='outer')  # Assuming both have the same `id` to merge

    # Use `name` from both files directly (no need for suffixes since the names match)
    # This assumes 'name' columns in both files are identical
    merged_df['name'] = merged_df['name_x'].combine_first(merged_df['name_y'])

    # Drop redundant 'name_x' and 'name_y' columns
    merged_df = merged_df.drop(columns=['name_x', 'name_y'])
    
    kwargs['ti'].xcom_push(key='merged_df', value=merged_df.to_json())

# Convert to JSON and save
def convert_to_json(**kwargs):
    ti = kwargs['ti']
    merged_df = pd.read_json(ti.xcom_pull(key='merged_df'))
    merged_df.to_json(OUTPUT_JSON, orient="records", indent=4)
    
    print(f"JSON file saved at: {OUTPUT_JSON}")

# Define the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 5),
    "retries": 1,
}

with DAG(
    dag_id="split_merge_csv_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    task_read_csv_1 = PythonOperator(
        task_id="read_csv_1",
        python_callable=read_csv_1,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    task_read_csv_2 = PythonOperator(
        task_id="read_csv_2",
        python_callable=read_csv_2,
        provide_context=True,
    )

    task_merge_csvs = PythonOperator(
        task_id="merge_csvs",
        python_callable=merge_csvs,
        provide_context=True,
    )

    task_convert_to_json = PythonOperator(
        task_id="convert_to_json",
        python_callable=convert_to_json,
        provide_context=True,
    )

    # Task Dependencies
    [task_read_csv_1, task_read_csv_2] >> task_merge_csvs >> task_convert_to_json