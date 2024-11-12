from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import re
import tarfile
import os

import os

# Get the directory of the current file
current_file_path = os.path.dirname(os.path.abspath(__file__))

# Set log_path relative to the current file
log_path = os.path.join(current_file_path, '../the_logs/log.txt')

# Normalize the path
log_path = os.path.abspath(log_path)

print(log_path)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 12),  # adjust to a relevant date
}

# Define the DAG
dag = DAG(
    'process_web_log_v2',
    default_args=default_args,
    description='A DAG to process web server logs',
    schedule_interval='@daily',
)

# Task 1: Scan for log
def scan_for_log():
    if os.path.isfile(log_path):
        print("Log file found.")
    else:
        raise FileNotFoundError("Log file not found in the specified path.")

scan_for_log_task = PythonOperator(
    task_id='scan_for_log',
    python_callable=scan_for_log,
    dag=dag,
)

# Task 2: Extract data
def extract_data():
    with open(log_path, 'r') as f, open('extracted_data.txt', 'w') as out:
        for line in f:
            match = re.search(r'(\d+\.\d+\.\d+\.\d+)', line)  # Regex to match IP addresses
            if match:
                out.write(match.group(0) + '\n')

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task 3: Transform data
def transform_data():
    with open('extracted_data.txt', 'r') as f, open('transformed_data.txt', 'w') as out:
        for line in f:
            if line.strip() != '198.46.149.143':
                out.write(line)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Task 4: Load data
def load_data():
    with tarfile.open('weblog.tar', 'w') as tar:
        tar.add('transformed_data.txt')

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)



# Define the task dependencies
scan_for_log_task >> extract_data_task >> transform_data_task >> load_data_task
