from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
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
    dag_id = 'process_web_log_v2',
    default_args=default_args,
    description='A DAG to process web server logs',
    schedule_interval='@daily',
    tags=["data_workflow"]
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


import discord

# Define the function to send a message to Discord
def send_discord_message_task1():
    TOKEN = 'MTMwNjcyMzI2NDc5NzYwNTk3OA.GM7iLs.TQfbfGpHhrfkiCZKm-LEcQvsF1bz_-CMBMUwoo'
    CHANNEL_ID = '1306734755399729234'

    client = discord.Client(intents=discord.Intents.default())

    @client.event
    async def on_ready():
        channel = client.get_channel(int(CHANNEL_ID))
        await channel.send('The workflow send discord 1 has been executed successfully!')
        await client.close()

    client.run(TOKEN)

# Define the task to send a message to Discord
send_discord_message_task1 = PythonOperator(
    task_id='send_discord_message_task1',
    python_callable=send_discord_message_task1,
    dag=dag,
)

# Set the task dependencies
load_data_task >> send_discord_message_task1

# Define a Python callable function
def send_discord_message_task2():
    # Use the DiscordWebhookHook
    hook = DiscordWebhookHook(
        http_conn_id='discord_default',  # Connection ID defined in Airflow
        webhook_endpoint='webhooks/1306740551835713548/Pucypy2B8ZUT5NY2L0UWYnnuscVsUI1MBIx5npwXkxJFwVHvAYUKxAbCzwe_OQNUuTju',
        message='The workflow send discord 2 has been executed successfully!',
        username='Captain Hook'
    )
    # Execute the webhook
    hook.execute()


send_discord_message_task2 = PythonOperator(
    task_id='send_discord_message_task2',
    python_callable=send_discord_message_task2,
    dag=dag,
)

send_discord_message_task1 >> send_discord_message_task2