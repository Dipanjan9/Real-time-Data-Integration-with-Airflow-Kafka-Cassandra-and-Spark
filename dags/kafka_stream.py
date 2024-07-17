from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import json
import requests 
import uuid
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

# Kafka broker configuration
# KAFKA_BROKER = 'broker:29092'  # Replace with your Kafka broker address

# Define a Kafka producer
producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)


# Define a callable function for PythonOperator

# This function will retrieve data from API endpoint
def get_data():
    url = "https://randomuser.me/api/"
    response = requests.get(url)
    data = response.json()
    return data['results'][0]
    
# This function will format the relevant data from data retrieved from API endpoint
def format_data(res):
    location = res['location']
    
    formatted_data = {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }
    
    return formatted_data

# This function will stream the formatted data
def stream_data():
    
    curr_time = time.time() # Current time in seconds
    topic = 'users_created'  # Replace with your Kafka topic name

    # Kafka producer configuration
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        request_timeout_ms=30000,
        max_block_ms=60000
    )

    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:
            res = get_data()
            formatted_res = format_data(res)
            producer.send(topic, formatted_res)
            producer.flush()
        except KafkaTimeoutError as e:
            logging.error(f'Kafka timeout error: {e}')
        except Exception as e:
            logging.error(f'An error occurred: {e}')
        time.sleep(1)  # Sleep for 1 second before sending the next message

    producer.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['dipanjanghosh430@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Instantiate the DAG
dag = DAG(
    'Kafka_dag',
    default_args = default_args,
    description ='DAG is for kafka Project',
    schedule = timedelta(days=1),
    start_date = datetime(2024, 6, 15),
    catchup = False,
)

# # Define the tasks
start_task = PythonOperator(
    task_id='StreamData_task',
    python_callable=stream_data,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
)


# #  Set task dependencies
start_task >> end_task
