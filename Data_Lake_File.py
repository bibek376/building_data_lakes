from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import boto3
from io import StringIO
import json
import pandas as pd



defaults_args={
    'owner':'bibek',
    'depends_on_past':False,
    'start_date':datetime(2024,2,29),
    'email':['airflowexample@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}



def fetch_and_save_country_details_to_s3():
    # Define country name and API URL
    country_name = "Nepal"
    api_url = f'https://api.api-ninjas.com/v1/country?name={country_name}'
    
    # Make API request to fetch country details
    response = requests.get(api_url, headers={'X-Api-Key': 'dCc4gXhgBdcjQI7wayV/wQ==tSXKKX4AYnBG5xYk'})
    
    # Check if the request was successful
    if response.status_code == requests.codes.ok:
        data = response.json()  # Convert response to JSON
        
        # Connect to S3
        s3 = boto3.client(
            's3',
            region_name='ap-south-1',
            aws_access_key_id="",
            aws_secret_access_key=""
        )
        
        # Convert JSON data to bytes
        json_data = json.dumps(data).encode('utf-8')
        
        # Upload JSON data to S3 bucket directly
        s3.put_object(Bucket='weather-api-country-data', Key='country_details.json', Body=json_data)
        print("JSON data saved to 'country_details.json' in S3 bucket")
    else:
        print("Error:", response.status_code, response.text)


def fetch_and_save_weather_data_to_s3():
    # Define country name and API endpoint
    country_name = "Nepal"
    token ="cb30e0dc15aa462246502f6d67a73bd8"
    api_url = f"https://api.openweathermap.org/data/2.5/weather?q={country_name}&appid={token}"

    # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        weather_data = response.json()  # Convert response to JSON
        
        # Connect to S3
        s3 = boto3.client(
            's3',
            region_name='ap-south-1',
            aws_access_key_id="",
            aws_secret_access_key=""
        )
        
        # Convert JSON data to bytes
        json_data_bytes = json.dumps(weather_data).encode('utf-8')
        
        # Upload JSON data to S3 bucket directly
        s3.put_object(Bucket='weather-api-country-data', Key='country_weather_details.json', Body=json_data_bytes)
        print("JSON data saved to 'country_weather_details.json' in S3 bucket")
    else:
        # Print an error message if the request was not successful
        print("Error:", response.status_code)

def read_email_data_s3():
    df = pd.read_csv('/home/bibek/Downloads/Emails.csv')

    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Connect to S3
    s3 = boto3.client(
        's3',
        region_name='ap-south-1',
        aws_access_key_id="",
        aws_secret_access_key=""
    )

    # Upload CSV data to S3 bucket
    s3.put_object(Bucket='weather-api-country-data', Key='email_details.csv', Body=csv_data.encode())

    print("CSV data saved to 'email_details.csv' in S3 bucket")



def transform_country_data_and_upload_to_s3():

    # Initialize AWS S3 client and resource
    s3 = boto3.client(
        's3',
        region_name='ap-south-1',
        aws_access_key_id='',
        aws_secret_access_key=''
    )
    # Retrieve JSON data from S3
    bucket_name = 'weather-api-country-data'
    json_key = 'country_details.json'

    json_obj = s3.get_object(Bucket=bucket_name, Key=json_key)
    json_data = json_obj['Body'].read().decode('utf-8')

    # Read JSON data from string
    try:
        # Load JSON data
        data = json.loads(json_data)

        # Initialize an empty DataFrame
        df = pd.DataFrame(columns=data[0].keys())

        # Append a new row to the DataFrame with the dictionary values
        for item in data:
            df = df.append(item, ignore_index=True)
            df = df[['gdp', 'sex_ratio', 'currency', 'employment_industry', 'urban_population_growth', 'population',
                     'gdp_growth', 'internet_users', 'gdp_per_capita']]
            # Convert DataFrame to CSV in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Upload CSV data to S3 bucket directly
            s3.put_object(Bucket='data-lake-for-weather-and-country', Key='t_country_details.csv',
                          Body=csv_buffer.getvalue())
            print("CSV data saved to 't_country_details.csv' in S3 bucket")

    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)


def transform_weather_data_and_upload_to_s3():


    # Initialize AWS S3 client and resource
    s3 = boto3.client(
        's3',
        region_name='ap-south-1',
        aws_access_key_id='',
        aws_secret_access_key=''
    )

    # Retrieve JSON data from S3
    bucket_name = 'weather-api-country-data'
    json_obj = s3.get_object(Bucket=bucket_name, Key='country_weather_details.json')
    json_data = json_obj['Body'].read().decode('utf-8')

    # Load JSON data as a dictionary
    data_dict = json.loads(json_data)

    # Initialize a DataFrame
    df = pd.DataFrame(columns=data_dict.keys())

    # Append data to the DataFrame
    df = df.append(data_dict, ignore_index=True)

    # Select specific columns and rename them
    selected_columns = df[['coord', 'visibility', 'wind', 'clouds', 'timezone', 'name']]
    selected_columns = selected_columns.rename(columns={'coord': 'coordinate', 'name': 'country_name'})

    # Rearrange the columns
    final_data = selected_columns[['country_name', 'coordinate', 'timezone', 'visibility', 'wind', 'clouds']]

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    final_data.to_csv(csv_buffer, index=False)

    # Upload CSV data to S3 bucket directly
    s3.put_object(Bucket='data-lake-for-weather-and-country', Key='t_country_weather_details.csv', Body=csv_buffer.getvalue())
    print("CSV data saved to 't_country_weather_details.csv' in S3 bucket")


def transform_email_data_and_upload_to_s3():


    # Initialize AWS S3 client and resource
    s3 = boto3.client(
        's3',
        region_name='ap-south-1',
        aws_access_key_id='',
        aws_secret_access_key=''
    )
    # Retrieve CSV data from S3
    bucket_name = 'weather-api-country-data'
    csv_key = 'email_details.csv'

    csv_obj = s3.get_object(Bucket=bucket_name, Key=csv_key)
    csv_data = csv_obj['Body'].read().decode('utf-8')

    # Load CSV data into a DataFrame
    df = pd.read_csv(StringIO(csv_data))

    # Filter out rows with missing gender values
    email_data = df.dropna(subset=["gender"])

    # Convert DataFrame to CSV in memory
    email_data_csv_buffer = StringIO()
    email_data.to_csv(email_data_csv_buffer, index=False)

    # Upload CSV data to S3 bucket directly
    s3.put_object(Bucket='data-lake-for-weather-and-country', Key='t_email_details.csv', Body=email_data_csv_buffer.getvalue())

    print("CSV data saved to 't_email_details.csv' in S3 bucket")






dag=DAG(
    'data_lake_dag',
    default_args=defaults_args,
    description="data lake etl code"
)

task1=PythonOperator(
    task_id='save_country_details_tk',
    python_callable=fetch_and_save_country_details_to_s3,
    dag=dag
)

task2=PythonOperator(
    task_id='save_weather_data_tk',
    python_callable=fetch_and_save_weather_data_to_s3,
    dag=dag
)


task3=PythonOperator(
    task_id='read_email_data_tk',
    python_callable=read_email_data_s3,
    dag=dag
)


task4=PythonOperator(
    task_id='transform_country_data_and_upload_to_s3_tk',
    python_callable=transform_country_data_and_upload_to_s3,
    dag=dag
)

task5=PythonOperator(
    task_id='transform_weather_data_and_upload_to_s3_tk',
    python_callable=transform_weather_data_and_upload_to_s3,
    dag=dag
)


task6=PythonOperator(
    task_id='transform_email_data_and_upload_to_s3_tk',
    python_callable=transform_email_data_and_upload_to_s3,
    dag=dag
)



task1 >> [task4]
task2 >> [task5]
task3 >> [task6]





