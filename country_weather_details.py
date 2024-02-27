import requests
import json
import env_variables
import boto3

def json_data():
    country_name = "Nepal"
    token = f"{env_variables.token}"
    # Define the API endpoint
    api_url = f"https://api.openweathermap.org/data/2.5/weather?q={country_name}&appid={token}"

    # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Return the JSON data directly
        return response.json()
    else:
        # Print an error message if the request was not successful
        print("Error:", response.status_code)
        # Return None if there's an error
        return None




def save_weather_data_s3():
    weather_data = json_data()
    # Check if data is not None (i.e., the request was successful)
    if weather_data:
        # Connect to S3
        s3 = boto3.client(
            's3',
            region_name='ap-south-1',
            aws_access_key_id=f"{env_variables.aws_access_key_id}",
            aws_secret_access_key=f"{env_variables.aws_secret_access_key}"
        )
        
        # Convert JSON data to bytes
        json_data_bytes = json.dumps(weather_data).encode('utf-8')
        
        # Upload JSON data to S3 bucket directly
        s3.put_object(Bucket='weather-api-country-data', Key='country_weather_details.json', Body=json_data_bytes)
        print("JSON data saved to 'country_weather_details.json' in S3 bucket")


