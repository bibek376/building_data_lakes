import requests
import boto3
import json
import env_variables



def country_details():
    country_name = "Nepal"
    api_url = f'https://api.api-ninjas.com/v1/country?name={country_name}'
    response = requests.get(api_url, headers={'X-Api-Key': 'dCc4gXhgBdcjQI7wayV/wQ==tSXKKX4AYnBG5xYk'})
    if response.status_code == requests.codes.ok:
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None


def save_country_details_s3():

    data = country_details()

    # Check if data is not None (i.e., the request was successful)
    if data:
        # Connect to S3
        s3 = boto3.client(
            's3',
            region_name='ap-south-1',
            aws_access_key_id=f"{env_variables.aws_access_key_id}",
            aws_secret_access_key=f"{env_variables.aws_secret_access_key}"
        )
        
        # Convert JSON data to bytes
        json_data = json.dumps(data).encode('utf-8')
        
        # Upload JSON data to S3 bucket directly
        s3.put_object(Bucket='weather-api-country-data', Key='country_details.json', Body=json_data)
        print("JSON data saved to 'country_details.json' in S3 bucket")

