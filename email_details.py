import pandas as pd
import env_variables
import boto3
from io import StringIO

def read_email_data():
    df = pd.read_csv('/home/ubuntu/Emails.csv')

    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Connect to S3
    s3 = boto3.client(
        's3',
        region_name='ap-south-1',
        aws_access_key_id=env_variables.aws_access_key_id,
        aws_secret_access_key=env_variables.aws_secret_access_key
    )

    # Upload CSV data to S3 bucket
    s3.put_object(Bucket='weather-api-country-data', Key='email_details.csv', Body=csv_data.encode())

    print("CSV data saved to 'email_details.csv' in S3 bucket")

read_email_data()
