import boto3
from datetime import datetime
import json
import requests
from datetime import datetime

url_year = datetime.now().year-1

def fetch_currency_data():
    url = f"https://api.frankfurter.dev/v1/{url_year}-01-01.."
    response = requests.get(url)
    data = response.json()
    return data

currency_data = fetch_currency_data()

# Add your AWS credentials here

# aws_access_key_id = ''
# aws_secret_access_key = ''
# region_name = ''

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

def upload_data_to_s3(data):
    bucket_name = 'frankfurter-currency-bucket'
    file_name = f'{url_year}-currency-data.json'
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print('Data uploaded successfully to S3')

upload_data_to_s3(currency_data)