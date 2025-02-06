import io
import requests
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import pendulum
from datetime import datetime
from airflow.exceptions import AirflowFailException
import pandas as pd
import psycopg2
import traceback
import logging

# Add your AWS credentials here

# aws_access_key_id = ''
# aws_secret_access_key = ''
# region_name = ''
# bucket_name = ''

url_year = datetime.now().year - 1

# Add your Postgres credentials here

# postgres_host = ''
# postgres_database = ''
# postgres_user = ''
# postgres_password = ''
# postgres_port = ''


default_args = {
    'owner': 'malavika',
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['@gmail.com'],
}

dag = DAG(
    dag_id='currency_data_etl_Pipeline_FINAL_TEST_S3_DataUpload',
    default_args=default_args,
    description='ETL api extraction and upload to S3',
    schedule='@daily',
    catchup=False,
)

s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)


def fetch_currency_data():
    url = f"https://api.frankfurter.dev/v1/{url_year}-01-01.."
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Currency data fetched successfully")
        return data
    except requests.exceptions.RequestException as e:
        raise AirflowFailException(f"Error fetching data from API: {e}")
    
def dataPreprocessing(data):
    df = pd.DataFrame(data)
    dfValues = df
    dfValues['currency'] = df['rates'].apply(lambda x : x.keys())
    dfValues['rates'] = df['rates'].apply(lambda x : x.values())
    dfValues = dfValues.drop(['start_date', 'end_date'],axis=1)
    dfValues = dfValues.explode(['rates','currency'])
    dfValues.reset_index(inplace=True)
    dfValues.rename(columns={'index':'DATE'},inplace=True)
    return dfValues


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    raise AirflowFailException(f"Couldn't create the Postgres connection: {e}")

def create_currency_table():
    try:
        cur.execute("DROP TABLE IF EXISTS currency_data_table")
        cur.execute("""CREATE TABLE IF NOT EXISTS currency_data_table(Date_Extracted DATE, Amount int, Base varchar(20), Rates float, Currency varchar(20))""")
        conn.commit()
        logging.info("Table created successfully in Postgres.")
    except Exception as e:
        traceback.print_exc()
        raise AirflowFailException(f"Error creating table: {e}")

def insert_currency_data(data):
    try:
        query = "INSERT INTO currency_data_table(Date_Extracted, Amount, Base, Rates, Currency) VALUES (%s,%s,%s,%s,%s)"
        row_count = 0
        for _,row in data.iterrows():
            values = (row['DATE'],row['amount'],row['base'],row['rates'],row['currency'])
            cur.execute(query,values)
            conn.commit()
            row_count +=1
        logging.info(f"{row_count} rows inserted into table currency_data_table")
        return row_count
    except Exception as e:
        raise AirflowFailException(f"Error inserting values into table: {e}")


def upload_data_to_s3(data):
    try:
        with io.BytesIO() as xlsx_buffer:
            data.to_excel(xlsx_buffer,index=False)
            xlsx_buffer.seek(0)

            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{url_year}-currency-data.xlsx",
                Body=xlsx_buffer.getvalue()
            )
    except Exception as e:
        raise AirflowFailException(f"Error uploading data to S3: {e}")





def etl_task(**kwargs):
    try:
        data = fetch_currency_data()
        currency_DF = dataPreprocessing(data)
        create_currency_table()
        row_count = insert_currency_data(currency_DF)
        upload_data_to_s3(currency_DF)
        subject = f"Currency Data Load Report - {datetime.now().strftime('%Y-%m-%d')}"
        body = f"Data load status: Success \n\n Postgres Upload: {row_count} rows inserted into table currency_data_table \n\n File uploaded: {url_year}-currency-data.xlsx"
    except Exception as e:
        subject = f"Currency Data Load Report - {datetime.now().strftime('%Y-%m-%d')}"
        body = f"Data load status: \n\nFailed: {str(e)} \n\nFile uploaded: {url_year}-currency-data.xlsx"
        raise AirflowFailException(f"ETL process failed: {e}")
    
    send_email = EmailOperator(
        task_id='send_email',
        to='@gmail.com',
        subject=subject,
        html_content=body,
        dag=dag,
    )
    send_email.execute(context=kwargs)


etl_operator = PythonOperator(
    task_id='etl_task',
    python_callable=etl_task,
    dag=dag,
)

if __name__ == "__main__":
    dag.test()

