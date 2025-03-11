from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

dag = DAG(
    's3_to_redshift',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

copy_to_redshift = S3ToRedshiftOperator(
    task_id='load_data',
    schema='public',
    table='my_table',
    s3_bucket='etl-source-bucket-225',
    s3_key='Prices/Boston/Boston-house-price-data.csv',
    copy_options=['CSV', 'IGNOREHEADER 1'],
    aws_conn_id='aws_default',
    redshift_conn_id='redshift_default',  # This should be the connection ID configured in Airflow, not the actual URL
    dag=dag
)

copy_to_redshift