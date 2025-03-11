from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from typing import List
from airflow.providers.mysql.hooks.mysql import MySqlHook


@task
def extract_data_from_rds(mysql_conn_id: str, query: str):
    """
    Extract data from RDS MySQL database using a provided query.
    """
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    data = mysql_hook.get_records(query)
    return data


@task
def transform_data(data: List[tuple]) -> List[tuple]:
    """
    Transform the extracted data if necessary.
    For this example, we assume no transformation is needed.
    """
    # Placeholder for transformation logic
    transformed_data = data  # No transformation applied
    return transformed_data


@task
def load_data_to_redshift_from_rds(redshift_conn_id: str, table: str, data: List[tuple]):
    """
    Load transformed data from RDS into Amazon Redshift.
    """
    redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    
    # Insert data into Redshift table
    for row in data:
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
        cursor.execute(insert_query, row)
    
    connection.commit()
    cursor.close()
    connection.close()


@task
def validate_s3_file(s3_conn_id: str, bucket_name: str, key: str) -> bool:
    """
    Validate the existence of the file in S3.
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    if not s3_hook.check_for_key(key, bucket_name):
        raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket_name}.")
    return True


@dag(
    dag_id='rds_s3_to_redshift_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'owner': 'billy', 'retries': 2},
    description="Pipeline to move data from Amazon RDS and S3 to Amazon Redshift with error handling and logging.",
    tags=['rds', 's3', 'redshift']
)
def rds_s3_to_redshift_pipeline():
    """
    DAG to extract data from Amazon RDS and S3, transform it, and load it into Amazon Redshift.
    """
    # Define variables for connections and queries
    mysql_conn_id = "rds_conn"
    redshift_conn_id = "aws_redshift_conn"
    s3_conn_id = "aws_conn_default"
    rds_query = "SELECT rdsdb.u.*, rdsdb.s.* FROM users u JOIN songs s ON u.user_id = s.id"
    redshift_table_rds = "user_songs"
    redshift_table_s3 = "streams"
    s3_bucket = "streaming-data-source-11"
    s3_key = "Batch_data/streams/"
    
    # Extract data from RDS
    extracted_data_rds = extract_data_from_rds(
        mysql_conn_id=mysql_conn_id,
        query=rds_query
    )
    
    # Transform data from RDS
    transformed_data_rds = transform_data(
        data=extracted_data_rds
    )
    
    # Load data from RDS into Redshift
    load_rds_to_redshift = load_data_to_redshift_from_rds(
        redshift_conn_id=redshift_conn_id,
        table=redshift_table_rds,
        data=transformed_data_rds
    )
    
    # Validate the existence of the file in S3
    s3_file_valid = validate_s3_file(
        s3_conn_id=s3_conn_id,
        bucket_name=s3_bucket,
        key=s3_key
    )
    
    # Load data from S3 into Redshift
    load_data_from_s3_to_redshift = S3ToRedshiftOperator(
        task_id="load_data_from_s3_to_redshift",
        redshift_conn_id=redshift_conn_id,
        aws_conn_id=s3_conn_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        schema="PUBLIC",
        table=redshift_table_s3,
        copy_options=["CSV", "IGNOREHEADER 1"],
    )
    
    # Task dependencies
    extracted_data_rds >> transformed_data_rds >> load_rds_to_redshift
    s3_file_valid >> load_data_from_s3_to_redshift


# Instantiate the DAG
rds_s3_to_redshift_pipeline()