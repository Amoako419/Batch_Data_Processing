from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from typing import List
import pandas as pd
import tempfile
import csv
import os
from airflow.providers.mysql.hooks.mysql import MySqlHook


@task
def extract_data_from_rds(mysql_conn_id: str, query: str):
    """
    Extract data from RDS MySQL database using a provided query.
    """
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    data = mysql_hook.get_pandas_df(query)  # Returns a Pandas DataFrame
    return data.to_dict(orient="records")  # Convert to list of dictionaries for easy transformation


@task
def transform_data(data: List[dict]) -> List[dict]:
    """
    Transform the extracted data to compute KPIs.
    """
    df = pd.DataFrame(data)

    # Compute Genre-Level KPIs
    genre_kpis = df.groupby("track_genre").agg(
        listen_count=("track_id", "count"),
        avg_track_duration_sec=("duration_ms", lambda x: x.mean() / 1000),
        popularity_index=("popularity", "mean")
    ).reset_index()
    
    genre_kpis['kpi_type'] = 'genre'  # Add KPI type indicator

    # Compute Hourly KPIs
    df["stream_hour"] = pd.to_datetime(df["created_at"]).dt.hour

    hourly_kpis = df.groupby("stream_hour").agg(
        unique_listeners=("user_id", "nunique"),
        top_artists=("artists", lambda x: x.mode()[0] if not x.mode().empty else None),
        track_diversity_index=("track_id", lambda x: x.nunique() / len(x))
    ).reset_index()
    
    hourly_kpis['kpi_type'] = 'hourly'  # Add KPI type indicator

    # Combine KPI data for loading
    transformed_data = pd.concat([genre_kpis, hourly_kpis], ignore_index=True)
    return transformed_data.to_dict(orient="records")


@task
def extract_transform_s3_data(s3_conn_id: str, bucket_name: str, key: str):
    """
    Extract data from S3, validate it exists, and transform it.
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    
    # Validate file exists
    if not s3_hook.check_for_key(key, bucket_name):
        raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket_name}.")
    
    # Get the list of files in the prefix
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=key)
    
    # Process each file
    all_data = []
    for file in files:
        if file.endswith('.csv'):  # Only process CSV files
            # Download file to temp location
            with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                s3_obj = s3_hook.get_key(file, bucket_name)
                temp_file.write(s3_obj.get()['Body'].read())
                temp_path = temp_file.name
            
            # Process the file
            try:
                df = pd.read_csv(temp_path)
                
                # Apply transformations specific to S3 data
                # Example: Calculate stream duration aggregates
                if 'duration_ms' in df.columns and 'user_id' in df.columns:
                    user_stream_stats = df.groupby('user_id').agg(
                        total_listen_time=('duration_ms', 'sum'),
                        avg_listen_duration=('duration_ms', 'mean'),
                        stream_count=('duration_ms', 'count')
                    ).reset_index()
                    
                    user_stream_stats['data_source'] = 's3'
                    all_data.extend(user_stream_stats.to_dict(orient='records'))
                
                # Add more transformations as needed
            
            finally:
                # Clean up the temp file
                os.unlink(temp_path)
    
    return all_data


@task
def load_data_to_redshift(redshift_conn_id: str, table: str, data: List[dict]):
    """
    Load transformed data into Amazon Redshift.
    """
    if not data:
        return
        
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        # Get column names from the first row
        fieldnames = data[0].keys()
        
        # Write to CSV
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        temp_file_path = temp_file.name
    
    try:
        # Create S3 Hook for temporary storage
        s3_hook = S3Hook(aws_conn_id=redshift_conn_id)
        temp_s3_key = f"temp/transformed_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        s3_hook.load_file(
            filename=temp_file_path,
            key=temp_s3_key,
            bucket_name="your-temp-bucket",  # Replace with your temporary S3 bucket
            replace=True
        )
        
        # Use COPY command to efficiently load data to Redshift
        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        copy_query = f"""
        COPY {table}
        FROM 's3://your-temp-bucket/{temp_s3_key}'
        IAM_ROLE 'arn:aws:iam::your-account-id:role/your-redshift-role'  -- Replace with your Redshift IAM role
        CSV
        IGNOREHEADER 1
        """
        redshift_hook.run(copy_query)
        
        # Clean up the temporary S3 file
        s3_hook.delete_objects(
            bucket="your-temp-bucket",
            keys=[temp_s3_key]
        )
        
    finally:
        # Clean up the temporary local file
        os.unlink(temp_file_path)


@dag(
    dag_id='rds_s3_to_redshift_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'owner': 'billy', 'retries': 2},
    description="Pipeline to move transformed data from Amazon RDS and S3 to Amazon Redshift with KPI computation.",
    tags=['rds', 's3', 'redshift']
)
def rds_s3_to_redshift_pipeline():
    """
    DAG to extract data from Amazon RDS and S3, transform it, and load only the transformed data into Amazon Redshift.
    """
    mysql_conn_id = "rds_conn"
    redshift_conn_id = "aws_redshift_conn"
    s3_conn_id = "aws_conn_default"
    rds_query = "SELECT u.*, s.* FROM users u JOIN songs s ON u.user_id = s.id"
    redshift_table_kpi = "kpi_results"
    redshift_table_streams = "stream_analytics"
    s3_bucket = "streaming-data-source-11"
    s3_key = "Batch_data/streams/"

    # RDS data pipeline
    extracted_data_rds = extract_data_from_rds(mysql_conn_id=mysql_conn_id, query=rds_query)
    transformed_data_rds = transform_data(data=extracted_data_rds)
    load_transformed_rds_data = load_data_to_redshift(
        redshift_conn_id=redshift_conn_id,
        table=redshift_table_kpi,
        data=transformed_data_rds
    )

    # S3 data pipeline
    transformed_data_s3 = extract_transform_s3_data(
        s3_conn_id=s3_conn_id,
        bucket_name=s3_bucket,
        key=s3_key
    )
    load_transformed_s3_data = load_data_to_redshift(
        redshift_conn_id=redshift_conn_id,
        table=redshift_table_streams,
        data=transformed_data_s3
    )

    # Define task dependencies
    extracted_data_rds >> transformed_data_rds >> load_transformed_rds_data
    transformed_data_s3 >> load_transformed_s3_data


# This line is crucial for Airflow to detect the DAG
dag_instance = rds_s3_to_redshift_pipeline()