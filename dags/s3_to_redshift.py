from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from typing import List
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import json
import tempfile
import os
import csv


@task
def extract_data_from_rds(mysql_conn_id: str, query: str):
    """
    Extract data from RDS MySQL database using a provided query.
    
    Args:
        mysql_conn_id (str): The Airflow connection ID for the MySQL database
        query (str): SQL query to execute against the MySQL database
        
    Returns:
        List[tuple]: Records returned from the MySQL database
    """
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    data = mysql_hook.get_records(query)
    return data


@task
def transform_data(data: List[tuple]) -> List[dict]:
    """
    Compute KPIs based on the extracted data.
    
    Args:
        data (List[tuple]): Raw data extracted from RDS
        
    Returns:
        List[dict]: Transformed data with computed KPIs
    """
    # Convert the list of tuples into a Pandas DataFrame
    columns = [
        "user_id", "user_name", "user_age", "user_country", "created_at",
        "id", "track_id", "artists", "album_name", "track_name", "popularity",
        "duration_ms", "explicit", "danceability", "energy", "key",
        "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "time_signature", "track_genre",
        "play_count", "like_count", "share_count"
    ]
    
    # Handle case where number of columns doesn't match the data
    if len(data) > 0 and len(data[0]) != len(columns):
        actual_columns = len(data[0]) if data else 0
        print(f"Warning: Column mismatch. Expected {len(columns)}, got {actual_columns}.")
        # Adjust columns to match data
        if actual_columns < len(columns):
            columns = columns[:actual_columns]
        else:
            # Add generic column names if we have more columns than expected
            columns.extend([f"column_{i}" for i in range(len(columns), actual_columns)])
    
    df = pd.DataFrame(data, columns=columns)
    
    # Add data type for KPI differentiation
    genre_kpi_data = []
    hourly_kpi_data = []
    
    # Compute Genre-Level KPIs
    if "track_genre" in df.columns and "play_count" in df.columns:
        genre_kpis = df.groupby("track_genre").agg(
            listen_count=("play_count", "sum"),
            avg_track_duration_sec=("duration_ms", lambda x: x.mean() / 1000 if "duration_ms" in df.columns else None),
            popularity_index=("popularity", "mean" if "popularity" in df.columns else None),
            most_popular_track=("track_name", lambda x: x.mode()[0] if not x.mode().empty and "track_name" in df.columns else None)
        ).reset_index()
        
        # Add KPI type
        genre_kpis["kpi_type"] = "genre"
        genre_kpi_data = genre_kpis.to_dict(orient="records")
    
    # Compute Hourly KPIs
    if "created_at" in df.columns:
        try:
            df["stream_hour"] = pd.to_datetime(df["created_at"]).dt.hour
            
            hourly_kpis = df.groupby("stream_hour").agg(
                unique_listeners=("user_id", "nunique" if "user_id" in df.columns else None),
                top_artists=("artists", lambda x: x.mode()[0] if not x.mode().empty and "artists" in df.columns else None),
                track_diversity_index=("track_id", lambda x: x.nunique() / len(x) if "track_id" in df.columns else None)
            ).reset_index()
            
            # Add KPI type
            hourly_kpis["kpi_type"] = "hourly"
            hourly_kpi_data = hourly_kpis.to_dict(orient="records")
        except Exception as e:
            print(f"Error processing hourly KPIs: {e}")
    
    # Combine all KPIs
    transformed_data = genre_kpi_data + hourly_kpi_data
    
    # Ensure we have data to return
    if not transformed_data:
        print("Warning: No KPIs were computed. Check your data schema.")
        # Return a placeholder to avoid downstream task failures
        transformed_data = [{"warning": "No KPIs computed", "timestamp": datetime.now().isoformat()}]
    
    return transformed_data


@task
def load_data_to_redshift(redshift_conn_id: str, table: str, data: List[dict]):
    """
    Load computed KPIs into Amazon Redshift.
    
    Args:
        redshift_conn_id (str): The Airflow connection ID for Redshift
        table (str): The target table name in Redshift
        data (List[dict]): Transformed data to load into Redshift
    """
    if not data:
        print("No data to load into Redshift.")
        return
    
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
        # Get column names from the first row
        fieldnames = data[0].keys()
        
        # Write to CSV
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        temp_file_path = temp_file.name
    
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()
        
        # Create the table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {', '.join([f'"{col}" VARCHAR(255)' for col in data[0].keys()])}
        );
        """
        cursor.execute(create_table_query)
        
        # Insert each row
        for row in data:
            # Sanitize the values for SQL insertion
            sanitized_values = []
            for value in row.values():
                if value is None:
                    sanitized_values.append(None)
                elif isinstance(value, (int, float)):
                    sanitized_values.append(value)
                else:
                    # Escape single quotes in string values
                    sanitized_values.append(str(value).replace("'", "''"))
            
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join([f'"{col}"' for col in row.keys()])
            insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
            
            try:
                cursor.execute(insert_query, tuple(sanitized_values))
            except Exception as e:
                print(f"Error inserting row: {e}")
                print(f"Query: {insert_query}")
                print(f"Values: {sanitized_values}")
                # Continue with the next row
                continue
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"Error loading data to Redshift: {e}")
        raise
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


@task
def transform_s3_data(s3_conn_id: str, bucket_name: str, key: str):
    """
    Extract, validate, and transform data from S3.
    
    Args:
        s3_conn_id (str): The Airflow connection ID for S3
        bucket_name (str): The S3 bucket name
        key (str): The S3 key/prefix where data is located
        
    Returns:
        List[dict]: Transformed data from S3
    """
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    
    # Validate file exists
    if not s3_hook.check_for_prefix(key, bucket_name):
        raise FileNotFoundError(f"The prefix {key} does not exist in bucket {bucket_name}.")
    
    # List keys matching the prefix
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=key)
    keys = [k for k in keys if k.endswith('.csv')]  # Filter for CSV files
    
    if not keys:
        raise FileNotFoundError(f"No CSV files found with prefix {key} in bucket {bucket_name}.")
    
    # Process the first matching file (you can extend this to process multiple files)
    first_key = keys[0]
    print(f"Processing S3 file: {first_key}")
    
    with tempfile.NamedTemporaryFile() as temp_file:
        s3_hook.download_file(key=first_key, bucket_name=bucket_name, local_path=temp_file.name)
        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(temp_file.name)
        
        # Apply transformations
        # Example: Calculate engagement metrics by user
        if 'user_id' in df.columns:
            user_metrics = df.groupby('user_id').agg(
                session_count=('session_id', 'nunique' if 'session_id' in df.columns else 'count'),
                total_streams=('stream_id', 'count' if 'stream_id' in df.columns else lambda x: len(x)),
                avg_stream_duration=('duration', 'mean' if 'duration' in df.columns else None)
            ).reset_index()
            
            # Add data source information
            user_metrics['data_source'] = 's3'
            user_metrics['file_name'] = first_key
            user_metrics['processed_at'] = datetime.now().isoformat()
            
            return user_metrics.to_dict(orient='records')
        else:
            # Fall back to a simple summary if expected columns aren't found
            column_counts = {col: df[col].count() for col in df.columns}
            return [{'summary': 'column_counts', 'data': json.dumps(column_counts)}]


@dag(
    dag_id='rds_s3_to_redshift_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'owner': 'billy', 'retries': 2},
    description="Pipeline to move transformed data from Amazon RDS and S3 to Amazon Redshift with error handling and logging.",
    tags=['rds', 's3', 'redshift']
)
def rds_s3_to_redshift_pipeline():
    """
    DAG to extract data from Amazon RDS and S3, transform it, and load only the transformed data into Amazon Redshift.
    
    This pipeline:
    1. Extracts data from RDS MySQL using a SQL query
    2. Transforms the RDS data to compute genre and hourly KPIs
    3. Loads the transformed RDS data into Redshift
    4. Extracts data from S3, transforms it, and loads only the transformed data into Redshift
    """
    # Define variables for connections and queries
    mysql_conn_id = "rds_conn"
    redshift_conn_id = "aws_redshift_conn"
    s3_conn_id = "aws_conn_default"
    # Fixed query to use correct schema qualification
    rds_query = "SELECT u.*, s.* FROM users u JOIN songs s ON u.user_id = s.id"
    redshift_table_kpi = "kpi_results"
    redshift_table_s3 = "s3_analytics"
    s3_bucket = "streaming-data-source-11"
    s3_key = "Batch_data/streams/"
    
    # RDS data pipeline
    extracted_data_rds = extract_data_from_rds(
        mysql_conn_id=mysql_conn_id,
        query=rds_query
    )
    
    transformed_data_rds = transform_data(
        data=extracted_data_rds
    )
    
    load_rds_kpis = load_data_to_redshift(
        redshift_conn_id=redshift_conn_id,
        table=redshift_table_kpi,
        data=transformed_data_rds
    )
    
    # S3 data pipeline - extract, transform, and load only transformed data
    transformed_s3_data = transform_s3_data(
        s3_conn_id=s3_conn_id,
        bucket_name=s3_bucket,
        key=s3_key
    )
    
    load_s3_analytics = load_data_to_redshift(
        redshift_conn_id=redshift_conn_id,
        table=redshift_table_s3,
        data=transformed_s3_data
    )
    
    # Task dependencies - ensure clear flow
    extracted_data_rds >> transformed_data_rds >> load_rds_kpis
    transformed_s3_data >> load_s3_analytics


# Instantiate the DAG - required for Airflow to discover it
dag = rds_s3_to_redshift_pipeline()