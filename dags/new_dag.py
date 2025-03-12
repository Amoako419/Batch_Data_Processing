from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
from typing import List
import pandas as pd
import tempfile
import os
import io

@dag(
    dag_id='etl_rds_s3_to_redshift_pipeline',
    start_date=datetime(2025, 3, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'owner': 'billy', 'retries': 3},
    description="ETL Pipeline that extracts data from S3 and RDS, applies transformation, and loads into Redshift",
    tags=['rds', 's3', 'redshift']
)
def rds_s3_to_redshift_pipeline():

    @task
    def extract_data_from_rds(mysql_conn_id: str, query: str) -> List[dict]:
        """
        Extract data from RDS MySQL database and convert datetime columns to strings.
        """
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        df = mysql_hook.get_pandas_df(query)

        # Convert all datetime columns to string to avoid serialization errors
        for col in df.select_dtypes(include=["datetime64", "datetime", "timedelta"]):
            df[col] = df[col].astype(str)

        return df.to_dict(orient="records")


    @task
    def validate_s3_data(s3_conn_id: str, bucket_name: str, key: str) -> List[str]:
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=key)
        csv_keys = [k for k in keys if k.endswith('.csv')]

        if not csv_keys:
            raise FileNotFoundError(f"No CSV files found at {key} in bucket {bucket_name}.")

        print(f"Found {len(csv_keys)} CSV files in {bucket_name}/{key}.")
        return csv_keys

    @task
    def extract_s3_data(s3_conn_id: str, bucket_name: str, keys: List[str]) -> List[dict]:
        """
        Extract data from S3 directly into a DataFrame without using temp files.

        Args:
            s3_conn_id (str): The Airflow connection ID for S3
            bucket_name (str): The S3 bucket name
            keys (List[str]): List of CSV file keys to process

        Returns:
            List[dict]: Extracted and concatenated data from all CSVs
        """
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        dataframes = []

        for key in keys:
            try:
                # Read file directly from S3 into memory
                s3_object = s3_hook.get_key(key, bucket_name)
                csv_data = s3_object.get()["Body"].read().decode("utf-8")

                # Load CSV into DataFrame
                df = pd.read_csv(io.StringIO(csv_data))

                # Ensure necessary columns exist
                expected_columns = {"user_id", "track_id", "listen_time"}
                missing_columns = expected_columns - set(df.columns)
                if missing_columns:
                    raise ValueError(f"Missing columns in {key}: {missing_columns}")

                # Convert listen_time to datetime string for JSON serialization
                df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce").astype(str)

                dataframes.append(df)
            except Exception as e:
                print(f" Error processing {key}: {e}")

        # Concatenate all DataFrames
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            print(f"Extracted {len(combined_df)} records from S3.")
            return combined_df.to_dict(orient="records")
        else:
            print("⚠️ No valid data extracted from S3.")
            return []



    @task
    def transform_and_compute_kpis(rds_data: List[dict], s3_data: List[dict]) -> List[dict]:
        """
        Merge S3 & RDS data, align columns, compute KPIs, and prepare for Redshift loading.

        Args:
            rds_data (List[dict]): Extracted data from RDS
            s3_data (List[dict]): Extracted data from S3

        Returns:
            List[dict]: Computed KPIs
        """
        # Convert both datasets into DataFrames
        df_rds = pd.DataFrame(rds_data)
        df_s3 = pd.DataFrame(s3_data)

        print(f"🟢 Initial RDS Columns: {df_rds.columns.tolist()}")
        print(f"🟢 Initial S3 Columns: {df_s3.columns.tolist()}")

        # 🔹 Fix: Remove duplicate 'track_id' column before renaming
        if "id" in df_rds.columns and "track_id" in df_rds.columns:
            df_rds.drop(columns=["id"], inplace=True)
        
        # 🔹 Rename 'id' to 'track_id' if needed
        elif "id" in df_rds.columns:
            df_rds.rename(columns={"id": "track_id"}, inplace=True)

        print(f"RDS Columns After Fixing 'track_id' Issue: {df_rds.columns.tolist()}")

        # 🔹 Ensure 'created_at' exists in S3 Data
        if "listen_time" in df_s3.columns:
            df_s3.rename(columns={"listen_time": "created_at"}, inplace=True)

        # 🔹 Add missing columns to RDS before selecting
        for col in ["play_count", "like_count", "share_count"]:
            if col not in df_rds.columns:
                df_rds[col] = 0  # Default to 0

        print(f"Updated RDS Columns (After Adding Missing Fields): {df_rds.columns.tolist()}")

        # 🔹 Add missing columns to S3 before merging
        for col in ["play_count", "like_count", "share_count", "track_genre", "track_name", "artists", "popularity", "duration_ms"]:
            if col not in df_s3.columns:
                df_s3[col] = 0 if col in ["play_count", "like_count", "share_count", "popularity", "duration_ms"] else "Unknown"

        print(f"Updated S3 Columns (After Adding Missing Fields): {df_s3.columns.tolist()}")

        # 🔹 Select only relevant columns for merging
        s3_columns_needed = list({"user_id", "track_id", "created_at"} | {"track_genre", "duration_ms", "popularity", "track_name", "artists", "play_count", "like_count", "share_count"})
        rds_columns_needed = list({"user_id", "track_id", "created_at"} | {"track_genre", "duration_ms", "popularity", "track_name", "artists", "play_count", "like_count", "share_count"})

        df_s3 = df_s3[s3_columns_needed]
        df_rds = df_rds[rds_columns_needed]

        # 🔹 Merge datasets (left join to retain RDS metadata, but keep S3 records too)
        combined_df = df_rds.merge(df_s3, on=["user_id", "track_id", "created_at"], how="outer")

        print(f"🟢 Final Merged Columns: {combined_df.columns.tolist()}")

        # 🔹 Ensure missing columns exist after merging
        for col in ["play_count", "like_count", "share_count", "duration_ms"]:
            if col not in combined_df.columns:
                combined_df[col] = 0  # Default missing numeric columns to 0

        # 🔹 Fill missing values in track metadata
        combined_df.fillna({
            "track_genre": "Unknown",
            "track_name": "Unknown",
            "artists": "Unknown",
            "play_count": 0,
            "like_count": 0,
            "share_count": 0,
            "popularity": 0,
            "duration_ms": 0
        }, inplace=True)

        # 🔹 Compute Genre-Level KPIs
        genre_kpis = combined_df.groupby("track_genre").agg(
            listen_count=("track_id", "count"),
            avg_track_duration_sec=("duration_ms", lambda x: x.mean() / 1000),
            popularity_index=("popularity", "mean"),
            most_popular_track=("track_name", lambda x: x.mode()[0] if not x.mode().empty else None)
        ).reset_index()

        # 🔹 Compute Hourly KPIs
        combined_df["stream_hour"] = pd.to_datetime(combined_df["created_at"], errors="coerce").dt.hour

        hourly_kpis = combined_df.groupby("stream_hour").agg(
            unique_listeners=("user_id", "nunique"),
            top_artists=("artists", lambda x: x.mode()[0] if not x.mode().empty else None),
            track_diversity_index=("track_id", lambda x: x.nunique() / len(x))
        ).reset_index()

        # 🔹 Merge KPI data for Redshift
        transformed_data = genre_kpis.to_dict(orient="records") + hourly_kpis.to_dict(orient="records")

        print(f"✅ Computed {len(transformed_data)} KPI records.")
        return transformed_data



    @task
    def create_kpi_table_in_redshift(redshift_conn_id: str, table: str):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            track_genre VARCHAR(255),
            listen_count INT,
            avg_track_duration_sec FLOAT,
            popularity_index FLOAT,
            most_popular_track VARCHAR(255),
            stream_hour INT,
            unique_listeners INT,
            top_artists VARCHAR(255),
            track_diversity_index FLOAT,
            kpi_type VARCHAR(50)
        );
        """
        
        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        redshift_hook.run(create_table_query)
        print(f"✅ Table {table} is ready in Redshift.")

    @task
    def load_kpis_to_redshift(redshift_conn_id: str, table: str, data: List[dict]):
        if not data:
            print("❌ No data to load into Redshift.")
            return

        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()

        columns = data[0].keys()
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        records = [tuple(row.values()) for row in data]

        try:
            cursor.executemany(insert_query, records)
            connection.commit()
            print(f"✅ Successfully loaded {len(data)} KPI records into {table}.")
        except Exception as e:
            connection.rollback()
            print(f"❌ Error inserting data into Redshift: {e}")
        finally:
            cursor.close()
            connection.close()

    # Define pipeline
    mysql_conn_id = "rds_conn"
    redshift_conn_id = "aws_redshift_conn"
    s3_conn_id = "aws_conn_default"
    rds_query = "SELECT u.*, s.* FROM users u JOIN songs s ON u.user_id = s.id"
    redshift_table_kpi = "kpi_results"
    s3_bucket = "streaming-data-source-11"
    s3_key = "Batch_data/streams/"

    extracted_data_rds = extract_data_from_rds(mysql_conn_id, rds_query)
    validated_s3_files = validate_s3_data(s3_conn_id, s3_bucket, s3_key)
    extracted_data_s3 = extract_s3_data(s3_conn_id, s3_bucket, validated_s3_files)

    transformed_data = transform_and_compute_kpis(extracted_data_rds, extracted_data_s3)
    create_kpi_table = create_kpi_table_in_redshift(redshift_conn_id, redshift_table_kpi)
    
    create_kpi_table >> transformed_data >> load_kpis_to_redshift(redshift_conn_id, redshift_table_kpi, transformed_data)


rds_s3_to_redshift_pipeline()
