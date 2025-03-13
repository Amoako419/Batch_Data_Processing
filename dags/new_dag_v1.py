import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from typing import List
import pandas as pd
import io
from airflow.providers.postgres.hooks.postgres import PostgresHook

begin = EmptyOperator(task_id="begin")
end = EmptyOperator(task_id="end")

@dag(
    dag_id='etl_rds_s3_to_redshift_pipeline_v1',
    start_date=datetime(2025, 3, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'owner': 'billy', 'retries': 3},
    description="ETL Pipeline that extracts data from S3 and RDS, applies transformation, and loads into Redshift",
    tags=['rds', 's3', 'redshift']
)
def rds_s3_to_redshift_pipeline():
    @task
    def extract_data_from_rds(postgres_conn_id: str, query: str) -> List[dict]:
        """
        Extract data from RDS PostgreSQL database and convert datetime columns to strings.

        Args:
            postgres_conn_id (str): The Airflow connection ID for PostgreSQL.
            query (str): The SQL query to execute.

        Returns:
            List[dict]: Extracted data as a list of dictionaries.
        """
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        df = postgres_hook.get_pandas_df(query)

        # Convert all datetime columns to string to avoid serialization issues
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
    def validate_s3_data_columns(s3_conn_id: str, bucket_name: str, keys: List[str]) -> List[str]:
        """
        Validate the columns in the S3 data to ensure they match the expected schema.

        Args:
            s3_conn_id (str): The Airflow connection ID for S3
            bucket_name (str): The S3 bucket name
            keys (List[str]): List of CSV file keys to process

        Returns:
            List[str]: List of valid CSV file keys
        """
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        valid_keys = []

        expected_columns = {"user_id", "track_id", "listen_time"}

        for key in keys:
            try:
                # Read file directly from S3 into memory
                s3_object = s3_hook.get_key(key, bucket_name)
                csv_data = s3_object.get()["Body"].read().decode("utf-8")

                # Load CSV into DataFrame
                df = pd.read_csv(io.StringIO(csv_data))

                # Check for missing columns
                missing_columns = expected_columns - set(df.columns)
                if missing_columns:
                    raise ValueError(f"Missing columns in {key}: {missing_columns}")

                valid_keys.append(key)

            except Exception as e:
                print(f"Error processing {key}: {e}")

        if not valid_keys:
            print("No valid S3 files found. DAG execution will stop.")
            raise ValueError("No valid S3 files found. DAG stopping.")  # This triggers `EmptyOperator`

        print(f"Found {len(valid_keys)} valid CSV files in {bucket_name}.")
        return valid_keys

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
                print("No valid data extracted from S3.")
                return []


    @task
    def transform_and_compute_kpis(user_songs: List[dict], streams: List[dict]) -> List[dict]:
        """
        Compute Daily KPIs from merged PostgreSQL (RDS) and S3 data.

        Args:
            user_songs (List[dict]): Extracted data from RDS.
            streams (List[dict]): Extracted data from S3.

        Returns:
            List[dict]: Computed KPIs per day.
        """
        import pandas as pd

        # Convert lists to DataFrames
        df_rds = pd.DataFrame(user_songs)
        df_s3 = pd.DataFrame(streams)

        # Ensure timestamps are converted to date format
        df_rds["created_at"] = pd.to_datetime(df_rds["created_at"]).dt.date
        df_s3["listen_time"] = pd.to_datetime(df_s3["listen_time"]).dt.date

        # Rename 'listen_time' to 'created_at' in S3 to align with RDS
        df_s3.rename(columns={"listen_time": "created_at"}, inplace=True)

        # Merge datasets (outer join to retain all data)
        merged_df = pd.merge(df_rds, df_s3, on=["user_id", "track_id", "created_at"], how="outer")

        # Compute KPIs
        kpis = merged_df.groupby("created_at").agg(
            unique_tracks_count=("track_id", "nunique"),  # Unique tracks played
            avg_track_duration=("duration_ms", lambda x: x.mean() / 60000),  # Convert ms to minutes
            popularity_index=("popularity", "mean"),  # Average popularity score
            unique_listeners=("user_id", "nunique"),  # Unique users per day
            track_diversity_index=("track_id", lambda x: x.nunique() / x.count()),  # Diversity index
        ).reset_index()

        # Find most popular track per day
        popular_tracks = merged_df.loc[merged_df.groupby("created_at")["popularity"].idxmax()][["created_at", "track_name"]]
        kpis = kpis.merge(popular_tracks, on="created_at", how="left").rename(columns={"track_name": "most_popular_track"})

        # Find top artist per day
        top_artists = merged_df.groupby(["created_at"])["artists"].value_counts().groupby("created_at").head(1).reset_index()
        kpis = kpis.merge(top_artists[["created_at", "artists"]], on="created_at", how="left").rename(columns={"artists": "top_artist"})

        # Add KPI Type Column
        kpis["kpi_type"] = "daily"

        # Convert to List of Dictionaries
        transformed_data = kpis.to_dict(orient="records")

        print(f"Computed {len(transformed_data)} KPI records.")
        return transformed_data


        
    @task
    def create_redshift_table(redshift_conn_id: str, table: str):
        """
        Create the KPI table in Amazon Redshift if it doesn't exist.

        Args:
            redshift_conn_id (str): The Airflow connection ID for Redshift.
            table (str): The name of the table to create.
        """
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            created_at DATE PRIMARY KEY,  
            unique_tracks_count INT,
            avg_track_duration FLOAT,  
            popularity_index FLOAT,  
            most_popular_track VARCHAR(255),  
            unique_listeners INT,  
            top_artist VARCHAR(255),
            track_diversity_index FLOAT,  
            kpi_type VARCHAR(50)  
        );
        """

        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        redshift_hook.run(create_table_query)
        print(f"Table `{table}` is ready in Redshift.")



    @task
    def load_data_to_redshift(redshift_conn_id: str, table: str, data: List[dict]):
        """
        Load computed KPI data into Amazon Redshift.

        Args:
            redshift_conn_id (str): The Airflow connection ID for Redshift.
            table (str): The name of the table in Redshift.
            data (List[dict]): The transformed KPI data to load.
        """
        if not data:
            print("No data to load into Redshift.")
            return

        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()

        # Define expected columns
        columns = [
            "created_at", "unique_tracks_count", "avg_track_duration", "popularity_index",
            "most_popular_track", "unique_listeners", "top_artist", "track_diversity_index", "kpi_type"
        ]

        # Convert list of dictionaries to list of tuples
        records = [tuple(row[col] for col in columns) for row in data]

        # Prepare SQL statement
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        try:
            cursor.executemany(insert_query, records)
            connection.commit()
            print(f"Successfully loaded {len(data)} KPI records into `{table}`.")
        except Exception as e:
            connection.rollback()
            print(f"Error inserting data into Redshift: {e}")
        finally:
            cursor.close()
            connection.close()



    # Define pipeline variables
    postgres_conn_id = "rds_conn"
    redshift_conn_id = "aws_redshift_conn"
    s3_conn_id = "aws_conn_default"
    rds_query = "SELECT u.*, s.* FROM users u JOIN songs s ON u.user_id = s.id"
    redshift_table_kpi = "kpi_result"
    s3_bucket = "streaming-data-source-11"
    s3_key = "Batch_data/streams/"

    # Extract RDS Data
    extracted_data_rds = extract_data_from_rds(postgres_conn_id, rds_query)

    # Stop DAG Execution if No RDS data is Found
    stop_dag_no_data = EmptyOperator(task_id="stop_dag_if_no_rds_data")

    # Validate S3 Data
    validated_s3_files = validate_s3_data(s3_conn_id, s3_bucket, s3_key)

    # Stop DAG Execution if No S3 Files Are Found
    stop_dag_no_files = EmptyOperator(task_id="stop_dag_if_no_s3_files")

    # Validate S3 Column Structure
    validated_s3_columns = validate_s3_data_columns(s3_conn_id, s3_bucket, validated_s3_files)

    # Stop DAG Execution if S3 Columns Are Invalid
    stop_dag_invalid_columns = EmptyOperator(task_id="stop_dag_if_s3_columns_invalid")

    #  Extract Data from S3
    extracted_data_s3 = extract_s3_data(s3_conn_id, s3_bucket, validated_s3_files)

    #  Transform & Compute KPIs
    transformed_data = transform_and_compute_kpis(extracted_data_rds, extracted_data_s3)

    # Create Redshift Table for KPIs
    create_kpi_table = create_redshift_table(redshift_conn_id, redshift_table_kpi)

    # Load Data into Redshift
    load_data = load_data_to_redshift(redshift_conn_id, redshift_table_kpi, transformed_data)

    # DAG Dependencies
    validated_s3_files >> [stop_dag_no_files, validated_s3_columns]
    validated_s3_columns >> [stop_dag_invalid_columns, extracted_data_s3]
    extracted_data_rds >> [stop_dag_no_data, transformed_data]
    create_kpi_table >> extracted_data_s3 >> transformed_data >> load_data




rds_s3_to_redshift_pipeline()  
