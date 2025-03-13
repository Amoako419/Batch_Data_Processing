from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from typing import List
import pandas as pd
import io
import logging

# ✅ Set up logging
logger = logging.getLogger("airflow.task")

# ✅ DAG Definition
@dag(
    dag_id='etl_rds_s3_to_redshift_pipeline_v2',
    start_date=datetime(2025, 3, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'owner': 'billy', 'retries': 3, 'retry_delay': timedelta(minutes=5)},
    description="ETL Pipeline that extracts data from PostgreSQL & S3, applies transformation, and loads into Redshift",
    tags=['postgresql', 's3', 'redshift']
)
def etl_rds_s3_to_redshift_pipeline():
    
    @task(retries=2)
    def extract_incremental_data(postgres_conn_id: str) -> List[dict]:
        """
        Extract only new or updated records from PostgreSQL using Change Data Capture (CDC).
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

            # Get last extraction timestamp
            last_extraction_time = pg_hook.get_first("SELECT MAX(extraction_timestamp) FROM extraction_log;")[0]

            query = f"""
            SELECT * FROM users u
            JOIN songs s ON u.user_id = s.id
            WHERE s.updated_at > '{last_extraction_time}' OR u.updated_at > '{last_extraction_time}'
            """

            df = pg_hook.get_pandas_df(query)
            df["extraction_timestamp"] = datetime.utcnow()

            logger.info(f"✅ Extracted {len(df)} records from RDS.")
            return df.to_dict(orient="records") if not df.empty else []
        except Exception as e:
            logger.error(f"❌ Failed to extract data from RDS: {e}")
            raise

    stop_dag_no_rds_data = EmptyOperator(task_id="stop_dag_if_no_rds_data")

    @task(retries=2)
    def validate_s3_data(s3_conn_id: str, bucket_name: str, key: str) -> List[str]:
        """
        Validate the presence of CSV files in S3.
        """
        try:
            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=key)
            csv_keys = [k for k in keys if k.endswith('.csv')]

            if not csv_keys:
                logger.warning(f"⚠️ No CSV files found in S3 bucket {bucket_name}/{key}. DAG will stop.")
                return []

            logger.info(f"✅ Found {len(csv_keys)} CSV files in {bucket_name}/{key}.")
            return csv_keys
        except Exception as e:
            logger.error(f"❌ Failed to validate S3 data: {e}")
            raise

    stop_dag_no_files = EmptyOperator(task_id="stop_dag_if_no_s3_files")

    @task(retries=2)
    def validate_s3_data_columns(s3_conn_id: str, bucket_name: str, keys: List[str]) -> List[str]:
        """
        Validate the columns in the S3 data to ensure they match the expected schema.
        """
        try:
            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            expected_columns = {"user_id", "track_id", "listen_time"}
            valid_keys = []

            for key in keys:
                s3_object = s3_hook.get_key(key, bucket_name)
                csv_data = s3_object.get()["Body"].read().decode("utf-8")
                df = pd.read_csv(io.StringIO(csv_data))

                missing_columns = expected_columns - set(df.columns)
                if missing_columns:
                    logger.warning(f"⚠️ Missing columns in {key}: {missing_columns}. Skipping file.")
                    continue

                valid_keys.append(key)

            if not valid_keys:
                logger.error("❌ No valid S3 files found. DAG execution will stop.")
                raise ValueError("No valid S3 files found.")

            logger.info(f"✅ {len(valid_keys)} valid CSV files found in {bucket_name}.")
            return valid_keys
        except Exception as e:
            logger.error(f"❌ Failed to validate S3 columns: {e}")
            raise

    stop_dag_invalid_columns = EmptyOperator(task_id="stop_dag_if_s3_columns_invalid")

    @task(retries=2)
    def extract_s3_data(s3_conn_id: str, bucket_name: str, keys: List[str]) -> List[dict]:
        """
        Extract data from S3 and return as a list of dictionaries.
        """
        try:
            s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            dataframes = []

            for key in keys:
                s3_object = s3_hook.get_key(key, bucket_name)
                csv_data = s3_object.get()["Body"].read().decode("utf-8")

                df = pd.read_csv(io.StringIO(csv_data))
                df["listen_time"] = pd.to_datetime(df["listen_time"], errors="coerce").astype(str)

                dataframes.append(df)

            combined_df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
            logger.info(f"✅ Extracted {len(combined_df)} records from S3.")
            return combined_df.to_dict(orient="records")
        except Exception as e:
            logger.error(f"❌ Failed to extract S3 data: {e}")
            raise

    transformed_data = transform_and_compute_kpis(extracted_incremental_data, extract_s3_data)

    create_kpi_table = create_redshift_table(redshift_conn_id, redshift_table_kpi)

    @task(retries=2)
    def load_data_to_redshift(redshift_conn_id: str, table: str, data: List[dict]):
        """
        Load computed KPI data into Amazon Redshift.
        """
        try:
            if not data:
                logger.warning("⚠️ No data to load into Redshift.")
                return

            redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
            connection = redshift_hook.get_conn()
            cursor = connection.cursor()

            required_columns = [
                "created_at", "listen_count", "avg_track_duration_sec", "popularity_index",
                "unique_listeners", "track_diversity_index", "stream_hour", "top_artists", "kpi_type"
            ]

            cleaned_data = [{k: row[k] for k in required_columns if k in row} for row in data]

            columns_str = ', '.join(required_columns)
            placeholders = ', '.join(['%s'] * len(required_columns))
            insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

            records = [tuple(row[col] for col in required_columns) for row in cleaned_data]

            cursor.executemany(insert_query, records)
            connection.commit()

            logger.info(f"✅ Successfully loaded {len(data)} KPI records into `{table}`.")
        except Exception as e:
            connection.rollback()
            logger.error(f"❌ Error inserting data into Redshift: {e}")
            raise
        finally:
            cursor.close()
            connection.close()

    validated_s3_files >> [stop_dag_no_files, validated_s3_columns]
    validated_s3_columns >> [stop_dag_invalid_columns, extract_s3_data]
    extract_incremental_data >> [stop_dag_no_rds_data, transformed_data]
    create_kpi_table >> extract_s3_data >> transformed_data >> load_data_to_redshift

etl_rds_s3_to_redshift_pipeline()
