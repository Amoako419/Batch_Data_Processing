from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from typing import List

@task
def create_kpi_table_in_redshift(redshift_conn_id: str, table: str):
    """
    Create the KPI table in Amazon Redshift if it doesn't exist.
    
    Args:
        redshift_conn_id (str): Airflow connection ID for Redshift
        table (str): Redshift table name
    
    Returns:
        None
    """
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
        kpi_type VARCHAR(50)  -- 'genre' or 'hourly'
    );
    """
    
    redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
    redshift_hook.run(create_table_query)
    print(f"✅ Table {table} is ready in Redshift.")


@task
def load_kpis_to_redshift(redshift_conn_id: str, table: str, data: List[dict]):
    """
    Load computed KPI data into Amazon Redshift.
    
    Args:
        redshift_conn_id (str): Airflow connection ID for Redshift
        table (str): Redshift table where KPIs will be inserted
        data (List[dict]): Computed KPI data
    
    Returns:
        None
    """
    if not data:
        print("❌ No data to load into Redshift.")
        return

    redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()

    # Extract column names from data dictionary
    columns = data[0].keys()
    columns_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))

    insert_query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

    # Convert list of dictionaries to list of tuples for executemany()
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
