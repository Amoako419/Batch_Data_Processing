import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load CSV file
users_df = pd.read_csv("data/users/users.csv")

# Establish MySQL Connection
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        if connection.is_connected():
            print("✅ Connected to MySQL.")
            return connection
    except mysql.connector.Error as e:
        print("❌ Error:", e)
        return None

# Function to create the 'users' table if it doesn't exist
def create_users_table(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        user_name VARCHAR(255) NOT NULL,
        user_age INT,
        user_country VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
        connection.commit()
        print("✅ Table 'users' is ready.")
    except mysql.connector.Error as e:
        print("❌ Failed to create table:", e)

# Optimized batch insertion function
def insert_users_batch(connection, data):
    insert_query = """
        INSERT INTO users (user_id, user_name, user_age, user_country, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        user_name=VALUES(user_name), user_age=VALUES(user_age), 
        user_country=VALUES(user_country), created_at=VALUES(created_at);
    """

    # 🔹 Convert DataFrame to a List of Tuples (handling NaNs)
    records = [tuple(x) for x in data.astype(object).where(pd.notnull(data), None).values]

    batch_size = 1000  # Insert 1000 rows at a time

    try:
        with connection.cursor() as cursor:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
                print(f"✅ Inserted {i + len(batch)} records so far...")
        print("🎉 All user records inserted successfully!")
    except mysql.connector.Error as e:
        print("❌ Failed to insert batch:", e)
        connection.rollback()

# Main function
def main():
    connection = connect_to_database()
    if connection:
        try:
            create_users_table(connection)  # Ensure table exists
            insert_users_batch(connection, users_df)
        finally:
            connection.close()
            print("🔒 Database connection closed.")

if __name__ == "__main__":
    main()
