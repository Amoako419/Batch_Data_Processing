import os
import boto3
from dotenv import load_dotenv

# Load AWS credentials from .env file
load_dotenv()

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("ACCESS_KEYS"),
    aws_secret_access_key=os.getenv("SECRET_KEYS"),
    region_name=os.getenv("eu-west-1")  # Change if needed
)

# Define bucket name and folder path
BUCKET_NAME = "streaming-data-source-11"  # S3 Bucket Name
S3_FOLDER = "Batch_data/streams/"  # S3 Folder Path
LOCAL_FOLDER = "data/streams"  # Local Folder Containing CSV Files

# Function to upload all CSV files in the folder
def upload_csv_files_to_s3(local_folder, bucket_name, s3_folder):
    try:
        for file_name in os.listdir(local_folder):
            if file_name.endswith(".csv"):  # Only process CSV files
                file_path = os.path.join(local_folder, file_name)
                s3_key = f"{s3_folder}{file_name}"  # S3 key (prefix in bucket)

                # Upload file
                s3.upload_file(file_path, bucket_name, s3_key)
                print(f"Uploaded: {file_name} -> s3://{bucket_name}/{s3_key}")
        
        print("All CSV files uploaded successfully.")
    except Exception as e:
        print("Error uploading files:", e)

# Run the upload function
upload_csv_files_to_s3(LOCAL_FOLDER, BUCKET_NAME, S3_FOLDER)
