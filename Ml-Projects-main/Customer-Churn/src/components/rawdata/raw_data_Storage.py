from google.cloud import storage
from datetime import datetime
import os
from prefect import flow
from src.logger import logging

key=r'D:\Ml-Projects\Customer-Churn\notebook\data\key.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=key



def upload_to_gcs(bucket_name, source_file_path, destination_folder):
    """
    Uploads a file to a Google Cloud Storage bucket under a partitioned folder structure.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        source_file_path (str): Path to the local file to be uploaded.
        destination_folder (str): Base folder in the bucket (e.g., 'raw_data').
    """
    # Initialize the Google Cloud Storage client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Get the current date for partitioning
    current_date = datetime.utcnow().strftime('%Y/%m/%d')
    
    # Construct the destination path in GCS
    file_name = os.path.basename(source_file_path)
    destination_blob_path = f"{destination_folder}/{current_date}/{file_name}"
    
    # Upload the file
    blob = bucket.blob(destination_blob_path)
    blob.upload_from_filename(source_file_path)
    
    logging.info(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_path}")

# Example usage
if __name__ == "__main__":
    BUCKET_NAME = "customer-churn-dataset-bucket"  # Replace with your GCS bucket name
    SOURCE_FILE_PATH = r"D:\Ml-Projects\Customer-Churn\notebook\data\customer_data1.csv"  # Replace with the actual file path
    DESTINATION_FOLDER = "raw_data"  # Top-level folder in GCS
    
    upload_to_gcs(BUCKET_NAME, SOURCE_FILE_PATH, DESTINATION_FOLDER)
